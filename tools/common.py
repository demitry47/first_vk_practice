"""Общий код для всех скриптов стенда.

Здесь подключение к Trino со сбором серверной статистики запроса, чтение метаданных
Iceberg ($files, SHOW CREATE TABLE), разбор футеров parquet/orc прямо из MinIO,
запись результатов в results/ и тексты запросов, которые гоняет бенчмарк.
"""
from __future__ import annotations

import csv
import json
import os
import re
import statistics
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import trino

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

SCALE = os.getenv("SCALE", "sf4")
WRITE_REPEATS = int(os.getenv("WRITE_REPEATS", "3"))
READ_REPEATS = int(os.getenv("READ_REPEATS", "5"))
CATALOG = "iceberg"
BUCKET = "tpch-lake"
QTY_LIMIT = 300          # порог Q18: sum(quantity) по заказу > 300


def info(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} | {msg}", flush=True)


# ============================================================================ Trino
@dataclass
class QueryResult:
    """Строки ответа, время на стороне клиента и статистика, которую вернул сервер."""
    rows: list
    columns: list[str]
    client_s: float
    stats: dict = field(default_factory=dict)
    query_id: str | None = None

    def _sec(self, key: str) -> float:
        return float(self.stats.get(key) or 0) / 1000

    @property
    def server_s(self) -> float:
        # время берем серверное (elapsedTimeMillis), клиентское - только запасной вариант
        v = self.stats.get("elapsedTimeMillis")
        return float(v) / 1000 if v is not None else self.client_s

    @property
    def cpu_s(self) -> float:
        return self._sec("cpuTimeMillis")

    @property
    def read_mb(self) -> float:
        return float(self.stats.get("physicalInputBytes") or 0) / 1e6

    @property
    def rows_processed(self) -> int:
        return int(self.stats.get("processedRows") or 0)

    @property
    def peak_mem_mb(self) -> float:
        return float(self.stats.get("peakMemoryBytes") or 0) / 1e6

    def metrics(self) -> dict:
        return {"server_s": round(self.server_s, 3), "client_s": round(self.client_s, 3),
                "cpu_s": round(self.cpu_s, 3), "read_mb": round(self.read_mb, 3),
                "rows_processed": self.rows_processed, "peak_mem_mb": round(self.peak_mem_mb, 1)}


def _connect(**kw):
    url = urlparse(os.getenv("TRINO_URL", "http://localhost:8080"))
    return trino.dbapi.connect(host=url.hostname, port=url.port or 8080, http_scheme=url.scheme,
                               user="etl", catalog=CATALOG, **kw)


class Session:
    """Подключение к Trino в каталоге iceberg.

    props - сессионные свойства каталога без префикса (compression_codec='GZIP' и т.п.).
    Свойства, которых нет в этой версии Trino, пропускаются с предупреждением.
    """

    _known: set[str] | None = None

    def __init__(self, **props):
        self.props = props
        session = {f"{CATALOG}.{k}": str(v) for k, v in props.items() if self._exists(k)}
        self.conn = _connect(schema="bronze", session_properties=session, request_timeout=3600)

    @classmethod
    def _exists(cls, name: str) -> bool:
        if cls._known is None:
            cur = _connect().cursor()
            cur.execute(f"SHOW SESSION LIKE '{CATALOG}.%'")
            cls._known = {r[0].split(".", 1)[1] for r in cur.fetchall() if "." in str(r[0])}
            info(f"в каталоге {CATALOG} доступно сессионных свойств: {len(cls._known)}")
        if name not in cls._known:
            info(f"!! свойства {CATALOG}.{name} в этой версии Trino нет, пропускаю")
            return False
        return True

    def run(self, sql: str) -> QueryResult:
        cur = self.conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        took = time.perf_counter() - t0
        cols = [d[0] for d in (cur.description or [])]
        return QueryResult(rows, cols, took, dict(cur.stats or {}), cur.query_id)

    def scalar(self, sql: str):
        return self.run(sql).rows[0][0]

    # ---------------------------------------------------------------- метаданные Iceberg
    def table_size(self, table: str) -> dict:
        """Файлы данных текущего снапшота по $files (content = 0, delete-файлы не считаем)."""
        schema, name = table.split(".")
        n, size, recs = self.run(
            f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0), coalesce(sum(record_count), 0) '
            f'FROM {CATALOG}.{schema}."{name}$files" WHERE content = 0').rows[0]
        return {"files": int(n), "bytes": int(size), "mb": round(int(size) / 1e6, 3), "records": int(recs)}

    def table_layout(self, table: str) -> dict:
        ddl = self.scalar(f"SHOW CREATE TABLE {CATALOG}.{table}")
        part = re.search(r"partitioning\s*=\s*ARRAY\[(.*?)\]", ddl)
        sort = re.search(r"sorted_by\s*=\s*ARRAY\[(.*?)\]", ddl)
        fmt = re.search(r"format\s*=\s*'(\w+)'", ddl)
        return {"format": fmt.group(1) if fmt else "?",
                "partitioning": part.group(1).replace("'", "") if part else "нет",
                "sorted_by": sort.group(1).replace("'", "") if sort else "нет"}

    def data_files(self, table: str) -> list[str]:
        """Пути файлов данных таблицы, самый большой первым."""
        schema, name = table.split(".")
        return [r[0] for r in self.run(f'SELECT file_path FROM {CATALOG}.{schema}."{name}$files" '
                                       f'WHERE content = 0 ORDER BY file_size_in_bytes DESC').rows]

    def largest_file(self, table: str) -> str | None:
        files = self.data_files(table)
        return files[0] if files else None


# ============================================================================ SQL-скрипты
def read_sql(path: Path, **params) -> list[str]:
    """Читает .sql, подставляет {{параметры}}, выкидывает комментарии и режет по ';'."""
    text = path.read_text(encoding="utf-8")
    for k, v in params.items():
        text = text.replace("{{" + k + "}}", str(v))
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", text) if s.strip()]


def new_load_id() -> str:
    return time.strftime("load-%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]


# ============================================================================ MinIO
def s3() -> "object":
    from pyarrow import fs
    return fs.S3FileSystem(access_key=os.getenv("MINIO_USER", "minio-lab"),
                           secret_key=os.getenv("MINIO_PASSWORD", "minio-lab-pass-2026"),
                           endpoint_override=os.getenv("S3_ENDPOINT", "localhost:9000"),
                           scheme="http", region="us-east-1")


def _key(path: str) -> str:
    return re.sub(r"^s3a?://", "", path)


def codec_from_footer(path: str) -> dict:
    """Какой кодек реально записан в файле: независимая проверка того, что задавалось в сессии."""
    try:
        key = _key(path)
        if key.lower().endswith(".parquet"):
            import pyarrow.parquet as pq
            with s3().open_input_file(key) as f:
                md = pq.ParquetFile(f).metadata
                codecs = {md.row_group(i).column(j).compression
                          for i in range(md.num_row_groups) for j in range(md.num_columns)}
            return {"container": "PARQUET", "codec": "/".join(sorted(codecs)), "blocks": md.num_row_groups}
        if key.lower().endswith(".orc"):
            import pyarrow.orc as orc
            with s3().open_input_file(key) as f:
                of = orc.ORCFile(f)
                return {"container": "ORC", "codec": str(of.compression), "blocks": of.nstripes}
        return {"container": "?", "codec": "непонятное расширение"}
    except Exception as e:  # noqa: BLE001
        return {"container": "?", "codec": f"ошибка: {str(e)[:120]}"}


def rowgroup_ranges(path: str, column: str) -> list[tuple]:
    """(номер, строк, min, max) колонки по каждому row group parquet-файла.

    Для DECIMAL pyarrow не показывает статистику из файлов Trino: Trino пишет только поля
    min_value/max_value, а pyarrow в таком файле понимает лишь старые min/max. Сам Trino эти
    поля читает и row group по ним пропускает (видно в EXPLAIN ANALYZE). Если статистики нет,
    диапазон считается по самой колонке, значения получаются те же."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    with s3().open_input_file(_key(path)) as f:
        pf = pq.ParquetFile(f)
        md = pf.metadata
        idx = [md.schema.column(i).name for i in range(md.num_columns)].index(column)
        out = []
        for i in range(md.num_row_groups):
            st = md.row_group(i).column(idx).statistics
            if st and st.has_min_max:
                lo, hi = str(st.min), str(st.max)
            else:
                mm = pc.min_max(pf.read_row_group(i, columns=[column]).column(0)).as_py()
                lo, hi = str(mm["min"]), str(mm["max"])
            out.append((i, md.row_group(i).num_rows, lo, hi))
        return out


def bucket_mb(bucket: str = BUCKET) -> float:
    from pyarrow import fs
    infos = s3().get_file_info(fs.FileSelector(bucket, recursive=True))
    return sum(i.size or 0 for i in infos if i.type == fs.FileType.File) / 1e6


# ============================================================================ результаты
def write_csv(name: str, rows: list[dict]) -> Path:
    p = RESULTS / name
    p.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        cols: list[str] = []
        for r in rows:
            cols += [k for k in r if k not in cols]
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        info(f"записан {p.relative_to(ROOT)} ({len(rows)} строк)")
    return p


def read_csv(name: str) -> list[dict]:
    p = RESULTS / name
    if not p.exists():
        return []
    with p.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_json(name: str, obj) -> None:
    (RESULTS / name).write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    info(f"записан results/{name}")


def median(values) -> float:
    v = [float(x) for x in values]
    return statistics.median(v) if v else float("nan")


# ============================================================================ запросы
def q18_sql(src: str) -> str:
    """Q18 без изменений из варианты.md. src - откуда читать: tpch.sf4, iceberg.bronze, iceberg.bench_orc_zstd ..."""
    return f"""
SELECT
  c.name,
  c.custkey,
  o.orderkey,
  o.orderdate,
  o.totalprice,
  sum(l.quantity) AS total_quantity
FROM {src}.customer AS c
JOIN {src}.orders   AS o ON c.custkey = o.custkey
JOIN {src}.lineitem AS l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey
  FROM {src}.lineitem
  GROUP BY orderkey
  HAVING sum(quantity) > {QTY_LIMIT}
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100"""


def like_scan_sql(src: str) -> str:
    """Второй тестовый запрос: LIKE по самой тяжелой текстовой колонке lineitem.comment.
    Здесь сильнее всего сказывается, насколько файл сжат и как быстро кодек распаковывает."""
    return f"""
SELECT shipmode, count(*) AS lines, sum(quantity) AS qty
FROM {src}.lineitem
WHERE comment LIKE '%carefully%'
GROUP BY shipmode
ORDER BY shipmode"""


def q18_silver_sql(fact_table: str) -> str:
    """Q18 поверх готового агрегата fact_orders (sum(quantity) уже посчитан при загрузке)."""
    return f"""
SELECT c.cust_name, f.custkey, f.orderkey, f.orderdate, f.totalprice, f.total_quantity
FROM {fact_table} f
JOIN {CATALOG}.silver.dim_customer c ON c.custkey = f.custkey
WHERE f.total_quantity > {QTY_LIMIT}
ORDER BY f.totalprice DESC, f.orderdate
LIMIT 100"""
