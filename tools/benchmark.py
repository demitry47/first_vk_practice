"""Бенчмарк хранения для Q18.

Часть 1. Таблицы Bronze, которые читает Q18, пересоздаются в 8 вариантах
         {PARQUET, ORC} x {NONE, SNAPPY, GZIP, ZSTD}: размер, время записи, время чтения.
Часть 2. Раскладка Silver: fact_orders с сортировкой и без, плюс сравнение слоев медальона.

Режимы (bench.sh запускает их по очереди):
  --writes-only    только запись вариантов (каждый WRITE_REPEATS раз, берется медиана)
  --reads-only     генератор без записи, чтение всех вариантов, часть 2
  --layout-only    только часть 2
  --variants PARQUET:ZSTD,ORC:GZIP    взять не все варианты
  --skip-existing  не перезаписывать вариант, если у него уже есть отметка .done

Зачем ждать между записями: после DROP/CTAS старые файлы удаляются не сразу. Скрипт ждет,
пока объем бакета перестанет меняться (wait_bucket_stable), иначе следующая запись идет на
раздутый диск и замер портится. Журнал ожиданий: results/bench/storage_log.csv.
"""
from __future__ import annotations

import argparse
import random
import re
import time

from common import (CATALOG, QTY_LIMIT, READ_REPEATS, RESULTS, ROOT, SCALE, WRITE_REPEATS, Session,
                    bucket_mb, codec_from_footer, info, like_scan_sql, q18_silver_sql, q18_sql, read_csv,
                    read_sql, rowgroup_ranges, write_csv)

FORMATS = ["PARQUET", "ORC"]
CODECS = ["NONE", "SNAPPY", "GZIP", "ZSTD"]
TABLES = ["customer", "orders", "lineitem"]      # ровно то, что читает Q18
OUT = "bench/"


def schema_name(fmt: str, codec: str) -> str:
    return f"bench_{fmt}_{codec}".lower()


def wait_bucket_stable(label: str, limit_s: int = 300) -> None:
    """Ждем, пока занятый объем бакета не перестанет уменьшаться (два замера подряд без изменений)."""
    journal = read_csv(OUT + "storage_log.csv")
    try:
        prev = bucket_mb()
    except Exception as e:  # noqa: BLE001
        info(f"объем бакета не измерить ({e}), просто жду 30 с")
        time.sleep(30)
        return
    first, t0, calm = prev, time.time(), 0
    while time.time() - t0 < limit_s and calm < 2:
        time.sleep(10)
        now = bucket_mb()
        calm = calm + 1 if abs(now - prev) < 1 else 0
        prev = now
    journal.append({"moment": label, "bucket_mb_before": round(first, 1), "bucket_mb_after": round(prev, 1),
                    "waited_s": round(time.time() - t0)})
    write_csv(OUT + "storage_log.csv", journal)
    info(f"бакет {first:,.0f} -> {prev:,.0f} МБ, ждали {time.time() - t0:.0f} с")


# ============================================================================ запись
def write_variant(fmt: str, codec: str) -> None:
    sch = schema_name(fmt, codec)
    s = Session(compression_codec=codec)
    s.run(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{sch}")
    rows = []
    for rep in range(1, WRITE_REPEATS + 1):
        for t in TABLES:
            s.run(f"DROP TABLE IF EXISTS {CATALOG}.{sch}.{t}")
            r = s.run(f"CREATE TABLE {CATALOG}.{sch}.{t} WITH (format = '{fmt}') AS SELECT * FROM tpch.{SCALE}.{t}")
            rows.append({"variant": f"{fmt}+{codec}", "format": fmt, "codec": codec, "repeat": rep,
                         "table": t, "rows": r.rows[0][0], **r.metrics()})
            info(f"[{fmt}+{codec}] {t:8s} запись {rep}: {r.server_s:6.1f} s, CPU {r.cpu_s:6.1f} s")
        if rep < WRITE_REPEATS:
            wait_bucket_stable(f"{fmt}+{codec}, после записи {rep}")
    write_csv(OUT + f"{sch}_writes.csv", rows)

    sizes = []
    for t in TABLES:
        size = s.table_size(f"{sch}.{t}")
        path = s.largest_file(f"{sch}.{t}")
        fc = codec_from_footer(path) if path else {}
        sizes.append({"variant": f"{fmt}+{codec}", "format": fmt, "codec": codec, "table": t,
                      "rows": size["records"], "files": size["files"], "size_bytes": size["bytes"],
                      "size_mb": size["mb"], "codec_in_file": fc.get("codec", ""),
                      "container_in_file": fc.get("container", ""), "blocks_in_file": fc.get("blocks", ""),
                      "checked_file": path})
    write_csv(OUT + f"{sch}_sizes.csv", sizes)
    total = sum(x["size_mb"] for x in sizes)
    li_codec = next(x["codec_in_file"] for x in sizes if x["table"] == "lineitem")
    info(f"[{fmt}+{codec}] Bronze занимает {total:,.1f} МБ, кодек в футере lineitem: {li_codec}")
    (RESULTS / OUT / f"{sch}.done").write_text(time.strftime("%F %T"))


# ============================================================================ генератор
def generator_only() -> None:
    """Сколько стоит сама генерация данных коннектором tpch, без записи в Iceberg."""
    s, rows = Session(), []
    for t in TABLES:
        cols = [r[0] for r in s.run(f"SHOW COLUMNS FROM tpch.{SCALE}.{t}").rows]
        for rep in range(1, WRITE_REPEATS + 1):
            r = s.run(f"SELECT count(*), checksum(ROW({', '.join(cols)})) FROM tpch.{SCALE}.{t}")
            rows.append({"table": t, "repeat": rep, **r.metrics()})
            info(f"генерация tpch.{SCALE}.{t:8s} #{rep}: {r.server_s:6.1f} s")
    write_csv(OUT + "generator.csv", rows)


# ============================================================================ чтение
def read_all(variants) -> None:
    queries = {"Q18": q18_sql, "LIKE_SCAN": like_scan_sql}
    s, out = Session(), []

    def one(fmt, codec, qname, rnd):
        r = s.run(queries[qname](f"{CATALOG}.{schema_name(fmt, codec)}"))
        out.append({"variant": f"{fmt}+{codec}", "format": fmt, "codec": codec, "query": qname,
                    "round": rnd, "warmup": rnd == 0, "result_rows": len(r.rows), **r.metrics()})
        info(f"[{fmt}+{codec}] {qname:9s} {'прогрев' if rnd == 0 else f'круг {rnd}'}: "
             f"{r.server_s:6.2f} s, прочитано {r.read_mb:8.1f} МБ")

    for fmt, codec in variants:                    # прогрев: по одному разу в фиксированном порядке
        for qn in queries:
            one(fmt, codec, qn, 0)
    shuffle = random.Random(14)
    for rnd in range(1, READ_REPEATS + 1):         # круги замеров: в каждом свой случайный порядок
        order = variants[:]
        shuffle.shuffle(order)
        for fmt, codec in order:
            for qn in queries:
                one(fmt, codec, qn, rnd)
    for fmt, codec in variants:
        write_csv(OUT + f"{schema_name(fmt, codec)}_reads.csv",
                  [x for x in out if x["variant"] == f"{fmt}+{codec}"])


# ============================================================================ раскладка Silver
def layout_experiment() -> None:
    s = Session(compression_codec="ZSTD", parquet_writer_block_size="8MB")
    s.run(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bench_layout")
    s.run(f"DROP TABLE IF EXISTS {CATALOG}.bench_layout.fact_orders_unsorted")
    # тот же SQL, что у silver.fact_orders, только без sorted_by:
    # после GROUP BY строки лежат в произвольном порядке относительно total_quantity
    stmt = next(x for x in read_sql(ROOT / "sql" / "02_silver.sql")
                if x.lower().startswith(f"create table {CATALOG}.silver.fact_orders"))
    stmt = stmt.replace(f"{CATALOG}.silver.fact_orders", f"{CATALOG}.bench_layout.fact_orders_unsorted", 1)
    stmt = re.sub(r",\s*sorted_by\s*=\s*ARRAY\['total_quantity'\]", "", stmt)
    info("строю fact_orders без сортировки")
    s.run(stmt)
    try:
        rg = []
        for path in s.data_files("bench_layout.fact_orders_unsorted"):
            rg += [{"file": path.rsplit("/", 1)[-1], "row_group": i, "rows": n, "min_total_quantity": lo,
                    "max_total_quantity": hi} for i, n, lo, hi in rowgroup_ranges(path, "total_quantity")]
        write_csv(OUT + "unsorted_rowgroups.csv", rg)
    except Exception as e:  # noqa: BLE001
        info(f"row group не прочитаны: {e}")

    # что видит сам Trino: сколько строк реально пришло в ScanFilter по фильтру Q18
    plans = []
    for title, tbl in [("fact_orders, sorted_by total_quantity", "silver.fact_orders"),
                       ("fact_orders без сортировки", "bench_layout.fact_orders_unsorted")]:
        sql = (f"EXPLAIN ANALYZE SELECT count(*), max(total_quantity) FROM {CATALOG}.{tbl} "
               f"WHERE total_quantity > {QTY_LIMIT}")
        plans += [f"-- {title}", f"-- {sql};", s.scalar(sql), ""]
    (RESULTS / "fact_orders_pruning_explain.txt").write_text("\n".join(plans), encoding="utf-8")

    cases = [
        ("Bronze: Q18 как в задании", q18_sql(f"{CATALOG}.bronze"), "bronze.lineitem"),
        ("Silver: fact_orders без сортировки", q18_silver_sql(f"{CATALOG}.bench_layout.fact_orders_unsorted"),
         "bench_layout.fact_orders_unsorted"),
        ("Silver: fact_orders, sorted_by total_quantity", q18_silver_sql(f"{CATALOG}.silver.fact_orders"),
         "silver.fact_orders"),
        ("Gold: витрина q18_answer", f"SELECT * FROM {CATALOG}.gold.q18_answer ORDER BY rank_no",
         "gold.q18_answer"),
    ]
    reader, out = Session(), []
    meta = {name: reader.table_size(tbl) for name, _, tbl in cases}
    order = list(range(len(cases)))
    shuffle = random.Random(3)
    for rnd in range(0, READ_REPEATS + 1):
        if rnd:
            shuffle.shuffle(order)
        for i in order:
            name, sql, tbl = cases[i]
            r = reader.run(sql)
            out.append({"case": name, "table": tbl, "table_files": meta[name]["files"],
                        "table_mb": meta[name]["mb"], "round": rnd, "warmup": rnd == 0,
                        "result_rows": len(r.rows), **r.metrics()})
            info(f"{name:46s} {'прогрев' if rnd == 0 else f'круг {rnd}'}: {r.server_s:6.2f} s, "
                 f"прочитано {r.read_mb:7.1f} МБ, строк {r.rows_processed:,}")
    write_csv(OUT + "layout.csv", out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variants", default="")
    ap.add_argument("--writes-only", action="store_true")
    ap.add_argument("--reads-only", action="store_true")
    ap.add_argument("--layout-only", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    a = ap.parse_args()

    variants = [(f, c) for f in FORMATS for c in CODECS]
    if a.variants:
        variants = [tuple(v.strip().upper().split(":")) for v in a.variants.split(",")]
    info(f"масштаб {SCALE}, вариантов {len(variants)}, записей {WRITE_REPEATS}, кругов чтения {READ_REPEATS}")
    t0 = time.time()

    if a.layout_only:
        layout_experiment()
        return
    if not a.reads_only:
        for i, (fmt, codec) in enumerate(variants, 1):
            if a.skip_existing and (RESULTS / OUT / f"{schema_name(fmt, codec)}.done").exists():
                info(f"[{i}/{len(variants)}] {fmt}+{codec} уже записан, пропускаю")
                continue
            info(f"[{i}/{len(variants)}] пишу {fmt}+{codec}")
            write_variant(fmt, codec)
            wait_bucket_stable(f"вариант {fmt}+{codec} готов")
            info(f"с начала прошло {(time.time() - t0) / 60:.1f} мин")
    if a.writes_only:
        return
    generator_only()
    read_all(variants)
    layout_experiment()
    info(f"бенчмарк закончен, заняло {(time.time() - t0) / 60:.1f} мин")


if __name__ == "__main__":
    main()
