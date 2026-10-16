"""ELT для Q18: tpch -> Bronze -> Silver -> Gold.

После загрузки скрипт описывает получившиеся слои (строки, файлы, размер, кодек из футера),
прогоняет проверки качества данных и сверяет витрину с прямым Q18 по генератору.

    docker compose run --rm -T toolbox python tools/pipeline.py

Что появляется в results/: environment.md, pipeline_steps.csv, layers.csv, dq_checks.csv,
fact_orders_rowgroups.csv, gold_*.csv, validation.md
"""
from __future__ import annotations

import os
import platform
import time
from decimal import Decimal

from common import (CATALOG, QTY_LIMIT, RESULTS, ROOT, SCALE, Session, codec_from_footer, info,
                    new_load_id, q18_sql, read_sql, rowgroup_ranges, write_csv)

LAYERS = {
    "bronze": ["region", "nation", "customer", "orders", "lineitem"],
    "silver": ["dim_customer", "fact_orders", "bulk_orders"],
    "gold": ["q18_answer", "bulk_orders_by_year", "order_qty_histogram"],
}
# Ответ Q18 из спецификации TPC-H для SF1: 57 строк, ниже - первая из них
REF_SF1_ROWS = 57
REF_SF1_TOP = {"custkey": 128120, "orderkey": 4722021, "orderdate": "1994-04-07",
               "totalprice": 544089.09, "quantity": 323}


def wait_for_trino(timeout_s: int = 300) -> Session:
    t0 = time.time()
    while True:
        try:
            s = Session()
            s.scalar("SELECT count(*) FROM tpch.tiny.nation")
            info("Trino на связи")
            return s
        except Exception as e:  # noqa: BLE001
            if time.time() - t0 > timeout_s:
                raise RuntimeError(f"Trino так и не ответил: {e}") from e
            time.sleep(3)


def save_environment(s: Session) -> None:
    nodes = s.run("SELECT node_id, node_version, coordinator FROM system.runtime.nodes").rows
    kb = 0
    try:
        kb = int(next(l for l in open("/proc/meminfo") if l.startswith("MemTotal")).split()[1])
    except (OSError, StopIteration):
        pass
    lines = ["| Параметр | Значение |", "|---|---|",
             f"| Trino | {nodes[0][1]} (узлов: {len(nodes)}) |",
             f"| Масштаб | {SCALE} |",
             f"| CPU в Docker VM | {os.cpu_count()} |",
             f"| RAM в Docker VM | {kb / 1024 / 1024:.1f} ГБ |",
             f"| Ядро Docker VM | {platform.release()} |"]
    (RESULTS / "environment.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_layers() -> None:
    load_id = new_load_id()
    info(f"идентификатор загрузки: {load_id}")
    plan = [
        ("bronze", "01_bronze.sql", {"compression_codec": "ZSTD"}),
        # в Silver row group уменьшен до 8 МБ: тогда у отсортированной fact_orders каждый
        # row group покрывает узкий диапазон total_quantity и фильтр Q18 их отбрасывает
        ("silver", "02_silver.sql", {"compression_codec": "ZSTD", "parquet_writer_block_size": "8MB"}),
        ("gold", "03_gold.sql", {"compression_codec": "ZSTD"}),
    ]
    steps = []
    for layer, fname, props in plan:
        s = Session(**props)
        info(f"----- {layer.upper()} ({fname})")
        for stmt in read_sql(ROOT / "sql" / fname, scale=SCALE, load_id=load_id):
            short = " ".join(stmt.split())[:100]
            r = s.run(stmt)
            info(f"{r.server_s:8.2f} s | {short}")
            if short.lower().startswith("create table"):
                steps.append({"layer": layer, "table": short.split()[2], "rows_written": r.rows[0][0],
                              **r.metrics()})
    write_csv("pipeline_steps.csv", steps)


def describe_layers(s: Session) -> None:
    out = []
    for layer, tables in LAYERS.items():
        for t in tables:
            fq = f"{layer}.{t}"
            size = s.table_size(fq)
            lay = s.table_layout(fq)
            cols = [r[0] for r in s.run(f"SHOW COLUMNS FROM {CATALOG}.{fq}").rows]
            big = s.largest_file(fq)
            codec = codec_from_footer(big) if big else {}
            out.append({"layer": layer, "table": f"{CATALOG}.{fq}",
                        "rows": s.scalar(f"SELECT count(*) FROM {CATALOG}.{fq}"),
                        "columns": len(cols), "column_list": ", ".join(cols),
                        "files": size["files"], "size_bytes": size["bytes"], "size_mb": size["mb"],
                        "format": lay["format"], "codec_in_file": codec.get("codec", ""),
                        "row_groups_biggest_file": codec.get("blocks", ""),
                        "partitioning": lay["partitioning"], "sorted_by": lay["sorted_by"]})
            info(f"{fq:34s} {out[-1]['rows']:>12,} строк {size['mb']:>10.2f} МБ "
                 f"{out[-1]['codec_in_file']:>8s} sort={lay['sorted_by']}")
    write_csv("layers.csv", out)


def quality_checks(s: Session) -> None:
    b, sv = f"{CATALOG}.bronze", f"{CATALOG}.silver"
    checks = [
        ("bronze.lineitem: строк", f"SELECT count(*) FROM {b}.lineitem"),
        ("lineitem: orderkey IS NULL", f"SELECT count_if(orderkey IS NULL) FROM {b}.lineitem"),
        ("lineitem: quantity <= 0", f"SELECT count_if(quantity <= 0) FROM {b}.lineitem"),
        ("lineitem: extendedprice <= 0", f"SELECT count_if(extendedprice <= 0) FROM {b}.lineitem"),
        ("lineitem: discount или tax вне [0;1]",
         f"SELECT count_if(discount NOT BETWEEN 0 AND 1 OR tax NOT BETWEEN 0 AND 1) FROM {b}.lineitem"),
        ("lineitem: позиции без заказа",
         f"SELECT count(*) FROM {b}.lineitem l LEFT JOIN {b}.orders o ON o.orderkey = l.orderkey "
         f"WHERE o.orderkey IS NULL"),
        ("bronze.orders: строк", f"SELECT count(*) FROM {b}.orders"),
        ("orders: заказы без позиций",
         f"SELECT count(*) FROM {b}.orders o LEFT JOIN (SELECT DISTINCT orderkey FROM {b}.lineitem) l "
         f"ON l.orderkey = o.orderkey WHERE l.orderkey IS NULL"),
        ("silver.fact_orders: строк", f"SELECT count(*) FROM {sv}.fact_orders"),
        ("fact_orders: price_ok = false", f"SELECT count_if(NOT price_ok) FROM {sv}.fact_orders"),
        ("fact_orders: макс. |totalprice − net_amount|",
         f"SELECT max(abs(totalprice - net_amount)) FROM {sv}.fact_orders"),
        ("fact_orders: макс. |totalprice − net_amount| / lines_cnt",
         f"SELECT max(abs(totalprice - net_amount) / lines_cnt) FROM {sv}.fact_orders"),
        # строгая сверка: повторяем арифметику генератора (копейки, два усечения на позицию)
        ("orders: totalprice ≠ сумме позиций с усечением как в генераторе",
         f"SELECT count_if(CAST(o.totalprice AS DECIMAL(12, 2)) <> g.gen_total) FROM {b}.orders o JOIN "
         f"(SELECT orderkey, sum(truncate(truncate(CAST(extendedprice AS DECIMAL(12, 2)) "
         f"* (1 - CAST(discount AS DECIMAL(4, 2))), 2) * (1 + CAST(tax AS DECIMAL(4, 2))), 2)) AS gen_total "
         f"FROM {b}.lineitem GROUP BY orderkey) g ON g.orderkey = o.orderkey"),
        ("bronze.customer: строк", f"SELECT count(*) FROM {b}.customer"),
        ("customer: повторы custkey", f"SELECT count(*) - count(DISTINCT custkey) FROM {b}.customer"),
        ("silver.dim_customer: строк", f"SELECT count(*) FROM {sv}.dim_customer"),
        ("dim_customer: nation IS NULL", f"SELECT count_if(nation IS NULL) FROM {sv}.dim_customer"),
        (f"fact_orders: total_quantity > {QTY_LIMIT}",
         f"SELECT count_if(total_quantity > {QTY_LIMIT}) FROM {sv}.fact_orders"),
        ("silver.bulk_orders: строк", f"SELECT count(*) FROM {sv}.bulk_orders"),
        ("fact_orders: max(total_quantity)", f"SELECT max(total_quantity) FROM {sv}.fact_orders"),
    ]
    rows = []
    for name, sql in checks:
        v = s.scalar(sql)
        rows.append({"check": name, "value": v})
        info(f"DQ | {name}: {v}")
    write_csv("dq_checks.csv", rows)


def sorted_rowgroups(s: Session) -> None:
    """Диапазоны total_quantity по row group во всех файлах fact_orders.
    Каждый файл пишет свой писатель Trino, и отсортирован он сам по себе."""
    try:
        rows = []
        for path in s.data_files("silver.fact_orders"):
            rows += [{"file": path.rsplit("/", 1)[-1], "row_group": i, "rows": n,
                      "min_total_quantity": lo, "max_total_quantity": hi}
                     for i, n, lo, hi in rowgroup_ranges(path, "total_quantity")]
        write_csv("fact_orders_rowgroups.csv", rows)
    except Exception as e:  # noqa: BLE001
        info(f"row group прочитать не удалось: {e}")


def check_gold(s: Session) -> None:
    info(f"сверка: Q18 прямо по tpch.{SCALE}")
    direct = s.run(q18_sql(f"tpch.{SCALE}"))
    gold = s.run(f"SELECT cust_name, custkey, orderkey, orderdate, totalprice, total_quantity "
                 f"FROM {CATALOG}.gold.q18_answer ORDER BY rank_no")
    write_csv("gold_q18_answer.csv", [dict(zip(gold.columns, r)) for r in gold.rows])
    for t in ["bulk_orders_by_year", "order_qty_histogram"]:
        r = s.run(f"SELECT * FROM {CATALOG}.gold.{t} ORDER BY 1, 2")
        write_csv(f"gold_{t}.csv", [dict(zip(r.columns, x)) for x in r.rows])

    n_bulk = s.scalar(f"SELECT count(*) FROM (SELECT orderkey FROM tpch.{SCALE}.lineitem "
                      f"GROUP BY orderkey HAVING sum(quantity) > {QTY_LIMIT})")
    md = ["# Сверка витрины Gold с прямым Q18\n",
          f"В tpch.{SCALE} заказов с суммарным количеством > {QTY_LIMIT}: **{n_bulk}**, "
          f"в топ-100 из них попадает {min(n_bulk, 100)}.\n",
          "| # | orderkey (Q18) | orderkey (Gold) | custkey Q18 / Gold | totalprice Q18 | totalprice Gold | qty Q18 | qty Gold |",
          "|---:|---:|---:|---|---:|---:|---:|---:|"]
    same_all = len(direct.rows) == len(gold.rows)
    for i, (d, g) in enumerate(zip(direct.rows, gold.rows), 1):
        same = (d[2] == g[2] and d[1] == g[1] and abs(float(d[4]) - float(Decimal(str(g[4])))) < 0.005
                and abs(float(d[5]) - float(Decimal(str(g[5])))) < 0.005)
        same_all &= same
        if i <= 10 or not same:
            md.append(f"| {i} | {d[2]} | {g[2]} | {d[1]} / {g[1]} | {float(d[4]):,.2f} | {float(g[4]):,.2f} | "
                      f"{float(d[5]):.0f} | {float(g[5]):.0f} |")
    md.append(f"\nСтрок: Q18 {len(direct.rows)}, Gold {len(gold.rows)}. "
              f"**Результат: {'совпадает' if same_all else 'ЕСТЬ РАСХОЖДЕНИЯ'}** "
              f"(сравнивались все строки, в таблице первые 10).\n")

    info("сверка: эталонный ответ TPC-H для SF1")
    sf1 = s.run(q18_sql("tpch.sf1"))
    top = sf1.rows[0] if sf1.rows else [None] * 6
    sf1_ok = (len(sf1.rows) == REF_SF1_ROWS and top[1] == REF_SF1_TOP["custkey"]
              and top[2] == REF_SF1_TOP["orderkey"] and str(top[3]) == REF_SF1_TOP["orderdate"]
              and abs(float(top[4]) - REF_SF1_TOP["totalprice"]) < 0.005
              and abs(float(top[5]) - REF_SF1_TOP["quantity"]) < 0.005)
    md += ["## Эталон из спецификации TPC-H (SF1)\n",
           "| Источник | строк | custkey | orderkey | orderdate | totalprice | quantity |",
           "|---|---:|---:|---:|---|---:|---:|",
           f"| Спецификация TPC-H | {REF_SF1_ROWS} | {REF_SF1_TOP['custkey']} | {REF_SF1_TOP['orderkey']} | "
           f"{REF_SF1_TOP['orderdate']} | {REF_SF1_TOP['totalprice']:,.2f} | {REF_SF1_TOP['quantity']} |",
           f"| Trino по tpch.sf1 | {len(sf1.rows)} | {top[1]} | {top[2]} | {top[3]} | {float(top[4]):,.2f} | "
           f"{float(top[5]):.0f} |",
           f"\n**Результат: {'совпадает' if sf1_ok else 'ЕСТЬ РАСХОЖДЕНИЯ'}**. На SF1 под условие попадает "
           f"всего {len(sf1.rows)} заказов, так что LIMIT 100 там ничего не обрезает.\n"]
    (RESULTS / "validation.md").write_text("\n".join(md), encoding="utf-8")
    info(f"Gold против Q18: {'OK' if same_all else 'FAIL'}; эталон SF1: {'OK' if sf1_ok else 'FAIL'}")


def main() -> None:
    s = wait_for_trino()
    save_environment(s)
    load_layers()
    s = Session()
    describe_layers(s)
    quality_checks(s)
    sorted_rowgroups(s)
    check_gold(s)
    info("слои готовы, следующий шаг: tools/iceberg_part2.py, потом ./bench.sh")


if __name__ == "__main__":
    main()
