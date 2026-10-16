"""Часть 2 (п. 6.1): эволюция схемы и time travel на Silver.

Работаем на копии silver.bulk_orders, чтобы не трогать рабочий слой.
  1. Меняем схему, не переписывая данные: добавляем size_class, переименовываем
     total_quantity -> total_qty, расширяем totalprice до DECIMAL(18,2).
  2. Заполняем size_class через UPDATE. Этот снапшот считаем "хорошим".
  3. Имитируем сбой ETL: UPDATE обнуляет totalprice у заказов с 1995 года.
  4. После сбоя в таблицу приходит нормальный новый заказ (INSERT).
  5. Читаем "хорошее" состояние через FOR VERSION AS OF и FOR TIMESTAMP AS OF.
  6. Способ A: MERGE возвращает из снапшота только испорченную колонку, новый заказ остается.
  7. Способ B: rollback_to_snapshot возвращает таблицу целиком, новый заказ пропадает.

    docker compose run --rm -T toolbox python tools/iceberg_part2.py
Протокол пишется в results/iceberg_demo.md
"""
from __future__ import annotations

from common import CATALOG, RESULTS, Session, info

T = f"{CATALOG}.silver.bulk_orders_evo"
SNAPSHOTS = f'{CATALOG}.silver."bulk_orders_evo$snapshots"'
FILES = f'{CATALOG}.silver."bulk_orders_evo$files"'
NEW_ORDER_KEY = 900000001
NEW_ORDER_PRICE = "234567.89"
journal: list[str] = ["# Iceberg: эволюция схемы и time travel (протокол прогона)\n"]


def step(s: Session, title: str, sql: str, show: bool = True, limit: int = 10):
    info(title)
    r = s.run(sql)
    journal.append(f"**{title}**\n\n```sql\n{sql.strip()}\n```\n")
    if show and r.columns:
        journal.append("| " + " | ".join(r.columns) + " |")
        journal.append("|" + "---|" * len(r.columns))
        for row in r.rows[:limit]:
            journal.append("| " + " | ".join("NULL" if v is None else str(v) for v in row) + " |")
        if len(r.rows) > limit:
            journal.append(f"\n_(всего строк: {len(r.rows)})_")
        journal.append("")
    return r


def totals(s: Session) -> dict:
    n, amount, zeros = s.run(f"SELECT count(*), sum(totalprice), count_if(totalprice = 0) FROM {T}").rows[0]
    return {"orders": n, "amount": amount, "zero_prices": zeros}


def history(s: Session):
    return step(s, "Снапшоты таблицы",
                f"SELECT snapshot_id, committed_at, operation, "
                f"element_at(summary, 'added-data-files') AS added_files, "
                f"element_at(summary, 'added-delete-files') AS added_delete_files, "
                f"element_at(summary, 'total-records') AS total_records "
                f"FROM {SNAPSHOTS} ORDER BY committed_at")


def main() -> None:
    s = Session(compression_codec="ZSTD")
    s.run(f"DROP TABLE IF EXISTS {T}")
    s.run(f"DROP TABLE IF EXISTS {CATALOG}.silver.bulk_orders_restored")

    journal.append("## 1. Эволюция схемы\n")
    step(s, "Копия крупных заказов для экспериментов",
         f"CREATE TABLE {T} WITH (format = 'PARQUET') AS SELECT * FROM {CATALOG}.silver.bulk_orders", show=False)
    step(s, "Схема в начале", f"DESCRIBE {T}")
    files_before = s.scalar(f"SELECT count(*) FROM {FILES}")
    step(s, "Добавляем колонку", f"ALTER TABLE {T} ADD COLUMN size_class VARCHAR", show=False)
    step(s, "Переименовываем колонку", f"ALTER TABLE {T} RENAME COLUMN total_quantity TO total_qty", show=False)
    widened = True
    try:
        step(s, "Расширяем тип суммы: DECIMAL(12,2) -> DECIMAL(18,2)",
             f"ALTER TABLE {T} ALTER COLUMN totalprice SET DATA TYPE DECIMAL(18, 2)", show=False)
    except Exception as e:  # noqa: BLE001
        widened = False
        journal.append(f"_Эта версия не умеет менять тип: `{str(e)[:160]}`_\n")
    step(s, "Схема после ALTER", f"DESCRIBE {T}")
    files_after = s.scalar(f"SELECT count(*) FROM {FILES}")
    step(s, "Старые файлы читаются уже по новой схеме",
         f"SELECT orderkey, cust_name, total_qty, totalprice, size_class FROM {T} ORDER BY orderkey LIMIT 5")
    journal.append(f"Файлов данных до ALTER: **{files_before}**, после: **{files_after}**. "
                   f"Поменялся только metadata.json с новой версией схемы. Iceberg связывает колонки "
                   f"с данными по id поля, а не по имени, поэтому переименование и расширение типа "
                   f"файлы не трогают. Расширение типа: {'получилось' if widened else 'не поддерживается'}.\n")

    step(s, "Заполняем size_class",
         f"UPDATE {T} SET size_class = CASE WHEN total_qty >= 320 THEN 'XL' ELSE 'L' END", show=False)
    step(s, "Сколько заказов в каждом классе",
         f"SELECT size_class, count(*) AS orders FROM {T} GROUP BY size_class ORDER BY 1")

    journal.append("## 2. Сбой, time travel и восстановление\n")
    good_id = s.scalar(f"SELECT snapshot_id FROM {SNAPSHOTS} ORDER BY committed_at DESC LIMIT 1")
    good_ts = s.scalar(f"SELECT CAST(committed_at AS varchar) FROM {SNAPSHOTS} WHERE snapshot_id = {good_id}")
    good = totals(s)
    journal.append(f"Последний исправный снапшот `{good_id}` ({good_ts}): {good}\n")

    step(s, "Сбой ETL: суммы заказов с 1995 года стали нулевыми",
         f"UPDATE {T} SET totalprice = 0 WHERE order_year >= 1995", show=False)
    broken = totals(s)
    journal.append(f"После сбоя: {broken}\n")
    step(s, "Уже после сбоя приходит нормальный новый заказ",
         f"INSERT INTO {T} (orderkey, custkey, cust_name, mktsegment, nation, region, orderdate, "
         f"order_year, totalprice, total_qty, lines_cnt, size_class) "
         f"SELECT {NEW_ORDER_KEY}, custkey, cust_name, mktsegment, nation, region, DATE '1998-08-01', "
         f"1998, {NEW_ORDER_PRICE}, total_qty, lines_cnt, 'L' FROM {T} ORDER BY orderkey LIMIT 1", show=False)
    with_new = totals(s)
    history(s)

    step(s, "Time travel по номеру снапшота", f"SELECT count(*) AS orders, sum(totalprice) AS amount, "
         f"count_if(totalprice = 0) AS zero_prices FROM {T} FOR VERSION AS OF {good_id}")
    step(s, "Time travel по времени коммита", f"SELECT count(*) AS orders, sum(totalprice) AS amount "
         f"FROM {T} FOR TIMESTAMP AS OF TIMESTAMP '{good_ts}'")

    journal.append("### Способ A: MERGE только испорченной колонки из снапшота\n")
    step(s, "Возвращаем totalprice из исправного снапшота",
         f"MERGE INTO {T} t USING (SELECT orderkey, totalprice FROM {T} FOR VERSION AS OF {good_id}) s "
         f"ON t.orderkey = s.orderkey "
         f"WHEN MATCHED AND t.totalprice <> s.totalprice THEN UPDATE SET totalprice = s.totalprice", show=False)
    merged = totals(s)
    kept = s.scalar(f"SELECT count(*) FROM {T} WHERE orderkey = {NEW_ORDER_KEY}")
    journal.append(f"После MERGE: {merged}; новый заказ остался: {bool(kept)}\n")

    journal.append("### Способ B: откат всей таблицы (rollback_to_snapshot)\n")
    step(s, "Отдельная копия таблицы в исправном состоянии (CTAS из снапшота)",
         f"CREATE TABLE {CATALOG}.silver.bulk_orders_restored AS SELECT * FROM {T} FOR VERSION AS OF {good_id}",
         show=False)
    try:
        step(s, "Откат таблицы", f"ALTER TABLE {T} EXECUTE rollback_to_snapshot({good_id})", show=False)
    except Exception as e:  # noqa: BLE001
        info(f"ALTER ... EXECUTE не сработал ({str(e)[:80]}), вызываю процедуру")
        step(s, "Откат таблицы через процедуру",
             f"CALL {CATALOG}.system.rollback_to_snapshot('silver', 'bulk_orders_evo', {good_id})", show=False)
    rolled = totals(s)
    kept_after_rb = s.scalar(f"SELECT count(*) FROM {T} WHERE orderkey = {NEW_ORDER_KEY}")
    history(s)

    journal.extend(["## Сводка\n", "| Состояние | Заказов | Сумма totalprice | Нулевых сумм | Новый заказ |",
                    "|---|---:|---:|---:|---|",
                    f"| Исправный снапшот | {good['orders']} | {good['amount']} | {good['zero_prices']} | — |",
                    f"| После сбоя ETL | {broken['orders']} | {broken['amount']} | {broken['zero_prices']} | — |",
                    f"| + новый заказ | {with_new['orders']} | {with_new['amount']} | {with_new['zero_prices']} | есть |",
                    f"| A: MERGE из снапшота | {merged['orders']} | {merged['amount']} | {merged['zero_prices']} | "
                    f"{'остался' if kept else 'пропал'} |",
                    f"| B: rollback_to_snapshot | {rolled['orders']} | {rolled['amount']} | {rolled['zero_prices']} | "
                    f"{'остался' if kept_after_rb else 'пропал'} |", ""])
    (RESULTS / "iceberg_demo.md").write_text("\n".join(journal), encoding="utf-8")
    info(f"готово: good={good} broken={broken} merged={merged} rollback={rolled}")


if __name__ == "__main__":
    main()
