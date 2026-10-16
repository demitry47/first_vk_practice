# Iceberg: эволюция схемы и time travel (протокол прогона)

## 1. Эволюция схемы

**Копия крупных заказов для экспериментов**

```sql
CREATE TABLE iceberg.silver.bulk_orders_evo WITH (format = 'PARQUET') AS SELECT * FROM iceberg.silver.bulk_orders
```

**Схема в начале**

```sql
DESCRIBE iceberg.silver.bulk_orders_evo
```

| Column | Type | Extra | Comment |
|---|---|---|---|
| orderkey | bigint |  |  |
| custkey | bigint |  |  |
| cust_name | varchar |  |  |
| mktsegment | varchar |  |  |
| nation | varchar |  |  |
| region | varchar |  |  |
| orderdate | date |  |  |
| order_year | bigint |  |  |
| totalprice | decimal(12,2) |  |  |
| total_quantity | decimal(15,2) |  |  |

_(всего строк: 11)_

**Добавляем колонку**

```sql
ALTER TABLE iceberg.silver.bulk_orders_evo ADD COLUMN size_class VARCHAR
```

**Переименовываем колонку**

```sql
ALTER TABLE iceberg.silver.bulk_orders_evo RENAME COLUMN total_quantity TO total_qty
```

**Расширяем тип суммы: DECIMAL(12,2) -> DECIMAL(18,2)**

```sql
ALTER TABLE iceberg.silver.bulk_orders_evo ALTER COLUMN totalprice SET DATA TYPE DECIMAL(18, 2)
```

**Схема после ALTER**

```sql
DESCRIBE iceberg.silver.bulk_orders_evo
```

| Column | Type | Extra | Comment |
|---|---|---|---|
| orderkey | bigint |  |  |
| custkey | bigint |  |  |
| cust_name | varchar |  |  |
| mktsegment | varchar |  |  |
| nation | varchar |  |  |
| region | varchar |  |  |
| orderdate | date |  |  |
| order_year | bigint |  |  |
| totalprice | decimal(18,2) |  |  |
| total_qty | decimal(15,2) |  |  |

_(всего строк: 12)_

**Старые файлы читаются уже по новой схеме**

```sql
SELECT orderkey, cust_name, total_qty, totalprice, size_class FROM iceberg.silver.bulk_orders_evo ORDER BY orderkey LIMIT 5
```

| orderkey | cust_name | total_qty | totalprice | size_class |
|---|---|---|---|---|
| 6882 | Customer#000070978 | 303.00 | 522208.58 | NULL |
| 29158 | Customer#000266129 | 305.00 | 414117.20 | NULL |
| 502886 | Customer#000065534 | 312.00 | 436520.22 | NULL |
| 551136 | Customer#000458344 | 308.00 | 456462.18 | NULL |
| 565574 | Customer#000564391 | 301.00 | 483333.08 | NULL |

Файлов данных до ALTER: **1**, после: **1**. Поменялся только metadata.json с новой версией схемы. Iceberg связывает колонки с данными по id поля, а не по имени, поэтому переименование и расширение типа файлы не трогают. Расширение типа: получилось.

**Заполняем size_class**

```sql
UPDATE iceberg.silver.bulk_orders_evo SET size_class = CASE WHEN total_qty >= 320 THEN 'XL' ELSE 'L' END
```

**Сколько заказов в каждом классе**

```sql
SELECT size_class, count(*) AS orders FROM iceberg.silver.bulk_orders_evo GROUP BY size_class ORDER BY 1
```

| size_class | orders |
|---|---|
| L | 233 |
| XL | 16 |

## 2. Сбой, time travel и восстановление

Последний исправный снапшот `414565968008137800` (2026-10-14 16:19:04.294 UTC): {'orders': 249, 'amount': Decimal('113403560.04'), 'zero_prices': 0}

**Сбой ETL: суммы заказов с 1995 года стали нулевыми**

```sql
UPDATE iceberg.silver.bulk_orders_evo SET totalprice = 0 WHERE order_year >= 1995
```

После сбоя: {'orders': 249, 'amount': Decimal('51052351.01'), 'zero_prices': 138}

**Уже после сбоя приходит нормальный новый заказ**

```sql
INSERT INTO iceberg.silver.bulk_orders_evo (orderkey, custkey, cust_name, mktsegment, nation, region, orderdate, order_year, totalprice, total_qty, lines_cnt, size_class) SELECT 900000001, custkey, cust_name, mktsegment, nation, region, DATE '1998-08-01', 1998, 234567.89, total_qty, lines_cnt, 'L' FROM iceberg.silver.bulk_orders_evo ORDER BY orderkey LIMIT 1
```

**Снапшоты таблицы**

```sql
SELECT snapshot_id, committed_at, operation, element_at(summary, 'added-data-files') AS added_files, element_at(summary, 'added-delete-files') AS added_delete_files, element_at(summary, 'total-records') AS total_records FROM iceberg.silver."bulk_orders_evo$snapshots" ORDER BY committed_at
```

| snapshot_id | committed_at | operation | added_files | added_delete_files | total_records |
|---|---|---|---|---|---|
| 5511910823423376963 | 2026-10-14 16:19:03.118000+00:00 | append | 1 | NULL | 249 |
| 414565968008137800 | 2026-10-14 16:19:04.294000+00:00 | overwrite | 1 | 1 | 498 |
| 5722849813937556515 | 2026-10-14 16:19:04.827000+00:00 | overwrite | 1 | 1 | 636 |
| 5418364776711512405 | 2026-10-14 16:19:05.212000+00:00 | append | 1 | NULL | 637 |

**Time travel по номеру снапшота**

```sql
SELECT count(*) AS orders, sum(totalprice) AS amount, count_if(totalprice = 0) AS zero_prices FROM iceberg.silver.bulk_orders_evo FOR VERSION AS OF 414565968008137800
```

| orders | amount | zero_prices |
|---|---|---|
| 249 | 113403560.04 | 0 |

**Time travel по времени коммита**

```sql
SELECT count(*) AS orders, sum(totalprice) AS amount FROM iceberg.silver.bulk_orders_evo FOR TIMESTAMP AS OF TIMESTAMP '2026-10-14 16:19:04.294 UTC'
```

| orders | amount |
|---|---|
| 249 | 113403560.04 |

### Способ A: MERGE только испорченной колонки из снапшота

**Возвращаем totalprice из исправного снапшота**

```sql
MERGE INTO iceberg.silver.bulk_orders_evo t USING (SELECT orderkey, totalprice FROM iceberg.silver.bulk_orders_evo FOR VERSION AS OF 414565968008137800) s ON t.orderkey = s.orderkey WHEN MATCHED AND t.totalprice <> s.totalprice THEN UPDATE SET totalprice = s.totalprice
```

После MERGE: {'orders': 250, 'amount': Decimal('113638127.93'), 'zero_prices': 0}; новый заказ остался: True

### Способ B: откат всей таблицы (rollback_to_snapshot)

**Отдельная копия таблицы в исправном состоянии (CTAS из снапшота)**

```sql
CREATE TABLE iceberg.silver.bulk_orders_restored AS SELECT * FROM iceberg.silver.bulk_orders_evo FOR VERSION AS OF 414565968008137800
```

**Откат таблицы**

```sql
ALTER TABLE iceberg.silver.bulk_orders_evo EXECUTE rollback_to_snapshot(414565968008137800)
```

**Снапшоты таблицы**

```sql
SELECT snapshot_id, committed_at, operation, element_at(summary, 'added-data-files') AS added_files, element_at(summary, 'added-delete-files') AS added_delete_files, element_at(summary, 'total-records') AS total_records FROM iceberg.silver."bulk_orders_evo$snapshots" ORDER BY committed_at
```

| snapshot_id | committed_at | operation | added_files | added_delete_files | total_records |
|---|---|---|---|---|---|
| 5511910823423376963 | 2026-10-14 16:19:03.118000+00:00 | append | 1 | NULL | 249 |
| 414565968008137800 | 2026-10-14 16:19:04.294000+00:00 | overwrite | 1 | 1 | 498 |
| 5722849813937556515 | 2026-10-14 16:19:04.827000+00:00 | overwrite | 1 | 1 | 636 |
| 5418364776711512405 | 2026-10-14 16:19:05.212000+00:00 | append | 1 | NULL | 637 |
| 804178375977114998 | 2026-10-14 16:19:05.991000+00:00 | overwrite | 1 | 1 | 775 |

## Сводка

| Состояние | Заказов | Сумма totalprice | Нулевых сумм | Новый заказ |
|---|---:|---:|---:|---|
| Исправный снапшот | 249 | 113403560.04 | 0 | — |
| После сбоя ETL | 249 | 51052351.01 | 138 | — |
| + новый заказ | 250 | 51286918.90 | 138 | есть |
| A: MERGE из снапшота | 250 | 113638127.93 | 0 | остался |
| B: rollback_to_snapshot | 249 | 113403560.04 | 0 | пропал |
