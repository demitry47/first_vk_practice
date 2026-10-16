# Сводка по всем замерам (собрана tools/summary.py)

## Окружение

| Параметр | Значение |
|---|---|
| Trino | 476 (узлов: 1) |
| Масштаб | sf4 |
| CPU в Docker VM | 12 |
| RAM в Docker VM | 7.6 ГБ |
| Ядро Docker VM | 6.18.40.1-microsoft-standard-WSL2 |

## Слои медальона

### bronze

| Таблица | Строк | Колонок | Файлов | МБ | Формат | Кодек в футере | Row group в макс. файле | Партиции | Сортировка |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `iceberg.bronze.region` | 5 | 5 | 1 | 0,001 | PARQUET | ZSTD | 1 | нет | нет |
| `iceberg.bronze.nation` | 25 | 6 | 1 | 0,002 | PARQUET | ZSTD | 1 | нет | нет |
| `iceberg.bronze.customer` | 600 000 | 10 | 2 | 32,665 | PARQUET | ZSTD | 1 | нет | нет |
| `iceberg.bronze.orders` | 6 000 000 | 11 | 8 | 152,394 | PARQUET | ZSTD | 1 | нет | нет |
| `iceberg.bronze.lineitem` | 23 996 604 | 18 | 12 | 622,933 | PARQUET | ZSTD | 1 | нет | нет |

### silver

| Таблица | Строк | Колонок | Файлов | МБ | Формат | Кодек в футере | Row group в макс. файле | Партиции | Сортировка |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `iceberg.silver.dim_customer` | 600 000 | 8 | 1 | 22,742 | PARQUET | ZSTD | 4 | нет | нет |
| `iceberg.silver.fact_orders` | 6 000 000 | 14 | 6 | 137,524 | PARQUET | ZSTD | 14 | нет | total_quantity ASC NULLS FIRST |
| `iceberg.silver.bulk_orders` | 249 | 11 | 1 | 0,007 | PARQUET | ZSTD | 1 | нет | нет |

### gold

| Таблица | Строк | Колонок | Файлов | МБ | Формат | Кодек в футере | Row group в макс. файле | Партиции | Сортировка |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `iceberg.gold.q18_answer` | 100 | 9 | 1 | 0,004 | PARQUET | ZSTD | 1 | нет | нет |
| `iceberg.gold.bulk_orders_by_year` | 175 | 8 | 1 | 0,003 | PARQUET | ZSTD | 1 | нет | нет |
| `iceberg.gold.order_qty_histogram` | 14 | 6 | 1 | 0,001 | PARQUET | ZSTD | 1 | нет | нет |

### Время построения

| Слой | Таблица | Строк | Время, с | CPU, с | Пик памяти, МБ |
|---|---:|---:|---:|---:|---:|
| bronze | `iceberg.bronze.region` | 5 | 0,22 | 0,1 | 0 |
| bronze | `iceberg.bronze.nation` | 25 | 0,24 | 0,1 | 0 |
| bronze | `iceberg.bronze.customer` | 600 000 | 12,45 | 17,8 | 54 |
| bronze | `iceberg.bronze.orders` | 6 000 000 | 9,36 | 50,6 | 229 |
| bronze | `iceberg.bronze.lineitem` | 23 996 604 | 34,28 | 230,4 | 825 |
| silver | `iceberg.silver.dim_customer` | 600 000 | 7,25 | 9,6 | 136 |
| silver | `iceberg.silver.fact_orders` | 6 000 000 | 34,14 | 159,0 | 1 394 |
| silver | `iceberg.silver.bulk_orders` | 249 | 1,10 | 1,1 | 74 |
| gold | `iceberg.gold.q18_answer` | 100 | 0,52 | 0,0 | 0 |
| gold | `iceberg.gold.bulk_orders_by_year` | 175 | 2,56 | 15,6 | 187 |
| gold | `iceberg.gold.order_qty_histogram` | 14 | 0,35 | 1,5 | 0 |

### Проверки качества

| Проверка | Значение |
|---|---:|
| bronze.lineitem: строк | 23996604 |
| lineitem: orderkey IS NULL | 0 |
| lineitem: quantity <= 0 | 0 |
| lineitem: extendedprice <= 0 | 0 |
| lineitem: discount или tax вне [0;1] | 0 |
| lineitem: позиции без заказа | 0 |
| bronze.orders: строк | 6000000 |
| orders: заказы без позиций | 0 |
| silver.fact_orders: строк | 6000000 |
| fact_orders: price_ok = false | 0 |
| fact_orders: макс. |totalprice − net_amount| | 0.1148 |
| fact_orders: макс. |totalprice − net_amount| / lines_cnt | 0.0205 |
| orders: totalprice ≠ сумме позиций с усечением как в генераторе | 0 |
| bronze.customer: строк | 600000 |
| customer: повторы custkey | 0 |
| silver.dim_customer: строк | 600000 |
| dim_customer: nation IS NULL | 0 |
| fact_orders: total_quantity > 300 | 249 |
| silver.bulk_orders: строк | 249 |
| fact_orders: max(total_quantity) | 328.00 |

## Бенчмарк Bronze (customer + orders + lineitem)

Записей на вариант: 3, кругов чтения после прогрева: 5. Везде медиана, в скобках min–max, жирным лучшая медиана в столбце.

| Вариант | Кодек в футере | Bronze, МБ | lineitem, МБ | Сжатие к NONE | Запись, с | Запись без генерации, с | Q18, с | Q18 прочитано, МБ | LIKE_SCAN, с | LIKE_SCAN прочитано, МБ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PARQUET + NONE | UNCOMPRESSED | 2 091,5 | 1 512,2 | 1,00× | 43,1 (43,0–46,5) | 15,4 | 2,94 (2,70–3,21) | 532,7 | 1,47 (1,39–1,60) | 758,2 |
| PARQUET + SNAPPY | SNAPPY | 1 112,4 | 834,5 | 1,88× | 43,5 (41,5–47,7) | 15,8 | 2,53 (2,40–2,82) | 229,2 | 1,29 (1,22–1,35) | 332,1 |
| PARQUET + GZIP | GZIP | **788,5** | 609,4 | 2,65× | 62,3 (61,1–71,2) | 34,5 | 2,77 (2,51–3,21) | 139,8 | 1,27 (1,25–1,35) | 219,8 |
| PARQUET + ZSTD | ZSTD | 808,5 | 623,4 | 2,59× | 42,7 (39,5–44,3) | 14,9 | **2,43 (2,32–2,79)** | 129,0 | 1,08 (1,06–1,24) | 233,3 |
| ORC + NONE | UNCOMPRESSED | 2 330,5 | 1 838,5 | 1,00× | 44,8 (44,7–53,6) | 17,1 | 3,33 (3,05–4,49) | 554,8 | 2,05 (1,92–2,90) | 899,6 |
| ORC + SNAPPY | SNAPPY | 1 147,1 | 910,3 | 2,03× | 40,4 (39,5–41,5) | 12,6 | 2,80 (2,76–3,21) | 249,1 | 1,05 (1,00–1,50) | 341,6 |
| ORC + GZIP | ZLIB | 808,0 | 640,4 | 2,88× | 50,3 (47,9–54,4) | 22,5 | 2,77 (2,67–2,97) | 169,4 | 1,18 (1,11–1,33) | 223,1 |
| ORC + ZSTD | ZSTD | 830,0 | 660,2 | 2,81× | **38,0 (37,9–39,8)** | 10,3 | 3,30 (3,14–3,40) | 175,8 | **0,92 (0,92–1,09)** | 231,3 |

Генерация тех же таблиц коннектором tpch без записи: **27,8 с** (медиана).

### Первый запуск после записи и CPU

| Вариант | Q18 первый, с | Q18 CPU, с | LIKE_SCAN первый, с | LIKE_SCAN CPU, с | Запись CPU, с |
|---|---:|---:|---:|---:|---:|
| PARQUET + NONE | 5,30 | 15,3 | 1,79 | 5,7 | 230 |
| PARQUET + SNAPPY | 2,91 | 15,7 | 1,10 | 6,7 | 232 |
| PARQUET + GZIP | 2,36 | 16,2 | 1,17 | 7,4 | 364 |
| PARQUET + ZSTD | 2,61 | 14,8 | 0,97 | 6,2 | 239 |
| ORC + NONE | 3,58 | 18,3 | 1,67 | 5,2 | 238 |
| ORC + SNAPPY | 3,61 | 18,5 | 1,02 | 5,9 | 230 |
| ORC + GZIP | 2,93 | 18,4 | 1,32 | 6,8 | 294 |
| ORC + ZSTD | 2,97 | 19,0 | 0,87 | 5,4 | 241 |

### GZIP и ZSTD (Parquet), как в форме отчета

| Сжатие на Bronze | Время запроса | Размер Bronze, МБ | Вывод |
|---|---:|---:|---:|
| GZIP | 2,77 с | 788,5 |  |
| ZSTD | 2,43 с | 808,5 |  |

## Раскладка Silver и слои медальона (Q18)

| Источник | Таблица | Файлов | МБ | Время, с | CPU, с | Прочитано, МБ | Строк обработано |
|---|---:|---:|---:|---:|---:|---:|---:|
| Bronze: Q18 как в задании | `bronze.lineitem` | 12 | 622,93 | 2,37 (2,29–2,84) | 16,23 | 133,00 | 55 193 457 |
| Silver: fact_orders без сортировки | `bench_layout.fact_orders_unsorted` | 6 | 133,45 | 0,36 (0,33–0,97) | 1,08 | 78,40 | 7 200 000 |
| Silver: fact_orders, sorted_by total_quantity | `silver.fact_orders` | 6 | 137,52 | 0,19 (0,17–0,39) | 0,23 | 10,99 | 1 751 658 |
| Gold: витрина q18_answer | `gold.q18_answer` | 1 | 0,00 | 0,04 (0,04–0,07) | 0,00 | 0,00 | 100 |

- silver.fact_orders (sorted_by): файлов 6, row group 34 (6 000 000 строк). У **6** из них max(total_quantity) > 300 (551 658 строк), пропустить их Trino не может.
- fact_orders без сортировки: файлов 6, row group 35 (6 000 000 строк). У **35** из них max(total_quantity) > 300 (6 000 000 строк), пропустить их Trino не может.

### Ожидание очистки бакета между записями

| Момент | Бакет до, МБ | Бакет после, МБ | Ждали, с |
|---|---:|---:|---:|
| PARQUET+NONE, после записи 1 | 3060.6 | 3060.6 | 20 |
| PARQUET+NONE, после записи 2 | 3060.6 | 3060.6 | 20 |
| вариант PARQUET+NONE готов | 3060.7 | 3060.7 | 20 |
| PARQUET+SNAPPY, после записи 1 | 4173.5 | 4173.5 | 20 |
| PARQUET+SNAPPY, после записи 2 | 4173.6 | 4173.6 | 20 |
| вариант PARQUET+SNAPPY готов | 4173.6 | 4173.6 | 20 |
| PARQUET+GZIP, после записи 1 | 4962.3 | 4962.3 | 20 |
| PARQUET+GZIP, после записи 2 | 4962.3 | 4962.3 | 20 |
| вариант PARQUET+GZIP готов | 4962.3 | 4962.3 | 20 |
| PARQUET+ZSTD, после записи 1 | 5771.2 | 5771.2 | 20 |
| PARQUET+ZSTD, после записи 2 | 5771.3 | 5771.3 | 20 |
| вариант PARQUET+ZSTD готов | 5771.1 | 5771.1 | 20 |
| ORC+NONE, после записи 1 | 8514.7 | 8102.1 | 40 |
| ORC+NONE, после записи 2 | 8102.1 | 8102.1 | 20 |
| вариант ORC+NONE готов | 8102.1 | 8102.1 | 20 |
| ORC+SNAPPY, после записи 1 | 9249.4 | 9249.4 | 20 |
| ORC+SNAPPY, после записи 2 | 9249.4 | 9249.4 | 20 |
| вариант ORC+SNAPPY готов | 9249.3 | 9249.3 | 20 |
| ORC+GZIP, после записи 1 | 10057.8 | 10057.8 | 20 |
| ORC+GZIP, после записи 2 | 10057.9 | 10057.9 | 20 |
| вариант ORC+GZIP готов | 10057.8 | 10057.8 | 20 |
| ORC+ZSTD, после записи 1 | 10888.3 | 10888.3 | 20 |
| ORC+ZSTD, после записи 2 | 10888.2 | 10888.2 | 20 |
| вариант ORC+ZSTD готов | 10888.2 | 10888.2 | 20 |

![Размер и запись](charts/storage_write.png)

![Чтение](charts/read_speed.png)

# Сверка витрины Gold с прямым Q18

В tpch.sf4 заказов с суммарным количеством > 300: **249**, в топ-100 из них попадает 100.

| # | orderkey (Q18) | orderkey (Gold) | custkey Q18 / Gold | totalprice Q18 | totalprice Gold | qty Q18 | qty Gold |
|---:|---:|---:|---|---:|---:|---:|---:|
| 1 | 22897223 | 22897223 | 328669 / 328669 | 564,350.60 | 564,350.60 | 309 | 309 |
| 2 | 17366275 | 17366275 | 66835 / 66835 | 539,704.09 | 539,704.09 | 316 | 316 |
| 3 | 18449123 | 18449123 | 538331 / 538331 | 537,736.85 | 537,736.85 | 313 | 313 |
| 4 | 22220514 | 22220514 | 44407 / 44407 | 535,926.78 | 535,926.78 | 306 | 306 |
| 5 | 21135524 | 21135524 | 155789 / 155789 | 532,920.05 | 532,920.05 | 320 | 320 |
| 6 | 16139072 | 16139072 | 526468 / 526468 | 526,055.27 | 526,055.27 | 304 | 304 |
| 7 | 11231077 | 11231077 | 205100 / 205100 | 522,991.24 | 522,991.24 | 304 | 304 |
| 8 | 6882 | 6882 | 70978 / 70978 | 522,208.58 | 522,208.58 | 303 | 303 |
| 9 | 4722021 | 4722021 | 512479 / 512479 | 518,421.75 | 518,421.75 | 323 | 323 |
| 10 | 4267751 | 4267751 | 430360 / 430360 | 515,053.98 | 515,053.98 | 301 | 301 |

Строк: Q18 100, Gold 100. **Результат: совпадает** (сравнивались все строки, в таблице первые 10).

## Эталон из спецификации TPC-H (SF1)

| Источник | строк | custkey | orderkey | orderdate | totalprice | quantity |
|---|---:|---:|---:|---|---:|---:|
| Спецификация TPC-H | 57 | 128120 | 4722021 | 1994-04-07 | 544,089.09 | 323 |
| Trino по tpch.sf1 | 57 | 128120 | 4722021 | 1994-04-07 | 544,089.09 | 323 |

**Результат: совпадает**. На SF1 под условие попадает всего 57 заказов, так что LIMIT 100 там ничего не обрезает.

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
