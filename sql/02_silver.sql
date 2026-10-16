-- ============================================================================
-- SILVER. Очищенные и обогащенные сущности, на которых строятся витрины.
--
-- dim_customer  клиент вместе со страной и регионом, без дублей по custkey
-- fact_orders   одна строка на заказ: поля заказа + итоги по его позициям
--               (сколько позиций, сколько товара, суммы, возвраты).
--               Пишется с sorted_by = total_quantity: фильтр Q18 total_quantity > 300
--               отсекает row group по их min/max, и читать почти ничего не надо.
-- bulk_orders   заказы с total_quantity > 300 вместе с клиентом, то есть подзапрос
--               Q18, сохраненный отдельной таблицей
--
-- Очистка:      пустые ключи и даты, quantity/extendedprice <= 0, discount и tax
--               вне [0;1], позиции без заказа (inner join), дубли клиентов
-- Нормализация: деньги и количество DOUBLE -> DECIMAL, коды через upper(trim())
-- Обогащение:   страна/регион клиента, итоги по позициям, флаг price_ok
--               (totalprice сходится с суммой позиций по формуле TPC-H
--               sum(extendedprice * (1 - discount) * (1 + tax)) с допуском на
--               усечение до копеек в генераторе)
-- ============================================================================

create schema if not exists iceberg.silver;

drop table if exists iceberg.silver.dim_customer;
create table iceberg.silver.dim_customer with (format = 'PARQUET') as
with dedup as (
    select c.*,
           row_number() over (partition by c.custkey order by c._loaded_at desc) as rn
    from iceberg.bronze.customer c
    where c.custkey is not null
)
select
    d.custkey,
    trim(d.name)                      as cust_name,
    trim(d.address)                   as address,
    trim(d.phone)                     as phone,
    cast(d.acctbal as decimal(12, 2)) as acctbal,
    upper(trim(d.mktsegment))         as mktsegment,
    upper(trim(n.name))               as nation,
    upper(trim(r.name))               as region
from dedup d
left join iceberg.bronze.nation n on n.nationkey = d.nationkey
left join iceberg.bronze.region r on r.regionkey = n.regionkey
where d.rn = 1;

drop table if exists iceberg.silver.fact_orders;
create table iceberg.silver.fact_orders
with (format = 'PARQUET', sorted_by = ARRAY['total_quantity']) as
with per_order as (
    select
        li.orderkey,
        count(*)                                                           as lines_cnt,
        cast(sum(cast(li.quantity as decimal(12, 2))) as decimal(15, 2))      as total_quantity,
        cast(sum(cast(li.extendedprice as decimal(12, 2))) as decimal(18, 2)) as gross_amount,
        cast(sum(cast(li.extendedprice as decimal(12, 2))
                 * (1 - cast(li.discount as decimal(4, 2)))
                 * (1 + cast(li.tax as decimal(4, 2)))) as decimal(18, 4))   as net_amount,
        count_if(li.returnflag = 'R')                                      as returned_cnt,
        max(li.receiptdate)                                                as last_receiptdate
    from iceberg.bronze.lineitem li
    where li.orderkey is not null
      and li.quantity > 0
      and li.extendedprice > 0
      and li.discount between 0 and 1
      and li.tax between 0 and 1
    group by li.orderkey
)
select
    o.orderkey,
    o.custkey,
    o.orderdate,
    year(o.orderdate)                    as order_year,
    upper(trim(o.orderstatus))           as orderstatus,
    upper(trim(o.orderpriority))         as orderpriority,
    cast(o.totalprice as decimal(12, 2)) as totalprice,
    p.lines_cnt,
    p.total_quantity,
    p.gross_amount,
    p.net_amount,
    -- генератор TPC-H считает totalprice в копейках и на каждой позиции два раза
    -- отбрасывает дробную часть (после скидки и после налога): до 2,06 коп. на позицию,
    -- поэтому допуск 0,021 * число позиций
    abs(cast(o.totalprice as decimal(12, 2)) - p.net_amount)
        <= 0.021 * p.lines_cnt           as price_ok,
    p.returned_cnt,
    p.last_receiptdate
from iceberg.bronze.orders o
join per_order p on p.orderkey = o.orderkey
where o.custkey is not null
  and o.orderdate is not null;

drop table if exists iceberg.silver.bulk_orders;
create table iceberg.silver.bulk_orders with (format = 'PARQUET') as
select
    f.orderkey,
    f.custkey,
    c.cust_name,
    c.mktsegment,
    c.nation,
    c.region,
    f.orderdate,
    f.order_year,
    f.totalprice,
    f.total_quantity,
    f.lines_cnt
from iceberg.silver.fact_orders f
join iceberg.silver.dim_customer c on c.custkey = f.custkey
where f.total_quantity > 300;
