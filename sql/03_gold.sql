-- ============================================================================
-- GOLD. Витрины, которые смотрит BI (Superset).
--
-- q18_answer          ответ на Q18: 100 самых дорогих крупных заказов
-- bulk_orders_by_year крупные заказы по годам, сегментам и регионам и их доля
--                     среди всех заказов той же ячейки
-- order_qty_histogram сколько заказов приходится на каждый интервал суммарного
--                     количества товара (шаг 25); видно, как редко бывает > 300
-- ============================================================================

create schema if not exists iceberg.gold;

drop table if exists iceberg.gold.q18_answer;
create table iceberg.gold.q18_answer with (format = 'PARQUET') as
select
    cast(row_number() over (order by totalprice desc, orderdate, orderkey) as integer) as rank_no,
    cust_name,
    custkey,
    orderkey,
    orderdate,
    totalprice,
    total_quantity,
    mktsegment,
    nation
from iceberg.silver.bulk_orders
order by totalprice desc, orderdate, orderkey
limit 100;

drop table if exists iceberg.gold.bulk_orders_by_year;
create table iceberg.gold.bulk_orders_by_year with (format = 'PARQUET') as
select
    f.order_year,
    c.mktsegment,
    c.region,
    count(*)                                                            as all_orders,
    count_if(f.total_quantity > 300)                                    as bulk_orders,
    count(distinct case when f.total_quantity > 300 then f.custkey end) as bulk_customers,
    -- множитель 100e0 (DOUBLE): с литералом 100.0 Trino делит в DECIMAL с одним знаком
    -- после запятой, и доли в сотые доли процента превращаются в 0,0
    cast(100e0 * count_if(f.total_quantity > 300) / count(*) as decimal(9, 4)) as bulk_share_pct,
    sum(case when f.total_quantity > 300 then f.totalprice end)        as bulk_amount
from iceberg.silver.fact_orders f
join iceberg.silver.dim_customer c on c.custkey = f.custkey
group by f.order_year, c.mktsegment, c.region;

drop table if exists iceberg.gold.order_qty_histogram;
create table iceberg.gold.order_qty_histogram with (format = 'PARQUET') as
with bucketed as (
    select cast(floor((total_quantity - 1) / 25) as integer) * 25 + 1 as qty_from
    from iceberg.silver.fact_orders
)
select
    qty_from,
    qty_from + 24                                                   as qty_to,
    format('%03d–%03d', qty_from, qty_from + 24)                    as qty_bucket,
    qty_from > 300                                                  as is_bulk,
    count(*)                                                        as orders,
    cast(100e0 * count(*) / sum(count(*)) over () as decimal(9, 4)) as share_pct
from bucketed
group by qty_from;
