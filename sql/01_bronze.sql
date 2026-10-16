-- ============================================================================
-- BRONZE. Сырые таблицы TPC-H один в один + две служебные колонки.
-- Источник: коннектор tpch, масштаб {{scale}}.
-- Формат Parquet, кодек ZSTD (по умолчанию в каталоге: iceberg.compression-codec=ZSTD).
--   _load_id   - идентификатор загрузки (какой запуск ELT принес строку)
--   _loaded_at - момент загрузки; по нему Silver оставляет самую свежую версию строки
-- ============================================================================

create schema if not exists iceberg.bronze;

drop table if exists iceberg.bronze.region;
create table iceberg.bronze.region with (format = 'PARQUET') as
select src.*, '{{load_id}}' as _load_id, cast(current_timestamp as timestamp(6) with time zone) as _loaded_at
from tpch.{{scale}}.region src;

drop table if exists iceberg.bronze.nation;
create table iceberg.bronze.nation with (format = 'PARQUET') as
select src.*, '{{load_id}}' as _load_id, cast(current_timestamp as timestamp(6) with time zone) as _loaded_at
from tpch.{{scale}}.nation src;

drop table if exists iceberg.bronze.customer;
create table iceberg.bronze.customer with (format = 'PARQUET') as
select src.*, '{{load_id}}' as _load_id, cast(current_timestamp as timestamp(6) with time zone) as _loaded_at
from tpch.{{scale}}.customer src;

drop table if exists iceberg.bronze.orders;
create table iceberg.bronze.orders with (format = 'PARQUET') as
select src.*, '{{load_id}}' as _load_id, cast(current_timestamp as timestamp(6) with time zone) as _loaded_at
from tpch.{{scale}}.orders src;

drop table if exists iceberg.bronze.lineitem;
create table iceberg.bronze.lineitem with (format = 'PARQUET') as
select src.*, '{{load_id}}' as _load_id, cast(current_timestamp as timestamp(6) with time zone) as _loaded_at
from tpch.{{scale}}.lineitem src;
