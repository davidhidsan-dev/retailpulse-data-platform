select
    cast(order_id as integer) as order_id,
    cast(customer_id as integer) as customer_id,
    cast(order_date as timestamp with time zone) as order_date,
    cast(order_status as text) as order_status,
    cast(country as text) as country,
    cast(ingestion_id as text) as ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    cast(source_system as text) as source_system,
    cast(source_table as text) as source_table,
    cast(quality_run_id as text) as quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'orders') }}
