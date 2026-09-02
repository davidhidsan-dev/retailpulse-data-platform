select
    cast(order_id as integer) as order_id,
    cast(customer_id as integer) as customer_id,
    cast(order_date as timestamp with time zone) as order_date,
    order_status,
    country,
    ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    source_system,
    source_table,
    quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'orders') }}
