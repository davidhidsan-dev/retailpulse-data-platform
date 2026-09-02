select
    cast(payment_id as integer) as payment_id,
    cast(order_id as integer) as order_id,
    payment_method,
    payment_status,
    cast(payment_amount as numeric(14, 2)) as payment_amount,
    cast(payment_date as timestamp with time zone) as payment_date,
    ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    source_system,
    source_table,
    quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'payments') }}
