select
    cast(payment_id as integer) as payment_id,
    cast(order_id as integer) as order_id,
    cast(payment_method as text) as payment_method,
    cast(payment_status as text) as payment_status,
    cast(payment_amount as numeric(14, 2)) as payment_amount,
    cast(payment_date as timestamp with time zone) as payment_date,
    cast(ingestion_id as text) as ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    cast(source_system as text) as source_system,
    cast(source_table as text) as source_table,
    cast(quality_run_id as text) as quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'payments') }}
