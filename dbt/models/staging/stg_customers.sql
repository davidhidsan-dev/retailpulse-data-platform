select
    cast(customer_id as integer) as customer_id,
    first_name,
    last_name,
    email,
    country,
    city,
    synthetic_behavior_segment,
    cast(created_at as timestamp with time zone) as created_at,
    ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    source_system,
    source_table,
    quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'customers') }}
