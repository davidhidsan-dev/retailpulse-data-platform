select
    cast(customer_id as integer) as customer_id,
    cast(first_name as text) as first_name,
    cast(last_name as text) as last_name,
    cast(email as text) as email,
    cast(country as text) as country,
    cast(city as text) as city,
    cast(synthetic_behavior_segment as text) as synthetic_behavior_segment,
    cast(created_at as timestamp with time zone) as created_at,
    cast(ingestion_id as text) as ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    cast(source_system as text) as source_system,
    cast(source_table as text) as source_table,
    cast(quality_run_id as text) as quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'customers') }}
