select
    cast(product_id as integer) as product_id,
    cast(sku as text) as sku,
    cast(product_name as text) as product_name,
    cast(category as text) as category,
    cast(unit_price as numeric(12, 2)) as unit_price,
    cast(created_at as timestamp with time zone) as created_at,
    cast(ingestion_id as text) as ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    cast(source_system as text) as source_system,
    cast(source_table as text) as source_table,
    cast(quality_run_id as text) as quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'products') }}
