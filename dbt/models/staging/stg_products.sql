select
    cast(product_id as integer) as product_id,
    sku,
    product_name,
    category,
    cast(unit_price as numeric(12, 2)) as unit_price,
    cast(created_at as timestamp with time zone) as created_at,
    ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    source_system,
    source_table,
    quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'products') }}
