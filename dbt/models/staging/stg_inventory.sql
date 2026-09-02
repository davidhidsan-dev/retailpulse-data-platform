select
    cast(product_id as integer) as product_id,
    cast(stock_quantity as integer) as stock_quantity,
    cast(reorder_level as integer) as reorder_level,
    cast(updated_at as timestamp with time zone) as updated_at,
    cast(ingestion_id as text) as ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    cast(source_system as text) as source_system,
    cast(source_table as text) as source_table,
    cast(quality_run_id as text) as quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'inventory') }}
