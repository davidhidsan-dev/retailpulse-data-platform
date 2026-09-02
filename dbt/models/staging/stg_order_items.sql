select
    cast(order_item_id as integer) as order_item_id,
    cast(order_id as integer) as order_id,
    cast(product_id as integer) as product_id,
    cast(quantity as integer) as quantity,
    cast(unit_price as numeric(12, 2)) as unit_price,
    cast(line_total as numeric(14, 2)) as line_total,
    cast(ingestion_id as text) as ingestion_id,
    cast(ingested_at as timestamp with time zone) as ingested_at,
    cast(source_system as text) as source_system,
    cast(source_table as text) as source_table,
    cast(quality_run_id as text) as quality_run_id,
    cast(quality_checked_at as timestamp with time zone) as quality_checked_at
from {{ source('warehouse_source', 'order_items') }}
