select
    product_id,
    sku,
    product_name,
    category,
    unit_price,
    created_at
from {{ ref('stg_products') }}
