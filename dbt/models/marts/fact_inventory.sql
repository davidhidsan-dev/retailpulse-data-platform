select
    inventory.product_id,
    products.sku,
    products.product_name,
    products.category,
    inventory.stock_quantity,
    inventory.reorder_level,
    inventory.updated_at,
    inventory.stock_quantity <= inventory.reorder_level as is_low_stock
from {{ ref('stg_inventory') }} as inventory
inner join {{ ref('stg_products') }} as products
    on inventory.product_id = products.product_id
