select
    items.order_item_id,
    items.order_id,
    orders.customer_id,
    items.product_id,
    orders.order_date,
    payments.payment_id,
    orders.order_status,
    payments.payment_status,
    payments.payment_method,
    items.quantity,
    items.unit_price,
    items.line_total
from {{ ref('stg_order_items') }} as items
inner join {{ ref('stg_orders') }} as orders
    on items.order_id = orders.order_id
left join {{ ref('stg_payments') }} as payments
    on items.order_id = payments.order_id
