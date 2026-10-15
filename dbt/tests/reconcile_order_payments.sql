-- Every published order must have lines and exactly one coherent payment.
with order_totals as (
    select
        order_id,
        round(sum(line_total), 2) as order_total
    from {{ ref('stg_order_items') }}
    group by order_id
),
payment_summary as (
    select
        order_id,
        count(*) as payment_count,
        min(payment_status) as payment_status,
        min(payment_amount) as payment_amount
    from {{ ref('stg_payments') }}
    group by order_id
),
order_contract as (
    select
        coalesce(orders.order_id, totals.order_id, payments.order_id) as order_id,
        orders.order_status,
        totals.order_total,
        payments.payment_count,
        payments.payment_status,
        payments.payment_amount
    from {{ ref('stg_orders') }} as orders
    full outer join order_totals as totals
        on orders.order_id = totals.order_id
    full outer join payment_summary as payments
        on coalesce(orders.order_id, totals.order_id) = payments.order_id
)

select
    order_id,
    order_status,
    order_total,
    payment_count,
    payment_status,
    payment_amount
from order_contract
where order_status is null
    or order_total is null
    or payment_count is distinct from 1
    or payment_status is null
    or payment_amount is distinct from order_total
    or payment_status is distinct from case order_status
        when 'completed' then 'paid'
        when 'cancelled' then 'failed'
        when 'refunded' then 'refunded'
        when 'pending' then 'pending'
    end
