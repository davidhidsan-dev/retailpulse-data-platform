select
    session_id,
    min(occurred_at) as session_started_at,
    max(occurred_at) as session_ended_at,
    min(customer_id) as customer_id,
    count(*) as event_count,
    count(*) filter (where event_type = 'product_view') as product_view_count,
    count(*) filter (where event_type = 'add_to_cart') as add_to_cart_count,
    count(distinct product_id) as distinct_product_count,
    bool_or(event_type = 'add_to_cart') as added_to_cart
from {{ ref('stg_web_events') }}
group by session_id
