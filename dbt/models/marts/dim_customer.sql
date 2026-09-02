select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    city,
    synthetic_behavior_segment,
    created_at
from {{ ref('stg_customers') }}
