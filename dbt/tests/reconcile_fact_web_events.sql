with staging as (
    select
        event_id,
        event_type,
        occurred_at,
        session_id,
        customer_id,
        product_id
    from {{ ref('stg_web_events') }}
),

fact as (
    select
        event_id,
        event_type,
        occurred_at,
        session_id,
        customer_id,
        product_id
    from {{ ref('fact_web_events') }}
)

select
    coalesce(staging.event_id, fact.event_id) as event_id,
    staging.event_id as staging_event_id,
    fact.event_id as fact_event_id
from staging
full outer join fact
    on staging.event_id = fact.event_id
where staging.event_id is null
   or fact.event_id is null
   or staging.event_type is distinct from fact.event_type
   or staging.occurred_at is distinct from fact.occurred_at
   or staging.session_id is distinct from fact.session_id
   or staging.customer_id is distinct from fact.customer_id
   or staging.product_id is distinct from fact.product_id
