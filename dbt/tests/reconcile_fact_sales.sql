-- Every validated order item must appear once in the sales fact with its amount intact.
with silver_items as (
    select order_item_id, line_total
    from {{ ref('stg_order_items') }}
),
fact_items as (
    select order_item_id, line_total
    from {{ ref('fact_sales') }}
),
totals as (
    select
        (select count(*) from silver_items) as silver_rows,
        (select count(*) from fact_items) as fact_rows,
        (select sum(line_total) from silver_items) as silver_amount,
        (select sum(line_total) from fact_items) as fact_amount
)

select
    coalesce(silver.order_item_id, fact.order_item_id) as order_item_id,
    case
        when silver.order_item_id is null then 'unexpected_in_fact'
        when fact.order_item_id is null then 'missing_from_fact'
        else 'line_total_mismatch'
    end as issue,
    silver.line_total as silver_line_total,
    fact.line_total as fact_line_total,
    totals.silver_rows,
    totals.fact_rows,
    totals.silver_amount,
    totals.fact_amount
from silver_items as silver
full outer join fact_items as fact
    on silver.order_item_id = fact.order_item_id
cross join totals
where silver.order_item_id is null
    or fact.order_item_id is null
    or silver.line_total is distinct from fact.line_total