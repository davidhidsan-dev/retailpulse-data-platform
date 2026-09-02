with date_bounds as (
    select
        cast(min(order_date) as date) as min_date,
        cast(max(order_date) as date) as max_date
    from {{ ref('stg_orders') }}
),

date_spine as (
    select
        cast(generate_series(min_date, max_date, interval '1 day') as date) as date_day
    from date_bounds
    where min_date is not null
)

select
    date_day,
    cast(extract(year from date_day) as integer) as year,
    cast(extract(month from date_day) as integer) as month,
    cast(extract(day from date_day) as integer) as day,
    trim(to_char(date_day, 'Month')) as month_name,
    cast(extract(quarter from date_day) as integer) as quarter
from date_spine
