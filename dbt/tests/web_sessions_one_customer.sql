select
    session_id
from {{ ref('stg_web_events') }}
where customer_id is not null
group by session_id
having count(distinct customer_id) > 1
