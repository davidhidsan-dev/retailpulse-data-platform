select
    event_id,
    event_type,
    occurred_at,
    session_id,
    customer_id,
    product_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    source_file,
    quality_run_id
from {{ ref('stg_web_events') }}
