CREATE SCHEMA IF NOT EXISTS warehouse_source;

CREATE TABLE IF NOT EXISTS warehouse_source.web_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    session_id TEXT NOT NULL,
    customer_id INTEGER,
    product_id INTEGER NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    kafka_key TEXT NOT NULL,
    ingestion_id TEXT NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    source_system TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_file TEXT NOT NULL,
    quality_run_id TEXT NOT NULL,
    quality_checked_at TIMESTAMP WITH TIME ZONE NOT NULL
);
