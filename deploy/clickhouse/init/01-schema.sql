CREATE DATABASE IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.events
(
    event_id         UUID,
    producer         LowCardinality(String),
    event_name       LowCardinality(String),
    schema_version   UInt16,
    occurred_at      DateTime64(3, 'UTC'),
    received_at      DateTime64(3, 'UTC') DEFAULT now64(3),
    subject_kind     LowCardinality(String),
    subject_id       String,
    session_id       String DEFAULT '',
    service_name     LowCardinality(String),
    service_version  LowCardinality(String),
    platform         LowCardinality(String) DEFAULT '',
    platform_version String DEFAULT '',
    attributes       String,
    country          LowCardinality(String) DEFAULT '',
    ingest_version   LowCardinality(String) DEFAULT ''
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (producer, event_name, occurred_at, event_id)
-- live_ping is a liveness heartbeat, not analytics: one row per online install every five
-- minutes, which makes it by far the highest-volume event in the table and worthless once the
-- dashboard's connected-clients window has passed. Expire it early; everything else keeps 180 days.
TTL toDateTime(occurred_at) + INTERVAL 2 DAY DELETE WHERE event_name = 'live_ping',
    toDateTime(occurred_at) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;
