CREATE DATABASE IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.events
(
    event_id     UUID,
    event_name   LowCardinality(String),
    event_time   DateTime64(3, 'UTC'),
    received_at  DateTime64(3, 'UTC') DEFAULT now64(3),
    install_id   UUID,
    session_id   UUID,
    app_version  LowCardinality(String),
    os           LowCardinality(String),
    os_version   String,
    properties   String,
    country      LowCardinality(String) DEFAULT '',
    ingest_version LowCardinality(String) DEFAULT ''
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_name, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;
