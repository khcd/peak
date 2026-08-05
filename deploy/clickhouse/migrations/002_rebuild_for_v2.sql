-- Destructive by design: telemetry data is local and disposable. The v2 ORDER BY
-- begins with producer, which cannot be changed in place for ReplacingMergeTree.
DROP TABLE IF EXISTS telemetry.events;

CREATE TABLE telemetry.events
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
TTL toDateTime(occurred_at) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;
