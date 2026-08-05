ALTER TABLE telemetry.events
    ADD COLUMN IF NOT EXISTS country LowCardinality(String) DEFAULT '';

ALTER TABLE telemetry.events
    ADD COLUMN IF NOT EXISTS ingest_version LowCardinality(String) DEFAULT '';
