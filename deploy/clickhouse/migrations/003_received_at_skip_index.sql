-- Optional: apply only if the dashboard's received_at chart becomes expensive at higher volume.
ALTER TABLE telemetry.events ADD INDEX idx_received_at received_at TYPE minmax GRANULARITY 4;
ALTER TABLE telemetry.events MATERIALIZE INDEX idx_received_at;
