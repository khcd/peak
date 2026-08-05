# Planar telemetry ingest

Stores authenticated, versioned telemetry events in ClickHouse.

## Contract

`POST /v2/events` accepts a JSON array and a bearer token:

```json
[
  {
    "event_id": "b9c176a4-badc-47a9-afbf-4a01cf1d65b9",
    "event_name": "session_start",
    "schema_version": 1,
    "occurred_at": "2026-08-05T12:34:56.789Z",
    "subject": { "kind": "install", "id": "1dd9d9c1-0bb4-4b21-b665-7f79d9f3e256" },
    "session_id": "679590a4-eea6-4e91-aeb0-23c8aa90ccaa",
    "resource": {
      "service_name": "planar",
      "service_version": "0.1.0",
      "platform": "macOS",
      "platform_version": "15.0"
    },
    "attributes": {}
  }
]
```

The producer is derived from `Authorization: Bearer <secret>`. The service accepts only reviewed `(producer, event_name, schema_version)` contracts and flat, scalar `attributes`. It returns `200` with `{ "accepted": N, "rejected": [...] }`; accepted events are durable.

## Run locally

Recreate the disposable local table:

```sh
docker exec -i clickhouse clickhouse-client --user planar --password planar123 --multiquery \
  < deploy/clickhouse/migrations/002_rebuild_for_v2.sql
```

Start the service with a token of at least 16 bytes:

```sh
export INGEST_KEYS='planar:replace-this-with-a-long-random-secret'
cargo run --release
```

See `.env.example` for configuration defaults.
