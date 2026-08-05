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

Copy `.env.example` to `.env` and set `INGEST_KEYS` to a secret of at least 16 bytes. Load it into
the shell before every command below:

```sh
set -a && . ./.env && set +a
```

`CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` must be set explicitly. `CLICKHOUSE_USER` falls back to
`default`, but the ClickHouse image disables the `default` account as soon as `CLICKHOUSE_USER` is
set on the container, so that fallback can never authenticate — it fails with `Code: 194`.

Recreate the disposable local table:

```sh
docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery \
  < deploy/clickhouse/migrations/002_rebuild_for_v2.sql
```

Start the service:

```sh
cargo run --release
```

See `.env.example` for configuration defaults.

## Terminal dashboard

The same binary includes a read-only ClickHouse dashboard for a configured producer. It does not
need `INGEST_KEYS`, so it can run inside the production network without an ingest secret:

```sh
docker compose exec -it handler planar-telemetry-ingest dashboard planar
```

Or, when running locally, with `.env` loaded as above:

```sh
set -a && . ./.env && set +a
cargo run -- dashboard planar
```

The dashboard uses `received_at` for its live incoming-events chart and `occurred_at` for the
historical totals and breakdowns.

Every historical window is a whole number of calendar days ending with today — `7d` starts at
midnight six days ago, not 168 hours ago — so the totals and the per-window breakdowns always
cover exactly the same span. Those day boundaries resolve in `DASHBOARD_TIMEZONE` (default `UTC`),
which is a property of the project rather than of the viewer: the same window means the same span
whether the dashboard runs on a laptop in Sydney or via `docker compose exec` in the handler
container. The active timezone is shown in the header. The live chart is unaffected — it is a
rolling 60 seconds of wall time.

Press `1`, `2`, or `3` for today, 7 days, or 30 days; `p` pauses
polling, `r` refreshes, and `q` (or Escape) exits. If ClickHouse is temporarily unavailable, the
last successful snapshot stays on screen and polling resumes automatically when it recovers.

The `CONNECTED` panel counts distinct installs that emitted a `live_ping` in the last 11 minutes.
The planar client pings every 5 minutes, so the threshold deliberately clears two intervals — at
exactly 5 minutes a healthy client would sit on the boundary and flicker in and out of the count.
Liveness is measured on `received_at`, so an install whose wall clock is wrong is still counted
correctly. A sleeping or suspended device stops pinging and drops out of the count, as intended.

## Retention

`live_ping` rows expire after 2 days; every other event keeps the standard 180 days. A heartbeat
from every online install every five minutes outgrows all other event types combined, and is
worthless once the connected-clients window has passed. Existing deployments pick this up via
`deploy/clickhouse/migrations/004_live_ping_ttl.sql`; fresh ones get it from the init schema.

`deploy/clickhouse/migrations/003_received_at_skip_index.sql` is an optional performance migration
for the live chart; it is not applied by the normal setup.
