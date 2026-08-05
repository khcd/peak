# Planar telemetry ingest

An authenticated, batched HTTP service that accepts Planar telemetry and stores it in ClickHouse. A `200 OK` is returned only after ClickHouse acknowledges an `async_insert` with `wait_for_async_insert=1`; clients can therefore safely remove events from their durable local buffer only after receiving 200.

## API contract

`POST /v1/events` accepts a JSON array of up to 200 events. Send `Authorization: Bearer <INGEST_KEY>`.

```json
[
  {
    "event_id": "b9c176a4-badc-47a9-afbf-4a01cf1d65b9",
    "event_name": "generation_completed",
    "event_time": "2026-07-24T12:34:56.789Z",
    "install_id": "1dd9d9c1-0bb4-4b21-b665-7f79d9f3e256",
    "session_id": "679590a4-eea6-4e91-aeb0-23c8aa90ccaa",
    "app_version": "0.1.0",
    "os": "macOS",
    "os_version": "15.0",
    "properties": { "duration_ms": 4210, "success": true, "backend": "sdcpp" }
  }
]
```

`received_at`, `country`, and `ingest_version` are set by the service/ClickHouse, never accepted from a client. `properties` is stored as JSON but validated against the approved v1 event contract: unknown keys, prompt text, paths, and arbitrary machine metadata are rejected. Adding a future event requires an explicit handler-contract update rather than silently broadening collection.

Responses are deliberately retry-safe:

- `200` — durably accepted; the client may remove exactly this batch from its pending snapshot.
- `400`, `413`, or `422` — invalid/non-retryable payload; drop or quarantine the batch.
- `401` — missing or invalid ingest key.
- `503` — ClickHouse unavailable; retain and retry with backoff.

The default allowlist contains the six events in the telemetry plan. New event names require a handler-contract update, which intentionally makes changes to collected data reviewable.

## Privacy and proxy trust

`install_id` is a random, per-install pseudonymous identifier. Do not send prompts, file paths, account data, IPs, or hardware identifiers. The table deliberately has no IP address or IP hash column.

Set `TRUST_CLOUDFLARE_HEADERS=true` only when the handler's public origin is locked to Cloudflare using Authenticated Origin Pulls or a Cloudflare-only firewall rule. Then the service accepts Cloudflare's `CF-IPCountry` header and stores only the two-character country code. With the default `false`, `country` is blank, preventing direct requests from spoofing geography.

Client event times may be up to 190 days old for offline delivery and up to five minutes in the future; configure `MAX_EVENT_AGE_DAYS` and `MAX_FUTURE_SKEW_SECONDS` if needed.

## Run against your existing local ClickHouse

Your ClickHouse container needs to be reachable at `127.0.0.1:8123`. First create the database and table once (use the username/password configured for your container):

```sh
docker exec -i clickhouse clickhouse-client \
  --user "${CLICKHOUSE_USER:-default}" \
  --password "${CLICKHOUSE_PASSWORD:-}" \
  --multiquery < deploy/clickhouse/init/01-schema.sql
```

If you already created the original table, apply the non-destructive schema migration instead:

```sh
docker exec -i clickhouse clickhouse-client \
  --user "${CLICKHOUSE_USER:-default}" \
  --password "${CLICKHOUSE_PASSWORD:-}" \
  --multiquery < deploy/clickhouse/migrations/001_add_country_and_ingest_version.sql
```

Then start the service (choose a secret of at least 16 bytes):

```sh
export INGEST_KEY='replace-this-with-a-long-random-secret'
cargo run --release
```

The service writes structured JSON logs to standard output. While diagnosing local ingestion, run `RUST_LOG=debug cargo run --release`. Logs include batch counts, event names, country (when trusted), rejection codes, and ClickHouse write latency—but intentionally never payloads, IPs, install IDs, or bearer tokens.

In a second terminal, verify the complete write path:

```sh
EVENT_TIME="$(date -u '+%Y-%m-%dT%H:%M:%S.000Z')"
curl -i http://127.0.0.1:8081/healthz
curl -i http://127.0.0.1:8081/v1/events \
  -H "Authorization: Bearer $INGEST_KEY" \
  -H 'Content-Type: application/json' \
  --data "[{\"event_id\":\"b9c176a4-badc-47a9-afbf-4a01cf1d65b9\",\"event_name\":\"session_start\",\"event_time\":\"$EVENT_TIME\",\"install_id\":\"1dd9d9c1-0bb4-4b21-b665-7f79d9f3e256\",\"session_id\":\"679590a4-eea6-4e91-aeb0-23c8aa90ccaa\",\"app_version\":\"0.1.0\",\"os\":\"macOS\",\"os_version\":\"15.0\",\"properties\":{}}]"
curl 'http://127.0.0.1:8123/?query=SELECT%20event_name%2C%20count%28%29%20FROM%20telemetry.events%20GROUP%20BY%20event_name'
```

If your local ClickHouse has credentials, additionally export `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`. Copy `.env.example` for a complete configuration reference; the binary deliberately does not load `.env` itself so secrets stay under your process manager's control.

## Production deployment

`docker-compose.yml` deploys ClickHouse on an internal-only network, the handler, and Caddy as the only public surface. Create a `.env` with `INGEST_KEY`, `CLICKHOUSE_PASSWORD`, and a DNS-backed `TELEMETRY_DOMAIN`, then run:

```sh
docker compose up -d --build
```

Caddy obtains HTTPS certificates automatically. The compose deployment is separate from your currently-running local ClickHouse container; do not start it on this machine unless you intend to replace that local setup.

## Operations

Keep ClickHouse private, back it up from day one, monitor disk free space and MergeTree part counts, and test a restore. `ReplacingMergeTree` removes retry duplicates eventually by the `(event_name, event_time, event_id)` key; use `FINAL` or group by `event_id` for a query that needs immediate de-duplication. Configure Cloudflare WAF/rate limiting and short retention for any proxy logs; a desktop client cannot hold a secret that proves it is genuine Planar.
