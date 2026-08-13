# peak

Authenticated, versioned multi-tenant telemetry ingestion backed by ClickHouse.

## Versioning

`peak` follows [Semantic Versioning 2.0.0](https://semver.org/). The initial release is `0.1.0`
(the `0.1` development line); while the major version is zero, minor releases may still include
breaking changes and patch releases are reserved for compatible fixes. `INGEST_VERSION` defaults
to the Cargo package version and can be overridden for a deployment.

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
      "service_name": "example-client",
      "service_version": "0.1.0",
      "platform": "macOS",
      "platform_version": "15.0"
    },
    "attributes": {}
  }
]
```

The producer is derived from `Authorization: Bearer <secret>`. The service accepts only reviewed `(producer, event_name, schema_version)` contracts and declared `attributes`. It returns `200` with `{ "accepted": N, "rejected": [...] }`; accepted events are fsynced to the server WAL before the response. ClickHouse insertion happens asynchronously, so an accepted event may take a few seconds to appear in the dashboard.

`/v2/events` accepts uncompressed JSON plus `Content-Encoding: gzip` or `Content-Encoding: zstd`.
Clients should compress batches before sending them; the service bounds both the encoded and
decoded request body sizes.

## Tenants

Tenant definitions are self-contained, reviewed TOML manifests in [`tenants`](tenants). The
[`_example.toml`](tenants/_example.toml) file documents the available manifest shapes and is
ignored by the loader. Add a tenant by copying it to `tenants/<name>.toml`, adding a
`<name>:<secret>` entry to `INGEST_KEYS` (see [Generate an ingest key](#generate-an-ingest-key)),
and restarting the service. `MANIFEST_DIR` defaults to `tenants`.

A manifest can define local reusable nested structs under `common_fields`. The type name is scoped to
that manifest and can be used from an event field with `type = "<name>"`:

```toml
[events.model.common_fields]
model = { type = "str", max_bytes = 256, required = true }
load_ms = { type = "u64", required = true }
size_mb = { type = "u64", required = true }

[events.model_loaded.fields]
model = { type = "model", required = true }
```

Nested struct definitions may refer to other local structs, but cyclic nesting is rejected when the
manifest is loaded.

Optionally set `services = ["your-service"]` in a tenant manifest to restrict
`resource.service_name`. The wire format and ClickHouse schema remain unchanged.

## Generate an ingest key

`keygen` prints a ready-to-paste `INGEST_KEYS` entry — the tenant name, a colon, and a 64-character
hex secret. It needs neither ClickHouse nor an existing ingest key, so it is the first thing you run
on a fresh checkout:

```sh
cargo run -- keygen demo
```

The `demo:<secret>` pair goes to stdout and the restart hint to stderr, so the pair can be
redirected straight into a file. Restart the service after updating `INGEST_KEYS`.

Without a Rust toolchain, any 64 hex characters work — the format is chosen so that it clears the
16-byte minimum and contains neither of the `,` and `:` characters `INGEST_KEYS` is split on:

```sh
printf 'demo:%s\n' "$(openssl rand -hex 32)"
```

Once the stack is already running, a key for an *additional* tenant can be generated in the
container. This cannot bootstrap the first key — Compose will not start any service, `exec`
included, until `INGEST_KEYS` already has a value:

```sh
docker compose exec -it handler peak keygen another-tenant
```

## Run locally

Copy `.env.example` to `.env` and set `INGEST_KEYS` to a generated entry. Load it into the shell
before every command below:

```sh
set -a && . ./.env && set +a
```

`CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` must be set explicitly. `CLICKHOUSE_USER` falls back to
`default`, but the ClickHouse image disables the `default` account as soon as `CLICKHOUSE_USER` is
set on the container, so that fallback can never authenticate — it fails with `Code: 194`.

## Run with Docker

Docker Compose starts ClickHouse and the telemetry handler together. Copy the example environment
file, generate a real ingest secret, set a ClickHouse password, and provide a domain for the
optional Caddy reverse proxy:

```sh
cp .env.example .env
cargo run -- keygen demo
```

Then edit `.env`. Compose interpolates the whole file before it starts anything, so all three of
these must have real values or every `docker compose` command fails with
`required variable ... is missing a value`:

- `INGEST_KEYS` — the generated `demo:<secret>` pair, replacing `CHANGE_ME`.
- `CLICKHOUSE_PASSWORD` — replacing `CHANGE_ME`.
- `TELEMETRY_DOMAIN` — `localhost` for a local-only run, or the DNS name pointing at this host for
  a public deployment.

Start the complete stack, including Caddy on ports 80 and 443:

```sh
docker compose up -d --build
```

For local development, the handler is also exposed directly at
`http://127.0.0.1:8081`; Caddy is not required for local clients. The `handler` service still
needs `TELEMETRY_DOMAIN` in `.env` because Compose validates the full file, even when only these
services are selected:

```sh
docker compose up -d --build clickhouse handler
curl http://127.0.0.1:8081/healthz
```

Follow service logs or stop the stack with:

```sh
docker compose logs -f handler
docker compose down
```

ClickHouse data is kept in the `clickhouse-data` Docker volume, and pending accepted telemetry is
kept in the `handler-data` volume until ClickHouse acknowledges it. The initial schema is applied
only when the ClickHouse volume is first created.

## Service settings

Non-secret, versioned settings live in [`config.json`](config.json). This file is tracked by design:
it contains service settings only and must never hold credentials. `CONFIG_PATH` can point to a
different file; it defaults to `config.json`. The supplied Docker image includes this file at
`/app/config.json`.

`clickhouse.transport_compression` controls the ClickHouse client connection: `lz4` uses native LZ4
transport blocks (the default), while `none` disables them. This affects only traffic between the
service and ClickHouse; MergeTree storage compression is configured independently by ClickHouse.

The ingest path appends accepted rows to a JSON-lines WAL before returning `200`, then combines
requests into ClickHouse inserts. `MAX_INSERT_BATCH_EVENTS` controls the size of those inserts;
`BATCH_WAIT_MS` defaults to 5000, so low-volume producers are flushed after five seconds even when
they never fill a batch. Smaller values reduce latency but increase ClickHouse insert frequency.
`WAL_PATH` defaults to `data/events.wal`; the Compose deployment uses the durable `handler-data`
volume. On startup, pending WAL rows are checked against ClickHouse by full row content. Matching
rows are acknowledged without another insert, missing rows are replayed, and event-ID content
mismatches are logged and replayed from the WAL. A crash between ClickHouse acknowledgement and
WAL compaction can therefore produce a duplicate physical row, but the table's
`ReplacingMergeTree`/`uniqExact(event_id)` design makes that replay safe.

Recreate the disposable local table:

```sh
docker compose exec -T clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery \
  < deploy/clickhouse/migrations/002_rebuild_for_v2.sql
```

Start the service:

```sh
cargo run --release
```

See `.env.example` for configuration defaults.

## Terminal dashboard

The same binary includes a read-only ClickHouse dashboard for a configured tenant. It does not
need `INGEST_KEYS`, so it can run inside the production network without an ingest secret:

```sh
docker compose exec -it handler peak dashboard demo
```

The Docker dashboard command uses the handler container's network and configuration, so it connects
to ClickHouse at the internal Compose address. Start the stack first, then replace `demo` with any
tenant name declared in `tenants/<name>.toml`:

```sh
docker compose up -d --build clickhouse handler
docker compose exec -it handler peak dashboard demo
```

Press `q` or Escape to leave the dashboard. The dashboard process exits without stopping the
background handler or ClickHouse containers.

Or, when running locally, with `.env` loaded as above:

```sh
set -a && . ./.env && set +a
cargo run -- dashboard demo
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

Press `1`, `2`, or `3` for the manifest's three configured windows; `t` switches to the next
tenant and `Shift+T` to the previous one. `p` pauses polling, `r` refreshes, and `q` (or Escape)
exits. If ClickHouse is temporarily unavailable, the
last successful snapshot stays on screen and polling resumes automatically when it recovers.

The `CONNECTED` panel follows each manifest's optional liveness event. Its offline threshold is
twice the declared ping interval plus one minute; tenants without liveness show `--`.
Liveness is measured on `received_at`, so an install whose wall clock is wrong is still counted
correctly. A sleeping or suspended device stops pinging and drops out of the count, as intended.

## Retention

`live_ping` rows expire after 2 days; every other event keeps the standard 180 days. A heartbeat
from every online install every five minutes outgrows all other event types combined, and is
worthless once the connected-clients window has passed. Existing deployments pick this up via
`deploy/clickhouse/migrations/004_live_ping_ttl.sql`; fresh ones get it from the init schema.

`deploy/clickhouse/migrations/003_received_at_skip_index.sql` is an optional performance migration
for the live chart; it is not applied by the normal setup.

## Security

Please do not report vulnerabilities in a public issue — see [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
