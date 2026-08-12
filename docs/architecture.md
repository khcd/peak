# peak

Authenticated, versioned multi-tenant telemetry ingestion. The single Rust binary has three modes:
an axum HTTP ingest server (`serve`), a read-only ratatui TUI dashboard (`dashboard <tenant>`), and
a dependency-free ingest-key generator (`keygen <tenant>`), dispatched in `src/main.rs`.

- Rust edition 2024, axum 0.8, tokio, ratatui 0.30, ClickHouse 26.6.
- ~2,900 lines of source across `src/`. No ORM: raw SQL strings plus `#[derive(clickhouse::Row)]`.
- Deployed via `docker-compose.yml` as clickhouse + handler + caddy.
- The binary is intentionally not publishable as a crates.io library (`publish = false`); `getrandom`
  is the only dependency used by `keygen`.
- Releases follow SemVer 2.0.0. The project is currently on the `0.1.x` development line, with the
  package and default ingest version at `0.1.0`.

## Layout

| Path | Role |
|---|---|
| `src/main.rs` | axum router, `/healthz`, `/v2/events`, the single write path `insert_rows` |
| `src/cli.rs` | dependency-free parser for `serve`, `dashboard`, and `keygen` |
| `src/event.rs` | `IncomingEvent` (wire) → `EventRow` (storage row) plus envelope validation |
| `src/contract.rs` | Validates `attributes` against the tenant manifest |
| `src/manifest.rs` | Tenant TOML loader; `FleetDimension`, local `common_fields` structs, cycle checking |
| `src/auth.rs` | `INGEST_KEYS` → producer resolution from the bearer token; `generate_secret()` mints parser-safe keys |
| `src/config.rs` | Env + `config.json` config; `ClickhouseConfig::client()` is the only client constructor |
| `src/dashboard/query.rs` | **All read SQL.** Seven builders and two entry points, `fast` / `slow` |
| `src/dashboard/{mod,app,ui}.rs` | TUI event loop, state, rendering |
| `deploy/clickhouse/init/01-schema.sql` | Authoritative schema, auto-applied on first container init only |
| `deploy/clickhouse/migrations/` | `002`–`004`, applied by hand. No migration tooling, no version table |
| `tenants/*.toml` | Reviewed per-tenant manifests; `demo.toml` is the checked-in fixture and `_example.toml` is skipped |

## Conventions

- Tests run with the repo root as CWD — several load `Registry::load(Path::new("tenants"))` and
  `Box::leak`; `tenants/` is the fixture.
- **No test touches a database.** The SQL tests are pure string assertions on generated SQL. That
  is the only regression net for query generation.
- CI runs `cargo fmt --check`, `cargo test --locked`, plus a dedicated manifest-compatibility job.
- Config structs use `#[serde(deny_unknown_fields)]` throughout — unknown keys are errors by design.
- Comments explain *why*, not *what*. Match the existing density; several load-bearing invariants
  are documented only in comments.

## The read path

There is no read API. `router()` exposes only `GET /healthz` and `POST /v2/events`; every analytics
query is reachable only by running the TUI. Reads have no HTTP auth story — the dashboard is a local
operator tool, `t`/`T` switches tenants freely, and ingest keys are write-scoped by construction
(secret → `&'static Tenant`).

The seven query builders are:

| builder | shape |
|---|---|
| `live_chart_sql` | `uniqExact(event_id)` by `toStartOfSecond(received_at)`, last 60s |
| `connected_sql` | `uniqExact(subject_id)` where liveness is within `offline_after_minutes()` |
| `tail_initial_sql` / `tail_since_sql` | raw row select, no aggregation, `LIMIT 50` |
| `totals_sql` | one row, six `uniqExactIf` expressions over three day windows |
| `breakdown_sql` | top 12 event names |
| `fleet_sql` | `UNION ALL` per fleet dimension, then group |

Every count is `uniqExact`, never `count()`: `ReplacingMergeTree(received_at)` means duplicate
`event_id` rows exist until a background merge. Windows are day-aligned and resolve in the
ClickHouse session timezone set from `DASHBOARD_TIMEZONE` (default `UTC`). Liveness is measured on
`received_at`, not `occurred_at`, so a device with a wrong wall clock still counts.

## Manifest and contract internals

- `Tenant::contracts` is private; the accessor is `contract(event_name, schema_version)`.
- `Registry::check_compatibility` is the load-time hook for cross-section references. It checks that
  `dashboard.liveness.event_name` has an event contract; CI runs the compatibility test separately.
- `FieldType` is `Bool | U64 | I64 | F64 | Str{max_bytes} | Enum(Vec<String>) | Struct{fields}`.
  `Struct` comes from `[events.<name>.common_fields]` and is resolved with cycle detection.
- Manifest inheritance (`extends`) was deliberately removed and is explicitly rejected. Do not
  reintroduce it.
- `valid_producer_name` restricts tenant names to `[a-z0-9_]{1,32}`, which makes `tenant.name` safe
  wherever it appears.

## Startup and CLI

`main` loads the registry, then dispatches `cli::Mode`. Argument errors exit 2. `src/cli.rs` is a
hand-rolled parser over an exhaustive slice match. Adding a subcommand with a variable tail needs a
specific arm; there is no clap and the dependency posture argues against adding it.

`Config::from_env` requires `INGEST_KEYS` via `required_env` and is called only by `serve`.
`dashboard` builds a `ClickhouseConfig` directly, and `keygen` needs neither configuration nor
ClickHouse: it only resolves the requested tenant from the registry, generates 32 random bytes as
64 lowercase hexadecimal characters, and prints `<tenant>:<secret>` to stdout. Its human-facing
restart hint goes to stderr so stdout can be redirected into a key list.

The secret format is deliberately aligned with `ProducerRegistry::from_pairs`: it is at least 16
characters and contains neither `,` nor `:`. Secrets are hashed in memory with SHA-256 and are never
persisted by the service. Key rotation therefore remains an environment update followed by restart;
there is no admin API, key store, or revocation without restart.

## Driver facts — `clickhouse` 0.13.3

- `.bind()` is client-side substitution, not a prepared statement; over- and under-binding are
  detected by the driver.
- `Bind` is blanket-implemented for `Serialize`. Binding a Rust enum directly serializes it as a
  tagged enum, not its inner primitive; match and bind the primitive instead.
- `Query::sql_display()` renders post-bind SQL and is useful as a free dry run.
- `fetch_bytes(format)` is the way to handle a dynamic column set; `fetch_all::<T>()` needs a
  compile-time row struct.
- POST queries already get `readonly=1` appended.
- Writes never build SQL: `insert_rows` uses `client.insert("events")` with
  `async_insert=1` + `wait_for_async_insert=1`, so a 200 means ClickHouse acknowledged. There are
  no transactions; a batch is not atomic.
