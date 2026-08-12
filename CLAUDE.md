# telemetry-ingest

Authenticated, versioned multi-tenant telemetry ingest. Single Rust binary with two modes:
an axum HTTP ingest server (`serve`) and a read-only ratatui TUI dashboard (`dashboard <tenant>`),
dispatched in `src/main.rs:72`.

- Rust edition 2024, axum 0.8, tokio, ratatui 0.30, ClickHouse 26.6.
- ~2,900 lines of source across `src/`. No ORM: raw SQL strings plus `#[derive(clickhouse::Row)]`.
- Deployed via `docker-compose.yml` as clickhouse + handler + caddy.

## Layout

| Path | Role |
|---|---|
| `src/main.rs` | axum router, `/healthz`, `/v2/events`, the single write path `insert_rows` (:203) |
| `src/event.rs` | `IncomingEvent` (wire) → `EventRow` (:43, storage row) plus envelope validation |
| `src/contract.rs` | Validates `attributes` against the tenant manifest |
| `src/manifest.rs` | Tenant TOML loader; `FleetDimension` (:130), local `common_fields` structs, cycle checking |
| `src/auth.rs` | `INGEST_KEYS` → producer resolution from the bearer token |
| `src/config.rs` | Env + `config.json` config; `ClickhouseConfig::client()` (:45) is the only client constructor |
| `src/dashboard/query.rs` | **All read SQL.** Six builders (:161–233) and two entry points, `fast` (:92) / `slow` (:138) |
| `src/dashboard/{mod,app,ui}.rs` | TUI event loop, state, rendering |
| `deploy/clickhouse/init/01-schema.sql` | Authoritative schema, auto-applied on first container init only |
| `deploy/clickhouse/migrations/` | `002`–`004`, applied by hand. No migration tooling, no version table |
| `tenants/*.toml` | Reviewed per-tenant manifests. `_example.toml` is skipped by the loader |

## Conventions

- Tests run with the **repo root as CWD** — several do `Registry::load(Path::new("tenants"))` and
  `Box::leak` (`src/main.rs:324`, `src/cli.rs:54`, `src/dashboard/query.rs:240`). `tenants/` is the
  fixture.
- **No test touches a database.** The SQL tests (`src/dashboard/query.rs:235-307`) are pure string
  assertions on generated SQL. That is the only regression net for query generation.
- CI (`.github/workflows/ci.yml`): `cargo fmt --check`, `cargo test --locked`, plus a dedicated
  manifest-compatibility job. No DB service container.
- Config structs use `#[serde(deny_unknown_fields)]` throughout — unknown keys are errors by design.
- Comments explain *why*, not *what*. Match the existing density; several load-bearing invariants
  are documented only in comments (e.g. `src/dashboard/query.rs:169-174`, `src/main.rs:204`).

## The read path

**There is no read API.** `router()` (`src/main.rs:114`) exposes only `GET /healthz` and
`POST /v2/events`. Every analytics query in the project lives in `src/dashboard/query.rs` and is
reachable only by running the TUI. Reads have no auth story at all — the dashboard is a local
operator tool, `t`/`T` switches tenants freely (`src/dashboard/mod.rs:162`), and ingest keys are
write-scoped by construction (secret → `&'static Tenant`, `src/auth.rs`).

Two async entry points, both returning plain data structs, both called from `event_loop`
(`src/dashboard/mod.rs:69`) on tokio intervals with one in-flight request per tier:

| entry | tier | issues |
|---|---|---|
| `fast` (:92) | 1s poll, 2s timeout | `live_chart_sql`, `connected_sql`, one of `tail_initial_sql`/`tail_since_sql` |
| `slow` (:138) | 10s poll, 10s timeout | `totals_sql`, `breakdown_sql`, `fleet_sql` |

Results are discarded if the tenant or window changed while in flight; the last good snapshot stays
on screen on error.

The seven builders and their shapes — worth knowing before touching any of them:

| builder | line | shape | binds |
|---|---|---|---|
| `live_chart_sql` | :161 | `uniqExact(event_id)` by `toStartOfSecond(received_at)`, last 60s | 1 |
| `connected_sql` | :175 | `uniqExact(subject_id)` where liveness event within `offline_after_minutes()` | 2 |
| `tail_initial_sql` / `tail_since_sql` | :185 / :191 | **raw row select**, no aggregation, `LIMIT 50` | 1 / 2 |
| `totals_sql` | :197 | one row, six `uniqExactIf` over three day windows | 1 |
| `breakdown_sql` | :213 | top 12 event names | 1 |
| `fleet_sql` | :222 | `UNION ALL` per fleet dimension, then group | **N** (one per dimension) |

Notes that constrain any refactor:

- **Every count is `uniqExact`, never `count()`.** `ReplacingMergeTree(received_at)` means duplicate
  `event_id` rows exist until a background merge, so `count()` is simply wrong here.
- **Windows are day-aligned, not rolling.** `Window::since` (:27) emits
  `toStartOfDay(now()) - INTERVAL n-1 DAY` so an N-day window covers N whole days including today.
  Two tests exist purely to keep it that way (:247, :262).
- Day boundaries resolve in the ClickHouse **session** timezone, set once at
  `src/dashboard/mod.rs:34` from `config::dashboard_timezone()` (`DASHBOARD_TIMEZONE`, default UTC).
  An unknown name makes ClickHouse reject the query, so the health check doubles as its validation.
- Liveness is measured on `received_at`, not `occurred_at`, deliberately — a device with a wrong
  wall clock still counts (comment at :169-174).
- `fleet_sql`'s bind count is kept in sync by a loop in the *caller* (:153-156). That coupling is
  invisible and is the one genuinely fragile thing in this file. Under-binding fails at
  `finish()`, so it is a runtime error rather than an injection — but it is still a footgun.
- **`attributes` is never read.** It is a JSON document stored as `String`; nothing in
  `src/dashboard/` mentions it. Any attribute-level analytics needs `JSONExtract*` and would be the
  first full-scan query in the project.
- `FleetDimension` (`src/manifest.rs:130`) is a closed enum whose `name()`/`column()` return
  `&'static str`. That closedness — not escaping — is what makes the interpolation at :224 safe.

## Manifest and contract internals

- `Tenant::contracts` is **private**; the only accessor is `contract(event_name, schema_version)`
  (`src/manifest.rs:24`). Anything needing to walk all of a tenant's contracts (e.g. resolving an
  attribute path) has to add an accessor.
- `Registry::check_compatibility` (:92) is the load-time hook for cross-section references. Today it
  checks exactly one thing: a `dashboard.liveness.event_name` must have an event contract. CI runs
  `manifest::tests::checked_in_manifests_pass_compatibility --exact` as its own job.
- `FieldType` (`src/contract.rs:6`) is `Bool | U64 | I64 | F64 | Str{max_bytes} | Enum(Vec<String>) |
  Struct{fields}`. `Struct` comes from `[events.<name>.common_fields]`, resolved with cycle
  detection by `resolve_common_fields` (`src/manifest.rs:222`). This is the only typed knowledge of
  what lives inside `attributes`.
- Manifest inheritance (`extends`) was deliberately removed in `3884691` and is explicitly rejected
  (`src/manifest.rs:622`). Do not reintroduce it.
- `valid_producer_name` (`src/auth.rs:68`) restricts tenant names to `[a-z0-9_]{1,32}`, which is
  what makes `tenant.name` safe wherever it appears.

## Startup and CLI

`main` (`src/main.rs:63`) loads the registry, then dispatches `cli::Mode`. Argument errors exit 2.

- `src/cli.rs` is a hand-rolled, dependency-free parser over an **exhaustive slice match**. Adding a
  subcommand with a variable tail needs a `[cmd, tenant, rest @ ..]` arm; there is no clap and the
  dependency posture argues against adding it.
- **`Config::from_env` requires `INGEST_KEYS`** (`src/config.rs:73`, via `required_env`) and is
  called only inside `serve`. Non-serve modes must use `ClickhouseConfig::from_env()` directly —
  this is exactly what `dashboard::run` does (`src/dashboard/mod.rs:24`).
- `SELECT 1 AS value` health checks are duplicated at `src/main.rs:139` and
  `src/dashboard/mod.rs:39` with separate `HealthRow` structs.

## Driver facts — `clickhouse` 0.13.3

Verified against the vendored source; several are non-obvious and easy to get wrong.

- **`.bind()` is client-side substitution, not a prepared statement.** `Part::Arg` placeholders are
  replaced in order with escaped text (`sql/mod.rs`, `sql/escape.rs` — escapes `\ ' \` \t \n`).
  Over-binding errors with `"unexpected bind(), all arguments are already bound"`; under-binding
  with `"unbound query argument"` at `finish()`.
- **`Bind` is blanket-impl'd for `Serialize`.** Binding a Rust enum directly serializes it as a
  *tagged* enum, not as its inner primitive. Always match and bind the primitive.
- `Query::sql_display()` (`query.rs:37`) renders the **post-bind** SQL — a free `--dry-run`.
- `fetch_bytes(format)` (`query.rs:140`) returns a `BytesCursor` and appends ` FORMAT <fmt>` to the
  finished SQL. The cursor implements `AsyncBufRead`, so results can be streamed. This is the only
  way to handle a **dynamic column set** — `#[derive(clickhouse::Row)]` needs a compile-time struct,
  so `fetch_all::<T>()` cannot express one.
- POST queries already get `readonly=1` appended (`query.rs:162`).
- `serde_json::Map` is a `BTreeMap` here (no `preserve_order` feature), so decoding `JSONEachRow`
  into maps **loses column order**. `JSONCompactEachRowWithNamesAndTypes` preserves it.
- Explicit `toUInt32`/`toUInt64` casts in the read SQL are required for RowBinary type matching
  against the row structs — they are not cosmetic.
- Writes never build SQL: `insert_rows` (`src/main.rs:203`) uses `client.insert("events")` with
  `async_insert=1` + `wait_for_async_insert=1` (`src/config.rs:57-58`), so a 200 means ClickHouse
  acknowledged. There are no transactions; a batch is not atomic.
