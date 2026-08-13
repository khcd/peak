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
| `src/main.rs` | axum router, `/healthz`, `/v2/events`, and durable enqueueing |
| `src/batcher.rs` | fsynced WAL enqueue, size/time batching, ClickHouse insert, startup recovery |
| `src/wal.rs` | append-only JSON-lines WAL and crash-safe prefix compaction |
| `src/cli.rs` | dependency-free parser for `serve`, `dashboard`, and `keygen` |
| `src/event.rs` | `IncomingEvent` (wire) → `EventRow` (storage row) plus envelope validation |
| `src/contract.rs` | Validates `attributes` against the tenant manifest |
| `src/manifest.rs` | Tenant TOML loader; `FleetDimension`, local `common_fields` structs, cycle checking |
| `src/auth.rs` | `INGEST_KEYS` → producer resolution from the bearer token; `generate_secret()` mints parser-safe keys |
| `src/config.rs` | Env + `config.json` config; `ClickhouseConfig::client()` is the only client constructor |
| `src/dashboard/query.rs` | **All read SQL.** Seven builders and two entry points, `fast` / `slow` |
| `src/dashboard/{mod,app,ui}.rs` | TUI event loop, state, rendering, and handler backlog gauge |
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

Two async entry points, both returning plain data structs, both called from `event_loop` on tokio
intervals with one in-flight request per tier. Results are discarded if the tenant or window
changed while in flight; the last good snapshot stays on screen on error.

| entry | tier | issues |
|---|---|---|
| `fast` | 1s poll, 2s timeout | `live_chart_sql`, `connected_sql`, one of `tail_initial_sql`/`tail_since_sql` |
| `slow` | 10s poll, 10s timeout | `totals_sql`, `breakdown_sql`, `fleet_sql` |

The seven query builders are:

| builder | shape | binds |
|---|---|---|
| `live_chart_sql` | `uniqExact(event_id)` by `toStartOfSecond(received_at)`, last 60s | 1 |
| `connected_sql` | `uniqExact(subject_id)` where liveness is within `offline_after_minutes()` | 2 |
| `tail_initial_sql` / `tail_since_sql` | raw row select, no aggregation, `LIMIT 50` | 1 / 2 |
| `totals_sql` | one row, six `uniqExactIf` expressions over three day windows | 1 |
| `breakdown_sql` | top 12 event names | 1 |
| `fleet_sql` | `UNION ALL` per fleet dimension, then group | **N** (one per dimension) |

Notes that constrain any refactor:

- **Every count is `uniqExact`, never `count()`.** `ReplacingMergeTree(received_at)` means duplicate
  `event_id` rows exist until a background merge, so `count()` is simply wrong here.
- **Windows are day-aligned, not rolling.** `Window::since` emits
  `toStartOfDay(now()) - INTERVAL n-1 DAY`, so an N-day window covers N whole days including today.
  Two tests exist purely to keep it that way.
- Day boundaries resolve in the ClickHouse **session** timezone, set once from
  `config::dashboard_timezone()` (`DASHBOARD_TIMEZONE`, default UTC). An unknown name makes
  ClickHouse reject the query, so the health check doubles as its validation.
- Liveness is measured on `received_at`, not `occurred_at`, deliberately — a device with a wrong
  wall clock still counts.
- **`fleet_sql`'s bind count is kept in sync by a loop in the *caller*.** That coupling is invisible
  and is the one genuinely fragile thing in this file. Under-binding fails at `finish()`, so it is a
  runtime error rather than an injection — but it is still a footgun.
- **`attributes` is never read.** It is a JSON document stored as `String`; nothing in
  `src/dashboard/` mentions it. Any attribute-level analytics needs `JSONExtract*` and would be the
  first full-scan query in the project.
- `FleetDimension` (`src/manifest.rs`) is a closed enum whose `name()`/`column()` return
  `&'static str`. **That closedness — not escaping — is what makes the interpolation in `fleet_sql`
  safe.** Widening it to accept caller-supplied column names would turn it into an injection.

## The write path

`POST /v2/events` → authenticate → parse → per-event `validate` → `BatchWriter::enqueue` → 200. One
background task (`BatchWriter::run`, spawned in `start()`) owns batching, ClickHouse, and retries.

- Accepted events are first appended to the JSON-lines WAL and fsynced. A background writer combines
  records up to `MAX_INSERT_BATCH_EVENTS` or until `BATCH_WAIT_MS` elapses (5 seconds by default),
  then uses `client.insert("events")` with `async_insert=1` + `wait_for_async_insert=1`. Thus a 200
  means the service has durably queued the event, not that ClickHouse has already received it.
- On cold start, pending WAL rows are queried by `event_id` and compared against the full stored row.
  Exact matches are removed from the WAL; missing or mismatched rows are replayed. The ClickHouse
  insert happens before WAL compaction, so a crash in between causes a duplicate replay rather than
  data loss. `ReplacingMergeTree(received_at)` plus the dashboard's `uniqExact(event_id)` handles
  that duplicate safely.
- There are no transactions; a ClickHouse batch is not atomic. Rows remain in the WAL until the
  successful insert has been observed and the WAL rewrite completes.
- `/healthz` includes `pending_events` and `batch_capacity` when ClickHouse is healthy. The separate
  dashboard process polls that handler endpoint through `INGEST_HEALTH_URL`; it does not infer the
  queue from ClickHouse, because queued rows have not necessarily been inserted there yet.

Known sharp edges in this path, all currently accepted:

- **`Wal::append` is two `write` syscalls** (the record, then `\n`) on an `O_APPEND` fd. Atomicity
  comes only from the in-process `Arc<Mutex<Wal>>` in `BatchWriter`. See the scaling section below.
- **`Wal::ack_prefix` is a full-file read-modify-write** — `read_all()`, slice off the acked prefix,
  write the remainder to a fixed `<path>.wal.tmp`, `fs::rename`. So each flush is O(pending), which
  goes quadratic under a large backlog, i.e. exactly when ClickHouse is slow. It also compacts all
  remaining records into a single line.
- **Acking is row-granular, not record-granular.** A record is one HTTP request (up to
  `MAX_BATCH_EVENTS`) and a batch is `MAX_INSERT_BATCH_EVENTS` rows; the two limits are independent,
  so once a backlog builds the batch boundary routinely falls mid-record and `read_prefix`
  truncates one. That is why the ack cannot just drop whole lines, and it is the reason for the JSON
  round-trip above. Switching to record granularity would be cheaper but would stall permanently on
  any request larger than the insert batch — a config combination nothing currently forbids.
- **A malformed WAL is a hard failure, by design.** `Wal::open` calls `read_all()` before the
  listener binds and `main` exits 2. Keeping the file untouched makes it operator-recoverable
  instead of silently dropping an accepted event.
- **The retry loop is unbounded.** Insert failures log and `sleep(1s)` forever. No dead letter, no
  backoff growth, no cap on WAL size. `enqueue` keeps returning 200 throughout. During a drain the
  caller's timeout is the only bound on this.

Shutdown is ordered: `axum::serve` finishes its own graceful shutdown first, so no further events
can be enqueued, and only then does `BatchWriter::drain` flush the fixed backlog. A drain skips the
`BATCH_WAIT_MS` window and exits at the first empty read. Rows still pending when
`SHUTDOWN_DRAIN_MS` expires stay on disk and replay on the next start — recoverable only if
`WAL_PATH` is persistent.

## Deployment topology and scaling

The design target is **one ingest process per WAL file**, and it is now enforced: `Wal::open` takes
a non-blocking exclusive `flock` on a `<wal path>.wal.lock` sidecar and the process exits 2 if
another owner holds it. There is still no `node_id` / `instance_id` anywhere — not in config, not in
the log fields, not in the stored row (`ingest_version` is the only provenance column and is
identical across instances).

**Never point two processes at the same `WAL_PATH`.** Both mutating operations are unsafe under
sharing, which is what the lock exists to prevent:

- `ack_prefix` reads the whole file, then renames a rewritten copy over it. A concurrent `append`
  that landed between the read and the rename would be discarded **after** its 200 was returned. The
  window spans a full ClickHouse insert round-trip, not microseconds. `<path>.wal.tmp` is a fixed
  name, so temp files would collide too.
- `append`'s record-then-newline pair could interleave into a line holding two concatenated JSON
  objects. Every subsequent `read_prefix` would fail, the worker would retry every second forever,
  `enqueue` would keep returning 200 (append does not parse), the WAL would grow unbounded, and any
  restart would exit 2 at `Wal::open`. There is no self-heal from that state.

The lock is on a sidecar rather than the log because `ack_prefix` renames a replacement over the WAL
path — its inode is not a stable identity, so a second process would open the replacement and lock
it successfully.

`docker compose up --scale handler=N` mounts one `handler-data` volume into every replica, so the
second replica now fails to start instead of corrupting the log.

Horizontal scaling means **per-instance WAL on per-instance persistent storage**. Ephemeral storage
is a trap: nothing ever adopts an orphaned WAL, since a WAL is only read by a process configured
with that exact path.

What is already correct across instances, and constrains any fix:

- **`event_id` is client-supplied**, so there is no server-side sequence or counter to coordinate.
- **`received_at` is ClickHouse's `now64(3)`** — one shared clock, not a per-instance one.
- **`ReplacingMergeTree` + universal `uniqExact(event_id)`** makes duplicate delivery harmless, so
  at-least-once across instances is already correct at the read layer.
- Auth, validation, contracts, and body limits are pure functions over immutable boot state. There
  is no rate limiter to drift, because the service has none.
- The dashboard's analytics path reads only ClickHouse.

What degrades under multiple instances without breaking:

- **`/healthz` and `UNFLUSHED` stop being answerable.** `pending_events` is a per-process
  `AtomicUsize`; `INGEST_HEALTH_URL` is a single scalar URL fetched by a hand-rolled blocking HTTP
  client. Behind a load balancer the gauge samples a random instance per poll. `/healthz` also runs
  a real `SELECT 1` per call, so LB health checks amplify it N×.
- **Insert pressure multiplies.** Each instance runs its own `BATCH_WAIT_MS` timer and size cap, so
  round-robin turns a small-request stream into N under-full batches. `async_insert=1` absorbs some
  of it; the tuning guidance in `README.md` stops describing reality.
- **Config drift is silent.** `INGEST_KEYS`, `tenants/*.toml`, and `config.json` are read once at
  boot into leaked/immutable structures. Mid-rollout, the same request can 200 on one instance and
  401 or `unknown_contract` on another. Key rotation is already env-update-plus-restart.
- **The dashboard tail can skip rows.** `app` advances a watermark and re-queries with 2s of slack;
  independent per-instance flush timers can land a row older than that slack, and it is then never
  shown in the tail. Aggregates are unaffected.

Two hardening steps make that safe; they are easy to conflate but address different moments:

| | guards against | when | says nothing about |
|---|---|---|---|
| `flock` in `Wal::open` | two processes sharing one file | startup | backlog at exit |
| `BatchWriter::drain` | backlog abandoned on exit | shutdown | file sharing |

Both are implemented. `flock` is a mutex on the file; the drain is application behaviour — on
SIGINT/SIGTERM the HTTP server finishes its in-flight requests, and only then does `serve` flush the
WAL to ClickHouse and let the worker exit.

### Deploying on Kubernetes

A **StatefulSet, not a Deployment** — a Deployment's pods have no stable volume identity, so a
rescheduled pod would not remount its own backlog.

- **`volumeClaimTemplates`**, one PVC per pod (`wal-peak-0`, `wal-peak-1`, …). Use an **RWO,
  block-backed StorageClass. Never RWX or NFS**: `flock` over NFS is advisory-at-best and can be
  silently node-local, which would defeat the guard exactly where it matters.
- **`persistentVolumeClaimRetentionPolicy.whenScaled` must stay `Retain`** (the default). Setting it
  to `Delete` destroys the WAL of any scaled-down pod along with whatever it had not yet flushed.
  Scaling back up re-attaches the PVC and the backlog replays.
- **`podManagementPolicy: Parallel`.** Ingest pods have no ordering relationship, and the default
  `OrderedReady` serialises rollouts for no benefit.
- **`terminationGracePeriodSeconds` must exceed `preStop` + `SHUTDOWN_DRAIN_MS`.** With the 25s
  default drain and a 5s `preStop`, use 40s. The Kubernetes default of 30s leaves no margin and
  SIGKILL would cut the drain short.
- **A `preStop` sleep of ~5s.** Endpoint removal races SIGTERM, so without it a terminating pod can
  still receive requests after it has stopped accepting them.

**Do not use `/healthz` as the readiness probe.** It runs a real ClickHouse `SELECT 1`, so a
ClickHouse blip would fail readiness on every pod at once, drop the whole fleet out of the Service,
and return connection errors to clients — precisely the outage the WAL exists to absorb, since the
service can still accept and durably queue with ClickHouse down. Use a `tcpSocket` readiness probe,
or add a cheap `/readyz` that reflects only "can I accept and fsync". `/healthz` remains the right
probe for humans and for `INGEST_HEALTH_URL`, and is fine as a low-frequency liveness check —
budget for one ClickHouse query per pod per period.

Secrets and manifests: `INGEST_KEYS` in a Secret, `tenants/*.toml` in a ConfigMap. Both are read
once at boot, so a rollout has a window where pods disagree; see the config-drift note below.

Autoscaling is not appropriate here. Each replica owns durable state, and scale-in strands a PVC
whose events only replay when the ordinal comes back.

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

- **`.bind()` is client-side substitution, not a prepared statement.** `Part::Arg` placeholders are
  replaced in order with escaped text (escapes `\ ' \` \t \n`). Over-binding errors with
  `"unexpected bind(), all arguments are already bound"`; under-binding with
  `"unbound query argument"` at `finish()`.
- `Bind` is blanket-implemented for `Serialize`. Binding a Rust enum directly serializes it as a
  *tagged* enum, not its inner primitive; match and bind the primitive instead.
- `Query::sql_display()` renders post-bind SQL and is useful as a free dry run.
- `fetch_bytes(format)` returns a `BytesCursor` implementing `AsyncBufRead` and appends
  ` FORMAT <fmt>`. It is the only way to handle a **dynamic column set** —
  `#[derive(clickhouse::Row)]` needs a compile-time struct, so `fetch_all::<T>()` cannot express one.
- POST queries already get `readonly=1` appended.
- `serde_json::Map` is a `BTreeMap` here (no `preserve_order` feature), so decoding `JSONEachRow`
  into maps **loses column order**. `JSONCompactEachRowWithNamesAndTypes` preserves it.
- Explicit `toUInt32`/`toUInt64` casts in the read SQL are required for RowBinary type matching
  against the row structs — they are not cosmetic.
