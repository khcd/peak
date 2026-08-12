# Plan: pluggable analytics backend — ClickHouse / PostgreSQL / MongoDB

**Status: not started.** Nothing here has been implemented or benchmarked. Every size figure is an
estimate.

Read [`../CLAUDE.md`](../CLAUDE.md) first — it is the authoritative description of the codebase as it
stands. This document only covers the change, and assumes that context.

## Context

Today the service is ClickHouse-only. `clickhouse::Client` is a concrete type threaded through
`AppState.clickhouse` (`src/main.rs:43`), `query::fast`/`query::slow`, and `dashboard::event_loop`
(`src/dashboard/mod.rs:71`), with `clickhouse::error::Result` in the read signatures. There is no
database abstraction of any kind.

The goal is that an operator picks whichever analytics store they are comfortable with, across the
full spectrum — columnar (ClickHouse), relational (PostgreSQL), document (MongoDB). All three
coexist behind one seam, and the dashboard works on all of them.

Neither Postgres nor MongoDB appears anywhere today (verified: no driver in `Cargo.lock`), so this is
a two-engine addition, not a port.

> **Scope history.** Earlier passes considered SQLite, then Redis. Both dropped. Redis was rejected
> on analysis: it has no query language, so all seven builders would have had to be materialized at
> write time. That breaks live tenant-config changes — `dashboard.windows` and `fleet_dimensions`
> currently take effect on restart because they only change a query — and HyperLogLog would have
> made counts approximate. MongoDB's aggregation pipeline keeps every query ad-hoc.

## What makes this tractable

**There is no read API.** `router()` (`src/main.rs:114`) exposes only `GET /healthz` and
`POST /v2/events`; every analytics query is reachable only by running the TUI. So the entire read
abstraction has *no external contract to preserve* — no HTTP responses, no clients, no compatibility
window. If a backend returns rows in a slightly different order, the blast radius is one operator's
terminal. This is the single biggest reason the change is safe to attempt.

The write path, by contrast, does have a contract: a 200 means durable
(`src/main.rs:203-227`, `src/config.rs:57-58`). That is the part to be careful with.

## Decisions

1. **Selection: runtime config, all three drivers compiled in.** One binary; a `database.backend`
   discriminator picks at startup. No `#[cfg]` fan-out through the store layer, one CI matrix cell
   per engine rather than per feature combination. Costs binary size and always-present deps.

2. **Dedup: unify on a single exact-count shape.** Every count becomes exact-by-construction rather
   than exact-by-aggregation.
   - **Postgres** — `event_id` is the primary key; `INSERT … ON CONFLICT DO NOTHING`.
   - **MongoDB** — `_id` *is* `event_id` (BSON Binary subtype 4). Free unique index, nothing extra to
     maintain; duplicate suppression from `insertMany` with `ordered: false` tolerating `E11000`.
     Distinct-count collapses to `$sum: 1`.
   - **ClickHouse** — `ReplacingMergeTree` + `FROM events FINAL`. See the caveat below.

3. **Scope: full parity in one change.** Write path *and* all seven builders land together. No
   intermediate state where choosing Postgres silently costs you the dashboard.

4. **Migrations: minimal.** A `schema_version` table/collection, embedded ordered DDL per backend,
   applied on demand. On MongoDB "migration" means index creation, not DDL. No down-migrations, no
   CLI, no drift detection — those are production-shaped and cheap to add later *on top of* a version
   table. Retrofitting the version table itself is the expensive part, so it goes in now.

5. **ClickHouse drift: fold `002`–`004` into one baseline**, including the currently-optional `003`
   skip index, before that duplication gets copied to three engines.

### Caveat on decision 2 — ClickHouse cannot do write-path dedup

ClickHouse has no unique constraints and no `ON CONFLICT`. The mechanism that delivers the unified
exact-count shape is the **`FINAL` modifier**, which applies `ReplacingMergeTree` collapsing at query
time rather than waiting for a background merge.

- `FINAL` is slower than a plain scan and must be **measured**, not assumed.
  `do_not_merge_across_partitions_select_final` is the usual mitigation, and is safe here because the
  table partitions by `toYYYYMM(occurred_at)`.
- Each `UNION ALL` subquery in `fleet_sql` needs its own `FINAL`, not just the outer select.
- If `FINAL` benchmarks badly, the fallback is per-backend count semantics (ClickHouse keeps
  `uniqExact`), which reintroduces two correctness models in the trait. **This is why phase 0
  exists.**

## The seam

Five choke points, already narrow:

| # | Location | Note |
|---|---|---|
| 1 | `query::fast` (`src/dashboard/query.rs:92`) and `query::slow` (:138) | Two async read entry points returning plain data structs. **This is the real seam.** |
| 2 | The seven builders (:161-233) | Pure `fn(&Tenant) -> String`, no I/O — but see the correction below |
| 3 | `insert_rows` (`src/main.rs:203`) | The single write path: `&[EventRow]` → `Result<(), ApiError>` |
| 4 | `ClickhouseConfig::client()` (`src/config.rs:45`) | The single construction point |
| 5 | `EventRow` (`src/event.rs:43-62`) | Needs BSON and SQL encodings alongside the ClickHouse `Row` derive |

### Seam correction — the trait returns data, not SQL

The obvious move is a `trait Dialect` whose methods return SQL strings, mirroring the seven builders.
**That cannot cover MongoDB**, whose aggregation pipelines are BSON documents, not strings.

So the primary abstraction is **`trait TelemetryStore`, returning the plain data structs** —
`SecondBucket`, `TailRow`, `Connected`, `Totals`, `CountRow`, `FleetRow` (`src/dashboard/query.rs:42-90`).
`Dialect` demotes to an implementation detail shared only by the ClickHouse and Postgres impls.
Choke point 1 already has exactly this shape, so this costs nothing — but it must be settled before
phase 1 fixes the trait.

Also fold in the duplicated `SELECT 1 AS value` health checks (`src/main.rs:139`,
`src/dashboard/mod.rs:39`, with separate `HealthRow` structs).

Two driver facts from `CLAUDE.md` bear directly on this:

- `#[derive(clickhouse::Row)]` needs a compile-time struct, so a dynamic column set requires
  `fetch_bytes(format)`. The fixed result structs above keep us out of that entirely — **do not**
  make the trait generic over column sets.
- `Bind` is blanket-impl'd for `Serialize`, so binding a Rust enum serializes it *tagged*. Any
  backend-selection or dimension enum must be matched and bound as its primitive.

## Per-engine mapping

| ClickHouse | PostgreSQL | MongoDB |
|---|---|---|
| `uniqExact(event_id)` (:163,178,215,229) | `COUNT(*)` — exact, given the PK | `$sum: 1` — exact, given `_id` |
| `uniqExactIf(v, cond)` (:206) | `COUNT(*) FILTER (WHERE …)` | `$facet` with per-window `$match` |
| `toStartOfDay(now())` (:29,199) | `date_trunc('day', …)` | `$dateTrunc` (**MongoDB 5.0+**) |
| `toStartOfSecond` / `toUnixTimestamp` (:162) | `date_trunc('second', …)` | `$dateTrunc`, unit `second` |
| `fromUnixTimestamp64Milli(?, 'UTC')` (:193) | `to_timestamp(? / 1000.0)` | BSON Date, bound directly |
| `now() - INTERVAL N DAY` (:30,165,180,187) | same syntax | computed in Rust, bound |
| `toUInt32`/`toUInt64` casts | mostly disappear | disappear |
| `UNION ALL` in `fleet_sql` (:224) | same | `$unionWith` or `$facet` |
| `SELECT … WHERE false`, no `FROM` (:226) | needs a shim | empty pipeline result |
| `LowCardinality(String)` ×8 | `TEXT` | plain BSON string |
| `DateTime64(3, 'UTC')` | `timestamptz` | BSON Date (ms — exact match) |
| Two-rule conditional `TTL` | scheduled `DELETE` | computed `expires_at` + TTL index |

### MongoDB specifics

- **Retention.** TTL indexes carry one `expireAfterSeconds` and cannot express the current two-rule
  conditional expiry. Use the standard idiom: write a computed `expires_at` at ingest
  (`occurred_at + 2d` for `live_ping`, `+180d` otherwise) with a single
  `{expires_at: 1}, expireAfterSeconds: 0` index. The reaper runs on a ~60s cycle, so expiry is
  approximate — fine for retention, but the dashboard must not depend on it.
- **`attributes` stays a JSON string.** A BSON subdocument is the natural document-store move, but
  manifest-declared field names can contain `.` and `$`, which are restricted in MongoDB field names.
  It is never queried today (nothing in `src/dashboard/` mentions it), so parity wins. Revisit only
  if attribute-level analytics is ever wanted — on ClickHouse that would need `JSONExtract*` and
  would be the project's first full-scan query anyway.
- **`$facet` memory.** Stages cap at 100MB without `allowDiskUse`. `totals_sql`'s six counts are safe
  *because* decision 2 makes them `$sum: 1` rather than set accumulation — but the
  distinct-`subject_id` counts still accumulate, bounded by fleet size. Verify at realistic scale.
- **Durability.** `{w: "majority", j: true}` matches the current `wait_for_async_insert=1` contract.
  Multi-document transactions exist but require a replica set.
- **Minimum version: MongoDB 5.0** for `$dateTrunc`; 6.0+ is the safer floor.
- Driver: the official `mongodb` crate (async/tokio).

## Hazard: config-driven query construction

Three builders interpolate with `format!` rather than binding. On MongoDB this becomes pipeline
construction, where the same care applies to `$` operator keys and field paths.

- `query.rs:30`, `:202` — `INTERVAL {} DAY` from `dashboard.windows`.
- `query.rs:178-181` — `{minutes}` from `ping_interval_minutes`.
- `query.rs:206` — window starts interpolated six times.
- `query.rs:224` — interpolates **both a string literal and a bare column name** from
  `FleetDimension::name()`/`column()` (`src/manifest.rs:138-153`).

What makes this safe today is **closedness, not escaping**: `FleetDimension` is a closed enum
returning `&'static str`, the TOML parser rejects anything else (`src/manifest.rs:401-406`), and
`valid_producer_name` (`src/auth.rs:68`) restricts tenant names to `[a-z0-9_]{1,32}`. Preserve all
three properties — they are equally what makes MongoDB field-path construction safe.

Separately, `fleet_sql`'s bind count is kept in sync by a loop in the *caller* (`query.rs:153-156`).
That coupling is invisible and is the one genuinely fragile thing in the file. The trait should
return placeholders and params **together** so it cannot drift. Note Postgres uses `$1`-style
placeholders, so placeholder style is itself dialect state.

## Sequenced work

Each phase is a reviewable commit that leaves the tree green.

| # | Phase | Touches | Rough size |
|---|---|---|---|
| 0 | **`FINAL` benchmark spike.** Measure `COUNT(*) … FINAL` against today's `uniqExact` on real data. Gates decision 2 and the phase 1 trait shape. | throwaway | ~1 session |
| 1 | **Seam extraction.** `trait TelemetryStore` returning data structs. ClickHouse stays the only impl. Zero behavior change. | `main.rs`, `dashboard/{mod,query}.rs`, new `store/` | ~300 changed, ~250 new |
| 2 | **Config restructure.** `database.backend` discriminator + per-backend settings, replacing hardcoded `CLICKHOUSE_*` names and the literal `clickhouse` JSON key. Keep `deny_unknown_fields`. Note `Config::from_env` requires `INGEST_KEYS` and is serve-only — the dashboard path must stay able to build a client without it. | `config.rs`, `.env.example`, `docker-compose.yml` | ~150 |
| 3 | **Schema runner + baselines.** `schema_version`, embedded ordered DDL, MongoDB index creation. Folds ClickHouse `002`–`004` into one baseline. | new `store/migrate.rs`, `deploy/*/` | ~250 code, ~200 SQL |
| 4 | **Day-boundary math into Rust.** Replace `toStartOfDay(now())` + `session_timezone` with computed timestamps bound as parameters. All three engines *can* do this server-side; doing it in Rust removes a whole class of cross-backend disagreement. Keep the two day-alignment tests (`query.rs:247,262`) passing. | `dashboard/query.rs`, `config.rs` | ~100 |
| 5 | **Postgres impl.** `ON CONFLICT DO NOTHING`, `$1` placeholders, `COUNT(*) FILTER`, retention job. | new `store/postgres.rs` | ~300 + ~150 dialect |
| 6 | **MongoDB impl.** `_id = event_id`, unordered `insertMany`, seven pipelines, `expires_at` TTL index. Pipelines are verbose — the largest single impl. | new `store/mongo.rs` | ~450 |
| 7 | **ClickHouse `FINAL` migration.** Convert off `uniqExact`/`uniqExactIf`. Only if phase 0 says yes. | `store/clickhouse.rs` | ~150 |
| 8 | **Tests + CI.** See below. | `query.rs` tests, new `tests/`, `ci.yml` | ~600 |

Roughly **2,400–2,900 new/changed lines against a 2,900-line codebase** — this more than doubles the
project. Phases 1–2 are worth doing on their own merits regardless of whether the other engines land.

## Verification

The current regression net is thin and entirely synthetic: the tests at `src/dashboard/query.rs:235-307`
are pure string assertions, and **no test touches a database**. That has to change, because string
assertions cannot prove that `COUNT(*) FILTER` and `$facet` and `uniqExactIf` return the same number.

Three layers:

1. **Dialect string assertions, parameterized per backend.** Cheap, no containers, keeps the existing
   tests' intent. `Query::sql_display()` (`clickhouse` `query.rs:37`) renders **post-bind** SQL — a
   free dry-run, and the right tool for asserting on fully-bound output rather than templates.
2. **Live round-trip suite.** Seed identical fixture events into all three engines, run all seven
   builders, assert the results are identical. This is the load-bearing test and the reason phase 8
   is large. Fixtures must include: duplicate `event_id`s (proving decision 2 on each engine),
   `live_ping` rows (retention), events with empty `platform`/`country` (the `value != ''` filter in
   `fleet_sql`), and a tenant with zero `fleet_dimensions` (the `WHERE false` path).
3. **CI containers.** Add Postgres + MongoDB + ClickHouse services. CI currently has no DB service at
   all, so this is new infrastructure. Keep the existing `manifest-compat` job untouched.

Manual end-to-end check per backend: `docker compose up`, `POST /v2/events` with the README's sample
batch, confirm `200 {"accepted":1}`, then `cargo run -- dashboard tenant-name` and confirm the tail and
counters populate.

## Where the estimate breaks

Writing the code is mechanical; verification is the cost. Budget per phase — the whole codebase is
only ~35k tokens to read, so each phase fits its own focused context, and phases 5–8 each want
several implement-test-iterate rounds against live engines.

- **Phase 0 gates phase 1.** If `FINAL` is too slow, the trait must carry two count models, and that
  ripples through every later phase. Do not skip it.
- **Phase 6** has the most unfamiliar surface: BSON type mapping (UUID subtype 4, Date precision),
  pipeline verbosity, `$facet` memory at realistic cardinality.
- **Phase 8** is where "all three agree" turns out to be false in small ways — integer widths,
  timestamp precision, ordering ties, and **empty-string vs null vs missing-field**, which MongoDB
  distinguishes and the SQL engines do not. This last one interacts directly with `fleet_sql`'s
  `value != ''` filter. Expect corrections to flow back into phases 5–7.

## Still open

- **Postgres retention.** No table-level TTL; needs a scheduled `DELETE` or partition drop. Combined
  with MongoDB's ~60s reaper and ClickHouse's merge-time TTL, all three expire on different
  schedules. Decide whether that divergence is acceptable or needs papering over.
- **Durability contract.** There are no transactions anywhere today — a batch is not atomic and a
  partial write is possible. Postgres and MongoDB would both silently *fix* this. Decide whether to
  promote it to a stated guarantee, which would then be a contract ClickHouse cannot meet.
- **Positioning.** `uniqExact` over a 180-day window is what ClickHouse is *for*. The other two will
  not match it at volume; set expectations in the README rather than implying a free swap.
