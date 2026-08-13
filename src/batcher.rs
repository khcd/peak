use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use clickhouse::Client;
use tokio::{sync::Notify, task::JoinHandle, time};
use tracing::{debug, error, info, warn};

use crate::{event::EventRow, wal::Wal};

const INSERT_TABLE: &str = "events";
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct BatchWriter {
    clickhouse: Client,
    wal: Arc<Mutex<Wal>>,
    max_events: usize,
    wait: Duration,
    notify: Arc<Notify>,
    pending_events: Arc<AtomicUsize>,
    draining: Arc<AtomicBool>,
}

impl BatchWriter {
    pub fn new(
        clickhouse: Client,
        wal_path: impl Into<std::path::PathBuf>,
        max_events: usize,
        wait: Duration,
    ) -> Result<Arc<Self>, String> {
        if max_events == 0 {
            return Err("MAX_INSERT_BATCH_EVENTS must be positive".into());
        }
        let wal = Wal::open(wal_path)?;
        let pending_events = wal.read_all()?.len();
        Ok(Arc::new(Self {
            clickhouse,
            wal: Arc::new(Mutex::new(wal)),
            max_events,
            wait,
            notify: Arc::new(Notify::new()),
            pending_events: Arc::new(AtomicUsize::new(pending_events)),
            draining: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// The returned handle must be passed to `drain` at shutdown; dropping it detaches the worker
    /// and abandons whatever is still in the WAL.
    #[must_use]
    pub fn start(self: &Arc<Self>) -> JoinHandle<()> {
        let writer = Arc::clone(self);
        tokio::spawn(async move { writer.run().await })
    }

    /// Flushes everything already accepted, then lets the worker exit.
    ///
    /// Call this only after the HTTP server has finished its own graceful shutdown, so that no
    /// further events can be enqueued while the WAL is being emptied. Rows still pending when
    /// `limit` expires stay durable on disk and replay on the next start of a process configured
    /// with the same `WAL_PATH` — which is why that path has to be persistent storage.
    pub async fn drain(&self, worker: JoinHandle<()>, limit: Duration) {
        self.draining.store(true, Ordering::Release);
        // `notify_one` stores a permit if the worker has not parked yet, so this cannot race with
        // the worker's own emptiness check.
        self.notify.notify_one();
        match time::timeout(limit, worker).await {
            Ok(Ok(())) => info!("drained the telemetry WAL"),
            Ok(Err(error)) => {
                error!(%error, "telemetry writer stopped before the WAL was drained");
            }
            Err(_) => warn!(
                pending_events = self.pending_events(),
                timeout_ms = limit.as_millis() as u64,
                "timed out draining the telemetry WAL; pending rows replay on the next start"
            ),
        }
    }

    /// Appending and fsyncing happens before the HTTP handler reports acceptance. ClickHouse is
    /// deliberately not on this request path; the worker owns batching and retries.
    pub async fn enqueue(&self, rows: Vec<EventRow>) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len();
        let wal = Arc::clone(&self.wal);
        tokio::task::spawn_blocking(move || {
            let wal = wal
                .lock()
                .map_err(|_| "WAL lock was poisoned".to_string())?;
            wal.append(&rows)
        })
        .await
        .map_err(|error| format!("WAL append task failed: {error}"))??;
        self.pending_events.fetch_add(count, Ordering::Relaxed);
        self.notify.notify_one();
        Ok(())
    }

    pub fn pending_events(&self) -> usize {
        self.pending_events.load(Ordering::Relaxed)
    }

    pub fn batch_capacity(&self) -> usize {
        self.max_events
    }

    #[cfg(test)]
    pub async fn pending_rows(&self) -> Result<Vec<EventRow>, String> {
        let wal = Arc::clone(&self.wal);
        tokio::task::spawn_blocking(move || {
            let wal = wal
                .lock()
                .map_err(|_| "WAL lock was poisoned".to_string())?;
            wal.read_all()
        })
        .await
        .map_err(|error| format!("WAL read task failed: {error}"))?
    }

    async fn run(&self) {
        let mut recovering = true;
        let mut first_pending_at = None;

        loop {
            let mut rows = match self.read_prefix().await {
                Ok(rows) => rows,
                Err(message) => {
                    error!(%message, "could not read telemetry WAL");
                    time::sleep(RETRY_DELAY).await;
                    continue;
                }
            };
            if rows.is_empty() {
                recovering = false;
                first_pending_at = None;
                // An empty WAL is the only safe exit point: everything accepted has been observed
                // by ClickHouse and acknowledged.
                if self.draining.load(Ordering::Acquire) {
                    return;
                }
                self.notify.notified().await;
                continue;
            }

            // A drain flushes whatever is already accepted immediately — waiting out the batch
            // window would only delay the exit, and the caller's timeout is the real budget.
            let draining = self.draining.load(Ordering::Acquire);
            if !recovering && !draining && rows.len() < self.max_events {
                let pending_at = first_pending_at.get_or_insert_with(Instant::now);
                let remaining = self.wait.saturating_sub(pending_at.elapsed());
                tokio::select! {
                    _ = time::sleep(remaining) => {},
                    _ = self.notify.notified() => continue,
                }
                rows = match self.read_prefix().await {
                    Ok(rows) => rows,
                    Err(message) => {
                        error!(%message, "could not read telemetry WAL after batch wait");
                        time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                };
                if rows.is_empty() {
                    first_pending_at = None;
                    continue;
                }
            }

            let count = rows.len();
            let result = if recovering {
                recover_rows(&self.clickhouse, &rows).await
            } else {
                insert_rows(&self.clickhouse, &rows)
                    .await
                    .map_err(|error| error.to_string())
            };
            match result {
                Ok(()) => {
                    if let Err(message) = self.ack_prefix(count).await {
                        error!(%message, "ClickHouse rows committed but WAL acknowledgement failed");
                        time::sleep(RETRY_DELAY).await;
                    } else {
                        first_pending_at = None;
                        debug!(accepted = count, recovering, "flushed telemetry rows");
                    }
                }
                Err(message) => {
                    error!(%message, recovering, "could not flush telemetry rows to ClickHouse");
                    time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    async fn read_prefix(&self) -> Result<Vec<EventRow>, String> {
        let wal = Arc::clone(&self.wal);
        let max_events = self.max_events;
        tokio::task::spawn_blocking(move || {
            let wal = wal
                .lock()
                .map_err(|_| "WAL lock was poisoned".to_string())?;
            wal.read_prefix(max_events)
        })
        .await
        .map_err(|error| format!("WAL read task failed: {error}"))?
    }

    async fn ack_prefix(&self, count: usize) -> Result<(), String> {
        let wal = Arc::clone(&self.wal);
        tokio::task::spawn_blocking(move || {
            let wal = wal
                .lock()
                .map_err(|_| "WAL lock was poisoned".to_string())?;
            wal.ack_prefix(count)
        })
        .await
        .map_err(|error| format!("WAL acknowledgement task failed: {error}"))??;
        self.pending_events.fetch_sub(count, Ordering::Relaxed);
        Ok(())
    }
}

/// The normal path sends one insert per size/time batch. `wait_for_async_insert=1` remains set on
/// the client, so a successful call means ClickHouse has accepted all rows in that batch.
pub async fn insert_rows(client: &Client, rows: &[EventRow]) -> clickhouse::error::Result<()> {
    let mut insert = client.insert(INSERT_TABLE)?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await
}

async fn recover_rows(client: &Client, rows: &[EventRow]) -> Result<(), String> {
    let mut query = client.query(&existing_rows_sql(rows));
    for row in rows {
        query = query.bind(row.event_id.to_string());
    }
    let existing = query
        .fetch_all::<EventRow>()
        .await
        .map_err(|error| format!("could not verify WAL rows against ClickHouse: {error}"))?;

    let replay = rows_needing_replay(&existing, rows);
    for row in &replay {
        if existing
            .iter()
            .any(|candidate| candidate.event_id == row.event_id)
        {
            warn!(event_id = %row.event_id, "WAL row differs from the ClickHouse row; replaying WAL contents");
        }
    }
    if !replay.is_empty() {
        insert_rows(client, &replay)
            .await
            .map_err(|error| format!("could not replay WAL rows: {error}"))?;
    }
    Ok(())
}

fn rows_needing_replay(existing: &[EventRow], wal_rows: &[EventRow]) -> Vec<EventRow> {
    wal_rows
        .iter()
        .filter(|row| !existing.iter().any(|candidate| candidate == *row))
        .cloned()
        .collect()
}

fn existing_rows_sql(rows: &[EventRow]) -> String {
    let ids = std::iter::repeat_n("toUUID(?)", rows.len())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT event_id, producer, event_name, schema_version, occurred_at, subject_kind, subject_id, session_id, service_name, service_version, platform, platform_version, attributes, country, ingest_version FROM {INSERT_TABLE} WHERE event_id IN ({ids})"
    )
}

#[cfg(test)]
mod tests {
    use super::{BatchWriter, existing_rows_sql, rows_needing_replay};
    use crate::event::EventRow;
    use std::{fs, path::PathBuf, time::Duration};
    use time::OffsetDateTime;
    use uuid::Uuid;

    fn path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "peak-batcher-{}-{}.wal",
            std::process::id(),
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
    }

    fn cleanup(path: &PathBuf) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("wal.lock"));
    }

    fn row(id: u128) -> EventRow {
        EventRow {
            event_id: Uuid::from_u128(id),
            producer: "demo".into(),
            event_name: "session_start".into(),
            schema_version: 1,
            occurred_at: OffsetDateTime::UNIX_EPOCH,
            subject_kind: "install".into(),
            subject_id: "subject".into(),
            session_id: String::new(),
            service_name: "service".into(),
            service_version: "version".into(),
            platform: String::new(),
            platform_version: String::new(),
            attributes: "{}".into(),
            country: String::new(),
            ingest_version: "test".into(),
        }
    }

    #[tokio::test]
    async fn enqueue_is_durable_before_the_worker_runs() {
        let path = path();
        let writer = BatchWriter::new(
            clickhouse::Client::default(),
            &path,
            100,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(writer.pending_events(), 0);
        writer.enqueue(vec![row(1)]).await.unwrap();
        assert_eq!(writer.pending_events(), 1);
        assert_eq!(writer.pending_rows().await.unwrap(), vec![row(1)]);
        cleanup(&path);
    }

    /// An empty WAL is the worker's exit point, so this covers the drain handshake without needing
    /// a ClickHouse to insert into. Returning well inside the limit is the assertion — a hung
    /// worker would burn the whole timeout instead.
    #[tokio::test]
    async fn drain_stops_the_worker_once_the_wal_is_empty() {
        let path = path();
        let writer = BatchWriter::new(
            clickhouse::Client::default(),
            &path,
            100,
            Duration::from_secs(60),
        )
        .unwrap();
        let worker = writer.start();

        let started = std::time::Instant::now();
        writer.drain(worker, Duration::from_secs(10)).await;

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "drain should exit on the first empty read, took {:?}",
            started.elapsed()
        );
        assert_eq!(writer.pending_events(), 0);
        cleanup(&path);
    }

    #[test]
    fn recovery_skips_exact_rows_but_replays_missing_and_mismatched_rows() {
        let exact = row(1);
        let mut mismatch = row(2);
        mismatch.attributes = r#"{"changed":true}"#.into();
        let missing = row(3);
        let mut wal_mismatch = row(2);
        wal_mismatch.attributes = "{}".into();

        assert_eq!(
            rows_needing_replay(
                &[exact.clone(), mismatch],
                &[exact, wal_mismatch.clone(), missing.clone()]
            ),
            vec![wal_mismatch, missing]
        );
    }

    #[test]
    fn recovery_query_has_one_uuid_bind_per_wal_row() {
        let sql = existing_rows_sql(&[row(1), row(2)]);
        assert_eq!(sql.matches("toUUID(?)").count(), 2);
        assert!(sql.contains("WHERE event_id IN (toUUID(?), toUUID(?))"));
    }
}
