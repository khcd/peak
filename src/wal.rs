use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::event::EventRow;

#[derive(Debug, Deserialize, Serialize)]
struct WalRecord {
    rows: Vec<EventRow>,
}

/// A small JSON-lines write-ahead log. Each line is a complete accepted HTTP batch, so a single
/// fsync covers all of the events returned in that response. The log is rewritten only after the
/// corresponding ClickHouse insert succeeds; a crash before that rewrite replays safely.
///
/// Exactly one process may own a given path; see `lock_exclusive`.
#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    /// Owns the `flock` for as long as the log is in use. Dropping the `Wal` closes it and
    /// releases the lock.
    _lock: File,
}

impl Wal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "could not create WAL directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| format!("could not open WAL {}: {error}", path.display()))?;
        let lock = lock_exclusive(&path.with_extension("wal.lock"))?;
        let wal = Self { path, _lock: lock };
        // Fail before binding the HTTP listener if a crash left a malformed record. Keeping the
        // file untouched makes the failure recoverable by an operator instead of silently losing
        // an accepted event or accepting new rows behind an unreadable prefix.
        wal.read_all()?;
        Ok(wal)
    }

    pub fn append(&self, rows: &[EventRow]) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        let record = serde_json::to_vec(&WalRecord {
            rows: rows.to_vec(),
        })
        .map_err(|error| format!("could not encode WAL record: {error}"))?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| format!("could not append WAL {}: {error}", self.path.display()))?;
        file.write_all(&record)
            .and_then(|_| file.write_all(b"\n"))
            .and_then(|_| file.sync_data())
            .map_err(|error| format!("could not fsync WAL {}: {error}", self.path.display()))
    }

    pub fn read_prefix(&self, limit: usize) -> Result<Vec<EventRow>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => {
                return Err(format!(
                    "could not read WAL {}: {error}",
                    self.path.display()
                ));
            }
        };
        let mut rows = Vec::with_capacity(limit);
        for line in BufReader::new(file).lines() {
            let line = line
                .map_err(|error| format!("could not read WAL {}: {error}", self.path.display()))?;
            let record = serde_json::from_str::<WalRecord>(&line).map_err(|error| {
                format!("could not decode WAL {}: {error}", self.path.display())
            })?;
            if record.rows.is_empty() {
                return Err(format!(
                    "WAL {} contains an empty record",
                    self.path.display()
                ));
            }
            let remaining = limit.saturating_sub(rows.len());
            rows.extend(record.rows.into_iter().take(remaining));
            if rows.len() == limit {
                break;
            }
        }
        Ok(rows)
    }

    /// Removes exactly `count` rows from the front. The caller serializes this with append and
    /// keeps the ClickHouse success-before-ack ordering, so a failed rewrite leaves the old WAL
    /// intact and only causes a harmless replay.
    pub fn ack_prefix(&self, count: usize) -> Result<(), String> {
        if count == 0 {
            return Ok(());
        }
        let pending = self.read_all()?;
        if count > pending.len() {
            return Err(format!(
                "cannot acknowledge {count} WAL rows; only {} remain",
                pending.len()
            ));
        }
        let remaining = &pending[count..];
        let temporary = self.path.with_extension("wal.tmp");
        let result = (|| {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temporary)
                .map_err(|error| format!("could not create temporary WAL: {error}"))?;
            if !remaining.is_empty() {
                let record = serde_json::to_vec(&WalRecord {
                    rows: remaining.to_vec(),
                })
                .map_err(|error| format!("could not encode compacted WAL: {error}"))?;
                file.write_all(&record)
                    .and_then(|_| file.write_all(b"\n"))
                    .map_err(|error| format!("could not write compacted WAL: {error}"))?;
            }
            file.sync_all()
                .map_err(|error| format!("could not fsync compacted WAL: {error}"))?;
            fs::rename(&temporary, &self.path)
                .map_err(|error| format!("could not replace WAL {}: {error}", self.path.display()))
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    pub(crate) fn read_all(&self) -> Result<Vec<EventRow>, String> {
        let mut rows = Vec::new();
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(rows),
            Err(error) => {
                return Err(format!(
                    "could not read WAL {}: {error}",
                    self.path.display()
                ));
            }
        };
        for line in BufReader::new(file).lines() {
            let line = line
                .map_err(|error| format!("could not read WAL {}: {error}", self.path.display()))?;
            let record = serde_json::from_str::<WalRecord>(&line).map_err(|error| {
                format!("could not decode WAL {}: {error}", self.path.display())
            })?;
            if record.rows.is_empty() {
                return Err(format!(
                    "WAL {} contains an empty record",
                    self.path.display()
                ));
            }
            rows.extend(record.rows);
        }
        Ok(rows)
    }
}

/// Takes a non-blocking exclusive `flock` and returns the handle that owns it.
///
/// Two processes on one WAL path corrupt it silently rather than loudly: `append` writes the record
/// and its newline as separate syscalls, so interleaved batches produce an undecodable line, and
/// `ack_prefix` rewrites the whole file, so a concurrent append is discarded *after* its 200 was
/// returned. Refusing to start is the only safe response, and it turns a deployment mistake into a
/// visible one.
///
/// The lock lives on a sidecar file because `ack_prefix` renames a replacement over the WAL path —
/// the log's own inode is not a stable identity, so a second process would open the replacement and
/// lock it successfully. The sidecar is only ever created, never replaced.
fn lock_exclusive(path: &Path) -> Result<File, String> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|error| format!("could not open WAL lock {}: {error}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `file` owns the descriptor and stays alive for the duration of the call.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            return Err(if error.kind() == std::io::ErrorKind::WouldBlock {
                format!(
                    "another process already holds the WAL lock {}; every instance needs its own WAL_PATH",
                    path.display()
                )
            } else {
                format!("could not lock WAL {}: {error}", path.display())
            });
        }
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::Wal;
    use crate::event::EventRow;
    use std::{fs, path::PathBuf};
    use time::OffsetDateTime;
    use uuid::Uuid;

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

    fn path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "peak-wal-{}-{}.log",
            std::process::id(),
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
    }

    fn cleanup(path: &PathBuf) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("wal.lock"));
    }

    #[test]
    fn append_round_trips_and_ack_compacts_only_the_prefix() {
        let path = path();
        let wal = Wal::open(&path).unwrap();
        wal.append(&[row(1), row(2)]).unwrap();
        wal.append(&[row(3)]).unwrap();

        assert_eq!(wal.read_prefix(2).unwrap().len(), 2);
        wal.ack_prefix(2).unwrap();
        assert_eq!(wal.read_prefix(10).unwrap(), vec![row(3)]);

        cleanup(&path);
    }

    /// `flock` is held per open file description, so a second `open` conflicts even in-process.
    #[cfg(unix)]
    #[test]
    fn open_refuses_a_second_owner_and_releases_on_drop() {
        let path = path();
        let held = Wal::open(&path).unwrap();

        let error = Wal::open(&path).unwrap_err();
        assert!(
            error.contains("another process already holds the WAL lock"),
            "{error}"
        );

        drop(held);
        Wal::open(&path).expect("the lock is released once the owner is dropped");

        cleanup(&path);
    }
}
