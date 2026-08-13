use std::collections::{HashSet, VecDeque};

use time::OffsetDateTime;
use uuid::Uuid;

use super::query::{CountRow, FleetRow, SecondBucket, TailRow, Totals, Window};
use crate::manifest::Tenant;

const TAIL_CAPACITY: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestHealth {
    pub pending_events: usize,
    pub batch_capacity: usize,
}

pub struct App {
    pub tenant: &'static Tenant,
    /// Reporting timezone every calendar-day window resolves in. Shown in the header so a figure
    /// read off this dashboard is never ambiguous about which "today" it means.
    pub timezone: String,
    pub window: Window,
    pub paused: bool,
    pub buckets: [u64; 60],
    /// Distinct subjects that have emitted the tenant's liveness event recently enough. A sleeping
    /// or suspended device stops pinging and correctly drops out of this count.
    pub connected: Option<u64>,
    pub totals: Totals,
    pub breakdown: Vec<CountRow>,
    pub fleet: Vec<FleetRow>,
    pub tail: VecDeque<TailRow>,
    tail_ids: HashSet<Uuid>,
    pub watermark: Option<OffsetDateTime>,
    pub fast_ok: Option<OffsetDateTime>,
    pub slow_ok: Option<OffsetDateTime>,
    pub fast_error: Option<String>,
    pub slow_error: Option<String>,
    pub pending_events: Option<usize>,
    pub pending_capacity: usize,
    pub ingest_health_error: Option<String>,
}

impl Default for App {
    fn default() -> Self {
        let registry = Box::leak(Box::new(
            crate::manifest::Registry::load(std::path::Path::new("tenants")).unwrap(),
        ));
        Self::new(registry.first().unwrap(), "UTC".to_owned())
    }
}

impl App {
    pub fn new(tenant: &'static Tenant, timezone: String) -> Self {
        Self {
            tenant,
            timezone,
            window: Window::D7,
            paused: false,
            buckets: [0; 60],
            connected: None,
            totals: Totals::default(),
            breakdown: Vec::new(),
            fleet: Vec::new(),
            tail: VecDeque::new(),
            tail_ids: HashSet::new(),
            watermark: None,
            fast_ok: None,
            slow_ok: None,
            fast_error: None,
            slow_error: None,
            pending_events: None,
            pending_capacity: 200,
            ingest_health_error: None,
        }
    }

    pub fn apply_fast(
        &mut self,
        buckets: Vec<SecondBucket>,
        rows: Vec<TailRow>,
        connected: Option<u64>,
    ) {
        let newest = buckets
            .iter()
            .map(|row| row.bucket)
            .max()
            .unwrap_or_else(current_second);
        self.buckets = fill_buckets(&buckets, newest);
        self.connected = connected;
        self.advance_watermark(&rows);
        self.insert_tail(rows);
        self.fast_ok = Some(OffsetDateTime::now_utc());
        self.fast_error = None;
    }

    pub fn switch_tenant(&mut self, tenant: &'static Tenant) {
        self.tenant = tenant;
        self.buckets = [0; 60];
        self.tail.clear();
        self.tail_ids.clear();
        self.watermark = None;
        self.connected = None;
        self.totals = Totals::default();
        self.breakdown.clear();
        self.fleet.clear();
        self.fast_ok = None;
        self.slow_ok = None;
        self.fast_error = None;
        self.slow_error = None;
    }

    pub fn apply_slow(&mut self, totals: Totals, breakdown: Vec<CountRow>, fleet: Vec<FleetRow>) {
        self.totals = totals;
        self.breakdown = breakdown;
        self.fleet = fleet;
        self.slow_ok = Some(OffsetDateTime::now_utc());
        self.slow_error = None;
    }

    pub fn apply_ingest_health(&mut self, result: Result<IngestHealth, String>) {
        match result {
            Ok(health) => {
                self.pending_events = Some(health.pending_events);
                self.pending_capacity = health.batch_capacity.max(1);
                self.ingest_health_error = None;
            }
            Err(error) => {
                self.pending_events = None;
                self.ingest_health_error = Some(error);
            }
        }
    }

    pub fn insert_tail(&mut self, rows: Vec<TailRow>) {
        for row in rows.into_iter().rev() {
            if !self.tail_ids.insert(row.event_id) {
                continue;
            }
            self.tail.push_front(row);
            if self.tail.len() > TAIL_CAPACITY
                && let Some(evicted) = self.tail.pop_back()
            {
                self.tail_ids.remove(&evicted.event_id);
            }
        }
    }

    pub fn advance_watermark(&mut self, rows: &[TailRow]) {
        if let Some(latest) = rows.iter().map(|row| row.received_at).max()
            && self.watermark.is_none_or(|watermark| latest > watermark)
        {
            self.watermark = Some(latest);
        }
    }

    pub fn error_message(&self) -> Option<&str> {
        self.fast_error.as_deref().or(self.slow_error.as_deref())
    }

    pub fn last_ok(&self) -> Option<OffsetDateTime> {
        self.fast_ok.into_iter().chain(self.slow_ok).max()
    }
}

pub fn fill_buckets(rows: &[SecondBucket], newest: u32) -> [u64; 60] {
    let mut buckets = [0; 60];
    let first = newest.saturating_sub(59);
    for row in rows {
        if (first..=newest).contains(&row.bucket) {
            buckets[(row.bucket - first) as usize] = row.events;
        }
    }
    buckets
}

fn current_second() -> u32 {
    OffsetDateTime::now_utc().unix_timestamp().max(0) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;

    fn tail(id: u128, received_at: OffsetDateTime) -> TailRow {
        TailRow {
            event_id: Uuid::from_u128(id),
            event_name: "session_start".into(),
            subject_id: "x".into(),
            platform: String::new(),
            service_version: String::new(),
            country: String::new(),
            received_at,
            occurred_at: received_at,
        }
    }

    #[test]
    fn gaps_are_zero_and_latest_slot_is_aligned() {
        let filled = fill_buckets(
            &[
                SecondBucket {
                    bucket: 100,
                    events: 3,
                },
                SecondBucket {
                    bucket: 102,
                    events: 5,
                },
            ],
            102,
        );
        assert_eq!(filled[57], 3);
        assert_eq!(filled[58], 0);
        assert_eq!(filled[59], 5);
    }

    #[test]
    fn tail_deduplicates_overlap_and_keeps_ids_in_sync() {
        let now = OffsetDateTime::now_utc();
        let mut app = App::default();
        app.insert_tail(vec![tail(1, now), tail(2, now + Duration::seconds(1))]);
        app.insert_tail(vec![
            tail(2, now + Duration::seconds(1)),
            tail(3, now + Duration::seconds(2)),
        ]);
        assert_eq!(app.tail.len(), 3);
        assert_eq!(app.tail_ids.len(), 3);
    }

    #[test]
    fn watermark_only_moves_forward() {
        let now = OffsetDateTime::now_utc();
        let mut app = App::default();
        app.advance_watermark(&[tail(1, now)]);
        app.advance_watermark(&[]);
        app.advance_watermark(&[tail(2, now - Duration::seconds(1))]);
        assert_eq!(app.watermark, Some(now));
    }

    #[test]
    fn tail_eviction_removes_its_id() {
        let now = OffsetDateTime::now_utc();
        let mut app = App::default();
        for id in 0..=TAIL_CAPACITY as u128 {
            app.insert_tail(vec![tail(id, now + Duration::seconds(id as i64))]);
        }
        assert_eq!(app.tail.len(), TAIL_CAPACITY);
        assert_eq!(app.tail_ids.len(), TAIL_CAPACITY);
        assert!(!app.tail_ids.contains(&Uuid::from_u128(0)));
    }

    #[test]
    fn ingest_health_updates_pending_gauge_and_unknown_state() {
        let mut app = App::default();
        app.apply_ingest_health(Ok(IngestHealth {
            pending_events: 12,
            batch_capacity: 200,
        }));
        assert_eq!(app.pending_events, Some(12));
        assert_eq!(app.pending_capacity, 200);
        assert!(app.ingest_health_error.is_none());

        app.apply_ingest_health(Err("handler unavailable".into()));
        assert_eq!(app.pending_events, None);
        assert_eq!(
            app.ingest_health_error.as_deref(),
            Some("handler unavailable")
        );
    }
}
