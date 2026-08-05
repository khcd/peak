use clickhouse::{Client, Row};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::contract::ProducerSpec;

const EVENTS: &str = "telemetry.events";

/// How long a client may go without a `live_ping` before the dashboard treats it as offline.
///
/// The planar client pings every 5 minutes and flushes immediately, bypassing its normal batching,
/// so a healthy ping arrives promptly. The threshold must still be comfortably larger than the
/// ping interval: at exactly 5 minutes a perfectly healthy client sits on the boundary and
/// flickers between connected and offline on ordinary jitter. Two intervals plus a minute of
/// slack absorbs one dropped ping or a delayed flush, and still spots a real disconnect quickly.
pub const OFFLINE_AFTER_MINUTES: u32 = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    D1,
    D7,
    D30,
}

impl Window {
    /// Whole calendar days the window covers, counting today.
    pub fn days(self) -> u32 {
        match self {
            Self::D1 => 1,
            Self::D7 => 7,
            Self::D30 => 30,
        }
    }

    /// Lower bound of the window: midnight at the start of its first day. Day-aligned everywhere,
    /// so the totals and the per-window breakdowns always cover exactly the same span. `now()` and
    /// `toStartOfDay` resolve in the session timezone set by the dashboard client.
    pub fn since(self) -> String {
        match self.days() {
            1 => "toStartOfDay(now())".to_owned(),
            days => format!("toStartOfDay(now()) - INTERVAL {} DAY", days - 1),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::D1 => "today",
            Self::D7 => "7d",
            Self::D30 => "30d",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Row)]
pub struct SecondBucket {
    pub bucket: u32,
    pub events: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
pub struct TailRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub event_id: Uuid,
    pub event_name: String,
    pub subject_id: String,
    pub platform: String,
    pub service_version: String,
    pub country: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub received_at: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    #[allow(dead_code)] // Retained for the tail row shape and future detail views.
    pub occurred_at: OffsetDateTime,
}

#[derive(Debug, Clone, Default, Deserialize, Row)]
pub struct Connected {
    pub connected: u64,
}

#[derive(Debug, Clone, Default, Deserialize, Row)]
pub struct Totals {
    pub events_today: u64,
    pub events_7d: u64,
    pub events_30d: u64,
    pub installs_today: u64,
    pub installs_7d: u64,
    pub installs_30d: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
pub struct CountRow {
    pub event_name: String,
    pub events: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
pub struct FleetRow {
    pub dimension: String,
    pub value: String,
    pub events: u64,
}

pub async fn fast(
    client: &Client,
    producer: &ProducerSpec,
    watermark: Option<OffsetDateTime>,
) -> clickhouse::error::Result<(Vec<SecondBucket>, Vec<TailRow>, u64)> {
    let buckets = client
        .query(live_chart_sql())
        .bind(producer.name)
        .fetch_all::<SecondBucket>()
        .await?;
    let connected = client
        .query(&connected_sql())
        .bind(producer.name)
        .fetch_one::<Connected>()
        .await?
        .connected;
    let tail = match watermark {
        Some(watermark) => {
            // OffsetDateTime's derived Serialize emits a 9-element tuple, which ClickHouse cannot
            // compare against DateTime64. Bind epoch millis and rebuild the timestamp in SQL.
            let since = watermark - time::Duration::seconds(2);
            let since_millis = (since.unix_timestamp_nanos() / 1_000_000) as i64;
            client
                .query(tail_since_sql())
                .bind(producer.name)
                .bind(since_millis)
                .fetch_all::<TailRow>()
                .await?
        }
        None => {
            client
                .query(tail_initial_sql())
                .bind(producer.name)
                .fetch_all::<TailRow>()
                .await?
        }
    };
    Ok((buckets, tail, connected))
}

pub async fn slow(
    client: &Client,
    producer: &ProducerSpec,
    window: Window,
) -> clickhouse::error::Result<(Totals, Vec<CountRow>, Vec<FleetRow>)> {
    let totals = client
        .query(totals_sql())
        .bind(producer.name)
        .fetch_one::<Totals>()
        .await?;
    let breakdown = client
        .query(&breakdown_sql(window))
        .bind(producer.name)
        .fetch_all::<CountRow>()
        .await?;
    let fleet = client
        .query(&fleet_sql(window))
        .bind(producer.name)
        .bind(producer.name)
        .bind(producer.name)
        .fetch_all::<FleetRow>()
        .await?;
    Ok((totals, breakdown, fleet))
}

pub fn live_chart_sql() -> &'static str {
    "SELECT toUInt32(toUnixTimestamp(toStartOfSecond(received_at))) AS bucket, \
            toUInt64(uniqExact(event_id)) AS events \
     FROM telemetry.events \
     WHERE producer = ? AND received_at >= now() - INTERVAL 60 SECOND \
     GROUP BY bucket"
}

/// Installs that have pinged recently enough to count as online.
///
/// Measured on `received_at`, not `occurred_at`: liveness is the server-side fact "we heard from
/// this install", so a device whose wall clock is wrong is still counted correctly. `producer` and
/// `event_name` are the first two columns of the table's sort key, so this filter hits the primary
/// index directly. Counts distinct installs -- two windows of the app on one machine count once.
pub fn connected_sql() -> String {
    format!(
        "SELECT toUInt64(uniqExact(subject_id)) AS connected FROM {EVENTS} \
         WHERE producer = ? AND event_name = 'live_ping' \
           AND received_at >= now() - INTERVAL {OFFLINE_AFTER_MINUTES} MINUTE"
    )
}

pub fn tail_initial_sql() -> &'static str {
    "SELECT event_id, event_name, subject_id, platform, service_version, country, received_at, occurred_at \
     FROM telemetry.events WHERE producer = ? AND received_at >= now() - INTERVAL 5 MINUTE \
     ORDER BY received_at DESC LIMIT 50"
}

pub fn tail_since_sql() -> &'static str {
    "SELECT event_id, event_name, subject_id, platform, service_version, country, received_at, occurred_at \
     FROM telemetry.events WHERE producer = ? AND received_at >= fromUnixTimestamp64Milli(?, 'UTC') \
     ORDER BY received_at DESC LIMIT 50"
}

pub fn totals_sql() -> &'static str {
    "SELECT \
       toUInt64(uniqExactIf(event_id, occurred_at >= toStartOfDay(now()))) AS events_today, \
       toUInt64(uniqExactIf(event_id, occurred_at >= toStartOfDay(now()) - INTERVAL 6 DAY)) AS events_7d, \
       toUInt64(uniqExactIf(event_id, occurred_at >= toStartOfDay(now()) - INTERVAL 29 DAY)) AS events_30d, \
       toUInt64(uniqExactIf(subject_id, occurred_at >= toStartOfDay(now()))) AS installs_today, \
       toUInt64(uniqExactIf(subject_id, occurred_at >= toStartOfDay(now()) - INTERVAL 6 DAY)) AS installs_7d, \
       toUInt64(uniqExactIf(subject_id, occurred_at >= toStartOfDay(now()) - INTERVAL 29 DAY)) AS installs_30d \
     FROM telemetry.events WHERE producer = ? AND occurred_at >= toStartOfDay(now()) - INTERVAL 29 DAY"
}

pub fn breakdown_sql(window: Window) -> String {
    format!(
        "SELECT event_name, toUInt64(uniqExact(event_id)) AS events FROM {EVENTS} \
         WHERE producer = ? AND occurred_at >= {} \
         GROUP BY event_name ORDER BY events DESC LIMIT 12",
        window.since()
    )
}

pub fn fleet_sql(window: Window) -> String {
    format!(
        "SELECT dimension, value, toUInt64(uniqExact(event_id)) AS events FROM ( \
           SELECT event_id, 'platform' AS dimension, platform AS value FROM {EVENTS} WHERE producer = ? AND occurred_at >= {since} \
           UNION ALL SELECT event_id, 'version', service_version FROM {EVENTS} WHERE producer = ? AND occurred_at >= {since} \
           UNION ALL SELECT event_id, 'country', country FROM {EVENTS} WHERE producer = ? AND occurred_at >= {since} \
         ) WHERE value != '' GROUP BY dimension, value ORDER BY dimension, events DESC",
        since = window.since(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_are_day_aligned_and_count_today() {
        // A window of N days starts at midnight N-1 days ago, so it covers N whole days
        // including today rather than N days plus the elapsed part of today.
        assert_eq!(Window::D1.since(), "toStartOfDay(now())");
        assert_eq!(Window::D7.since(), "toStartOfDay(now()) - INTERVAL 6 DAY");
        assert_eq!(Window::D30.since(), "toStartOfDay(now()) - INTERVAL 29 DAY");
    }

    #[test]
    fn no_window_uses_a_rolling_interval_from_now() {
        for window in [Window::D1, Window::D7, Window::D30] {
            assert!(!breakdown_sql(window).contains("now() - INTERVAL"));
            assert!(!fleet_sql(window).contains("now() - INTERVAL"));
        }
        assert!(!totals_sql().contains("now() - INTERVAL"));
    }

    #[test]
    fn offline_threshold_clears_the_client_ping_interval() {
        // The planar client pings every 5 minutes. A threshold at or below that would make a
        // healthy client flicker in and out of the connected count on ordinary jitter.
        const CLIENT_PING_INTERVAL_MINUTES: u32 = 5;
        assert!(OFFLINE_AFTER_MINUTES > CLIENT_PING_INTERVAL_MINUTES * 2);
    }

    #[test]
    fn connected_counts_distinct_installs_on_received_at() {
        let sql = connected_sql();
        assert!(sql.contains("uniqExact(subject_id)"));
        assert!(sql.contains("event_name = 'live_ping'"));
        // Liveness must not depend on the client's wall clock.
        assert!(sql.contains("received_at >= now() - INTERVAL 11 MINUTE"));
        assert!(!sql.contains("occurred_at"));
    }

    #[test]
    fn sql_uses_bound_producers_and_windows() {
        assert!(live_chart_sql().contains("producer = ?"));
        assert!(tail_since_sql().contains("received_at >= fromUnixTimestamp64Milli(?, 'UTC')"));
        assert!(breakdown_sql(Window::D7).contains("toStartOfDay(now()) - INTERVAL 6 DAY"));
        assert_eq!(fleet_sql(Window::D30).matches("producer = ?").count(), 3);
    }
}
