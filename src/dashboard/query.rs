use clickhouse::{Client, Row};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::manifest::Tenant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    D1,
    D7,
    D30,
}

impl Window {
    pub fn index(self) -> usize {
        match self {
            Self::D1 => 0,
            Self::D7 => 1,
            Self::D30 => 2,
        }
    }

    /// Lower bound of the window: midnight at the start of its first day. Day-aligned everywhere,
    /// so the totals and the per-window breakdowns always cover exactly the same span. `now()` and
    /// `toStartOfDay` resolve in the session timezone set by the dashboard client.
    pub fn since(self, tenant: &Tenant) -> String {
        match tenant.dashboard.windows[self.index()] {
            1 => "toStartOfDay(now())".to_owned(),
            days => format!("toStartOfDay(now()) - INTERVAL {} DAY", days - 1),
        }
    }

    pub fn label(self, tenant: &Tenant) -> String {
        match tenant.dashboard.windows[self.index()] {
            1 => "today".into(),
            days => format!("{days}d"),
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
    pub subjects_today: u64,
    pub subjects_7d: u64,
    pub subjects_30d: u64,
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
    tenant: &Tenant,
    watermark: Option<OffsetDateTime>,
) -> clickhouse::error::Result<(Vec<SecondBucket>, Vec<TailRow>, Option<u64>)> {
    let buckets = client
        .query(live_chart_sql())
        .bind(&tenant.name)
        .fetch_all::<SecondBucket>()
        .await?;
    let connected = match connected_sql(tenant) {
        Some(sql) => Some(
            client
                .query(&sql)
                .bind(&tenant.name)
                .bind(&tenant.dashboard.liveness.as_ref().unwrap().event_name)
                .fetch_one::<Connected>()
                .await?
                .connected,
        ),
        None => None,
    };
    let tail = match watermark {
        Some(watermark) => {
            // OffsetDateTime's derived Serialize emits a 9-element tuple, which ClickHouse cannot
            // compare against DateTime64. Bind epoch millis and rebuild the timestamp in SQL.
            let since = watermark - time::Duration::seconds(2);
            let since_millis = (since.unix_timestamp_nanos() / 1_000_000) as i64;
            client
                .query(tail_since_sql())
                .bind(&tenant.name)
                .bind(since_millis)
                .fetch_all::<TailRow>()
                .await?
        }
        None => {
            client
                .query(tail_initial_sql())
                .bind(&tenant.name)
                .fetch_all::<TailRow>()
                .await?
        }
    };
    Ok((buckets, tail, connected))
}

pub async fn slow(
    client: &Client,
    tenant: &Tenant,
    window: Window,
) -> clickhouse::error::Result<(Totals, Vec<CountRow>, Vec<FleetRow>)> {
    let totals = client
        .query(&totals_sql(tenant))
        .bind(&tenant.name)
        .fetch_one::<Totals>()
        .await?;
    let breakdown = client
        .query(&breakdown_sql(window, tenant))
        .bind(&tenant.name)
        .fetch_all::<CountRow>()
        .await?;
    let mut fleet_query = client.query(&fleet_sql(window, tenant));
    for _ in &tenant.dashboard.fleet_dimensions {
        fleet_query = fleet_query.bind(&tenant.name);
    }
    let fleet = fleet_query.fetch_all::<FleetRow>().await?;
    Ok((totals, breakdown, fleet))
}

pub fn live_chart_sql() -> &'static str {
    "SELECT toUInt32(toUnixTimestamp(toStartOfSecond(received_at))) AS bucket, \
            toUInt64(uniqExact(event_id)) AS events \
     FROM events \
     WHERE producer = ? AND received_at >= now() - INTERVAL 60 SECOND \
     GROUP BY bucket"
}

/// Installs that have pinged recently enough to count as online.
///
/// Measured on `received_at`, not `occurred_at`: liveness is the server-side fact "we heard from
/// this install", so a device whose wall clock is wrong is still counted correctly. `producer` and
/// `event_name` are the first two columns of the table's sort key, so this filter hits the primary
/// index directly. Counts distinct installs -- two windows of the app on one machine count once.
pub fn connected_sql(tenant: &Tenant) -> Option<String> {
    tenant.dashboard.offline_after_minutes().map(|minutes| {
        format!(
            "SELECT toUInt64(uniqExact(subject_id)) AS connected FROM events \
         WHERE producer = ? AND event_name = ? \
           AND received_at >= now() - INTERVAL {minutes} MINUTE"
        )
    })
}

pub fn tail_initial_sql() -> &'static str {
    "SELECT event_id, event_name, subject_id, platform, service_version, country, received_at, occurred_at \
     FROM events WHERE producer = ? AND received_at >= now() - INTERVAL 5 MINUTE \
     ORDER BY received_at DESC LIMIT 50"
}

pub fn tail_since_sql() -> &'static str {
    "SELECT event_id, event_name, subject_id, platform, service_version, country, received_at, occurred_at \
     FROM events WHERE producer = ? AND received_at >= fromUnixTimestamp64Milli(?, 'UTC') \
     ORDER BY received_at DESC LIMIT 50"
}

pub fn totals_sql(tenant: &Tenant) -> String {
    let starts = tenant.dashboard.windows.map(|days| {
        if days == 1 {
            "toStartOfDay(now())".to_owned()
        } else {
            format!("toStartOfDay(now()) - INTERVAL {} DAY", days - 1)
        }
    });
    format!(
        "SELECT toUInt64(uniqExactIf(event_id, occurred_at >= {a})) AS events_today, toUInt64(uniqExactIf(event_id, occurred_at >= {b})) AS events_7d, toUInt64(uniqExactIf(event_id, occurred_at >= {c})) AS events_30d, toUInt64(uniqExactIf(subject_id, occurred_at >= {a})) AS subjects_today, toUInt64(uniqExactIf(subject_id, occurred_at >= {b})) AS subjects_7d, toUInt64(uniqExactIf(subject_id, occurred_at >= {c})) AS subjects_30d FROM events WHERE producer = ? AND occurred_at >= {c}",
        a = starts[0],
        b = starts[1],
        c = starts[2]
    )
}

pub fn breakdown_sql(window: Window, tenant: &Tenant) -> String {
    format!(
        "SELECT event_name, toUInt64(uniqExact(event_id)) AS events FROM events \
         WHERE producer = ? AND occurred_at >= {} \
         GROUP BY event_name ORDER BY events DESC LIMIT 12",
        window.since(tenant)
    )
}

pub fn fleet_sql(window: Window, tenant: &Tenant) -> String {
    let since = window.since(tenant);
    let parts = tenant.dashboard.fleet_dimensions.iter().map(|dimension| format!("SELECT event_id, '{}' AS dimension, {} AS value FROM events WHERE producer = ? AND occurred_at >= {since}", dimension.name(), dimension.column())).collect::<Vec<_>>();
    if parts.is_empty() {
        "SELECT '' AS dimension, '' AS value, toUInt64(0) AS events WHERE false".into()
    } else {
        format!(
            "SELECT dimension, value, toUInt64(uniqExact(event_id)) AS events FROM ({}) WHERE value != '' GROUP BY dimension, value ORDER BY dimension, events DESC",
            parts.join(" UNION ALL ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Registry;
    use std::path::Path;
    fn tenant() -> &'static Tenant {
        Box::leak(Box::new(Registry::load(Path::new("tenants")).unwrap()))
            .first()
            .unwrap()
    }

    #[test]
    fn windows_are_day_aligned_and_count_today() {
        // A window of N days starts at midnight N-1 days ago, so it covers N whole days
        // including today rather than N days plus the elapsed part of today.
        assert_eq!(Window::D1.since(tenant()), "toStartOfDay(now())");
        assert_eq!(
            Window::D7.since(tenant()),
            "toStartOfDay(now()) - INTERVAL 6 DAY"
        );
        assert_eq!(
            Window::D30.since(tenant()),
            "toStartOfDay(now()) - INTERVAL 29 DAY"
        );
    }

    #[test]
    fn no_window_uses_a_rolling_interval_from_now() {
        for window in [Window::D1, Window::D7, Window::D30] {
            assert!(!breakdown_sql(window, tenant()).contains("now() - INTERVAL"));
            assert!(!fleet_sql(window, tenant()).contains("now() - INTERVAL"));
        }
        assert!(!totals_sql(tenant()).contains("now() - INTERVAL"));
    }

    #[test]
    fn connected_counts_distinct_installs_on_received_at() {
        let tenant = tenant();
        let sql = connected_sql(tenant).unwrap();
        assert!(sql.contains("uniqExact(subject_id)"));
        assert!(sql.contains("event_name = ?"));
        // Liveness must not depend on the client's wall clock.
        let interval = format!(
            "received_at >= now() - INTERVAL {} MINUTE",
            tenant.dashboard.offline_after_minutes().unwrap()
        );
        assert!(sql.contains(&interval));
        assert!(!sql.contains("occurred_at"));
    }

    #[test]
    fn sql_uses_bound_producers_and_windows() {
        assert!(live_chart_sql().contains("producer = ?"));
        assert!(tail_since_sql().contains("received_at >= fromUnixTimestamp64Milli(?, 'UTC')"));
        assert!(
            breakdown_sql(Window::D7, tenant()).contains("toStartOfDay(now()) - INTERVAL 6 DAY")
        );
        assert_eq!(
            fleet_sql(Window::D30, tenant())
                .matches("producer = ?")
                .count(),
            3
        );
    }
}
