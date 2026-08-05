use std::{env, net::SocketAddr};

use clickhouse::Client;

pub struct Limits {
    pub max_attributes_bytes: usize,
    pub max_event_age_days: i64,
    pub max_future_skew_seconds: i64,
}

pub struct Config {
    pub bind_addr: SocketAddr,
    pub clickhouse: ClickhouseConfig,
    pub ingest_keys: String,
    pub max_batch_events: usize,
    pub max_body_bytes: usize,
    pub limits: Limits,
    pub trust_cloudflare_headers: bool,
    pub ingest_version: String,
}

pub struct ClickhouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
}

impl ClickhouseConfig {
    pub fn from_env() -> Self {
        Self {
            url: env_or("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
            database: env_or("CLICKHOUSE_DATABASE", "telemetry"),
            user: env_or("CLICKHOUSE_USER", "default"),
            password: env::var("CLICKHOUSE_PASSWORD")
                .ok()
                .filter(|value| !value.is_empty()),
        }
    }

    pub fn client(&self) -> Client {
        let client = Client::default()
            // ClickHouse async inserts buffer the raw HTTP body. Native LZ4 framing can then be
            // parsed as RowBinary when the buffered insert is flushed, corrupting the row stream.
            .with_compression(clickhouse::Compression::None)
            .with_url(&self.url)
            .with_database(&self.database)
            .with_user(&self.user)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1");
        match &self.password {
            Some(password) => client.with_password(password),
            None => client,
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            bind_addr: env_or("BIND_ADDR", "127.0.0.1:8081")
                .parse()
                .map_err(|_| "BIND_ADDR must be an IP address and port".to_string())?,
            clickhouse: ClickhouseConfig::from_env(),
            ingest_keys: required_env("INGEST_KEYS")?,
            max_batch_events: positive_env("MAX_BATCH_EVENTS", 200)?,
            max_body_bytes: positive_env("MAX_BODY_BYTES", 1_048_576)?,
            limits: Limits {
                max_attributes_bytes: positive_env("MAX_ATTRIBUTES_BYTES", 16_384)?,
                max_event_age_days: positive_i64_env("MAX_EVENT_AGE_DAYS", 190)?,
                max_future_skew_seconds: positive_i64_env("MAX_FUTURE_SKEW_SECONDS", 300)?,
            },
            trust_cloudflare_headers: bool_env("TRUST_CLOUDFLARE_HEADERS", false)?,
            ingest_version: env_or("INGEST_VERSION", env!("CARGO_PKG_VERSION")),
        })
    }
}

/// Reporting timezone for the dashboard's calendar-day boundaries. This is a property of the
/// project, not of whoever is looking: the same window must mean the same span whether the
/// dashboard runs on a laptop or via `docker compose exec` in the handler container. UTC by
/// default, matching how `occurred_at` is stored and how the table is partitioned.
pub fn dashboard_timezone() -> String {
    env_or("DASHBOARD_TIMEZONE", "UTC")
}

pub fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is required"))
}
pub fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}
pub fn positive_env(name: &str, default: usize) -> Result<usize, String> {
    env::var(name)
        .ok()
        .map_or(Ok(default), |value| value.parse::<usize>())
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}
pub fn positive_i64_env(name: &str, default: i64) -> Result<i64, String> {
    env::var(name)
        .ok()
        .map_or(Ok(default), |value| value.parse::<i64>())
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}
pub fn bool_env(name: &str, default: bool) -> Result<bool, String> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| format!("{name} must be true or false"))
    })
}
