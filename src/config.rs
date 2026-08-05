use std::{env, net::SocketAddr};

pub struct Limits {
    pub max_attributes_bytes: usize,
    pub max_event_age_days: i64,
    pub max_future_skew_seconds: i64,
}

pub struct Config {
    pub bind_addr: SocketAddr,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: Option<String>,
    pub ingest_keys: String,
    pub max_batch_events: usize,
    pub max_body_bytes: usize,
    pub limits: Limits,
    pub trust_cloudflare_headers: bool,
    pub ingest_version: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            bind_addr: env_or("BIND_ADDR", "127.0.0.1:8081")
                .parse()
                .map_err(|_| "BIND_ADDR must be an IP address and port".to_string())?,
            clickhouse_url: env_or("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "telemetry"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "default"),
            clickhouse_password: env::var("CLICKHOUSE_PASSWORD")
                .ok()
                .filter(|value| !value.is_empty()),
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
