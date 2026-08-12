use std::{env, fs, net::SocketAddr, path::Path};

use clickhouse::Client;
use serde::Deserialize;

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
    transport_compression: TransportCompression,
}

impl ClickhouseConfig {
    pub fn from_env() -> Result<Self, String> {
        let file_config = FileConfig::from_path(Path::new(&config_path()))?;
        Ok(Self {
            url: env_or("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
            database: env_or("CLICKHOUSE_DATABASE", "telemetry"),
            user: env_or("CLICKHOUSE_USER", "default"),
            password: env::var("CLICKHOUSE_PASSWORD")
                .ok()
                .filter(|value| !value.is_empty()),
            transport_compression: file_config.clickhouse.transport_compression,
        })
    }

    pub fn client(&self) -> Client {
        let compression = match self.transport_compression {
            TransportCompression::Lz4 => clickhouse::Compression::Lz4,
            TransportCompression::None => clickhouse::Compression::None,
        };
        let client = Client::default()
            // For LZ4, the driver adds `decompress=1`, so ClickHouse decompresses each native
            // LZ4 block before it buffers and later parses the RowBinary stream for async insert.
            .with_compression(compression)
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
            clickhouse: ClickhouseConfig::from_env()?,
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

pub fn manifest_dir() -> String {
    env_or("MANIFEST_DIR", "tenants")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    clickhouse: ClickhouseFileConfig,
}

impl FileConfig {
    fn from_path(path: &Path) -> Result<Self, String> {
        let contents = fs::read_to_string(path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        serde_json::from_str(&contents)
            .map_err(|error| format!("could not parse {}: {error}", path.display()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClickhouseFileConfig {
    transport_compression: TransportCompression,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TransportCompression {
    Lz4,
    None,
}

fn config_path() -> String {
    env_or("CONFIG_PATH", "config.json")
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

#[cfg(test)]
mod tests {
    use super::{FileConfig, TransportCompression};

    #[test]
    fn parses_clickhouse_transport_compression() {
        let config: FileConfig =
            serde_json::from_str(r#"{"clickhouse":{"transport_compression":"lz4"}}"#).unwrap();
        let disabled: FileConfig =
            serde_json::from_str(r#"{"clickhouse":{"transport_compression":"none"}}"#).unwrap();

        assert_eq!(
            config.clickhouse.transport_compression,
            TransportCompression::Lz4
        );
        assert_eq!(
            disabled.clickhouse.transport_compression,
            TransportCompression::None
        );
    }

    #[test]
    fn rejects_unknown_file_settings() {
        let error = serde_json::from_str::<FileConfig>(
            r#"{"clickhouse":{"transport_compression":"lz4","unexpected":true}}"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unexpected"));
    }
}
