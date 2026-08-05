use std::{
    collections::HashSet,
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use subtle::ConstantTimeEq;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::signal;
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const INSERT_TABLE: &str = "telemetry.events";
const DEFAULT_EVENT_NAMES: &str =
    "session_start,session_end,generation_requested,generation_completed,model_loaded,feature_used";

#[derive(Clone)]
struct AppState {
    clickhouse: Client,
    ingest_key: Arc<str>,
    allowed_event_names: Option<Arc<HashSet<String>>>,
    max_batch_events: usize,
    max_properties_bytes: usize,
    max_event_age_days: i64,
    max_future_skew_seconds: i64,
    trust_cloudflare_headers: bool,
    ingest_version: Arc<str>,
}

#[derive(Debug, Deserialize)]
struct IncomingEvent {
    event_id: Uuid,
    event_name: String,
    /// RFC 3339 timestamp with milliseconds, set by Planar when it emits the event.
    event_time: String,
    install_id: Uuid,
    session_id: Uuid,
    app_version: String,
    os: String,
    os_version: String,
    #[serde(default = "empty_properties")]
    properties: Value,
}

fn empty_properties() -> Value {
    Value::Object(Default::default())
}

#[derive(Debug, Serialize, Row)]
struct ClickHouseEvent {
    #[serde(with = "clickhouse::serde::uuid")]
    event_id: Uuid,
    event_name: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    event_time: OffsetDateTime,
    #[serde(with = "clickhouse::serde::uuid")]
    install_id: Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    session_id: Uuid,
    app_version: String,
    os: String,
    os_version: String,
    properties: String,
    country: String,
    ingest_version: String,
}

#[derive(Debug, Serialize)]
struct AcceptedResponse {
    accepted: usize,
}

#[derive(Debug, Deserialize, Row)]
struct HealthRow {
    value: u8,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: "a valid Bearer token is required".into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_payload",
            message: message.into(),
        }
    }

    fn unprocessable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "invalid_event",
            message: message.into(),
        }
    }

    fn too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "payload_too_large",
            message: message.into(),
        }
    }

    fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "storage_unavailable",
            message: "telemetry storage is temporarily unavailable; retry this batch".into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Keep diagnostics useful without leaking bearer tokens, payloads, IPs, or install IDs.
        warn!(status = %self.status, code = self.code, "telemetry request rejected");
        (
            self.status,
            Json(serde_json::json!({ "error": self.code, "message": self.message })),
        )
            .into_response()
    }
}

#[tokio::main]
async fn main() {
    init_logging();

    let config = Config::from_env().unwrap_or_else(|message| {
        error!(%message, "invalid configuration");
        std::process::exit(2);
    });

    let state = AppState {
        clickhouse: clickhouse_client(&config),
        ingest_key: Arc::from(config.ingest_key),
        allowed_event_names: config.allowed_event_names.map(Arc::new),
        max_batch_events: config.max_batch_events,
        max_properties_bytes: config.max_properties_bytes,
        max_event_age_days: config.max_event_age_days,
        max_future_skew_seconds: config.max_future_skew_seconds,
        trust_cloudflare_headers: config.trust_cloudflare_headers,
        ingest_version: Arc::from(config.ingest_version),
    };
    let app = router(state, config.max_body_bytes);

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .unwrap_or_else(|error| {
            error!(%error, address = %config.bind_addr, "failed to bind listener");
            std::process::exit(2);
        });
    info!(
        address = %config.bind_addr,
        database = %config.clickhouse_database,
        max_batch_events = config.max_batch_events,
        trust_cloudflare_headers = config.trust_cloudflare_headers,
        "telemetry ingest service listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

fn router(state: AppState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/events", post(ingest_events))
        .layer(RequestBodyLimitLayer::new(max_body_bytes))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        state
            .clickhouse
            .query("SELECT 1 AS value")
            .fetch_one::<HealthRow>(),
    )
    .await;

    match result {
        Ok(Ok(row)) if row.value == 1 => Ok(Json(serde_json::json!({ "ok": true }))),
        Ok(Ok(_)) => {
            error!("unexpected ClickHouse health result");
            Err(ApiError::unavailable())
        }
        Ok(Err(error)) => {
            error!(%error, "ClickHouse health query failed");
            Err(ApiError::unavailable())
        }
        Err(_) => {
            error!("ClickHouse health query timed out");
            Err(ApiError::unavailable())
        }
    }
}

async fn ingest_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<AcceptedResponse>, ApiError> {
    debug!(body_bytes = body.len(), "received telemetry request");
    authorize(&headers, &state.ingest_key)?;

    let events: Vec<IncomingEvent> = serde_json::from_slice(&body).map_err(|error| {
        ApiError::bad_request(format!("body must be a JSON event array: {error}"))
    })?;
    if events.is_empty() {
        return Err(ApiError::bad_request("event array must not be empty"));
    }
    if events.len() > state.max_batch_events {
        return Err(ApiError::too_large(format!(
            "batch contains {} events; the maximum is {}",
            events.len(),
            state.max_batch_events
        )));
    }

    // `CF-IPCountry` is only meaningful if the origin is locked to Cloudflare
    // (Authenticated Origin Pulls or a firewall allowlist). It is deliberately
    // ignored unless that deployment guarantee is made explicitly in config.
    let country = country_from_headers(&headers, &state);
    let rows = events
        .into_iter()
        .map(|event| validate_event(event, &state, &country))
        .collect::<Result<Vec<_>, _>>()?;
    let accepted = rows.len();
    let mut event_names = rows
        .iter()
        .map(|event| event.event_name.as_str())
        .collect::<Vec<_>>();
    event_names.sort_unstable();
    event_names.dedup();

    // wait_for_async_insert=1 means this only resolves after ClickHouse confirms the
    // buffered async insert. Returning 200 before then would violate the client buffer's
    // "drain only after confirmation" durability contract.
    let write_started = Instant::now();
    let mut insert = state.clickhouse.insert(INSERT_TABLE).map_err(|error| {
        error!(%error, "failed to create ClickHouse insert");
        ApiError::unavailable()
    })?;
    for row in &rows {
        insert.write(row).await.map_err(|error| {
            error!(%error, "failed to write ClickHouse insert row");
            ApiError::unavailable()
        })?;
    }
    insert.end().await.map_err(|error| {
        error!(%error, "ClickHouse did not acknowledge async insert");
        ApiError::unavailable()
    })?;

    info!(
        accepted,
        event_names = ?event_names,
        country = %country,
        clickhouse_write_ms = write_started.elapsed().as_millis(),
        "accepted telemetry batch"
    );
    Ok(Json(AcceptedResponse { accepted }))
}

fn authorize(headers: &HeaderMap, expected_key: &str) -> Result<(), ApiError> {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(ApiError::unauthorized());
    };
    let Some(provided_key) = value.strip_prefix("Bearer ") else {
        return Err(ApiError::unauthorized());
    };

    let matches = provided_key.len() == expected_key.len()
        && provided_key
            .as_bytes()
            .ct_eq(expected_key.as_bytes())
            .into();
    matches.then_some(()).ok_or_else(ApiError::unauthorized)
}

fn validate_event(
    event: IncomingEvent,
    state: &AppState,
    country: &str,
) -> Result<ClickHouseEvent, ApiError> {
    if event.event_name.is_empty() || event.event_name.len() > 128 {
        return Err(ApiError::unprocessable(
            "event_name must be between 1 and 128 bytes",
        ));
    }
    if let Some(allowed) = &state.allowed_event_names
        && !allowed.contains(&event.event_name)
    {
        return Err(ApiError::unprocessable(format!(
            "event_name '{}' is not allowed",
            event.event_name
        )));
    }
    if event.app_version.len() > 128 || event.os.len() > 64 || event.os_version.len() > 256 {
        return Err(ApiError::unprocessable(
            "envelope string exceeds its maximum length",
        ));
    }

    let event_time = OffsetDateTime::parse(&event.event_time, &Rfc3339)
        .map_err(|_| ApiError::unprocessable("event_time must be an RFC 3339 timestamp"))?;
    let now = OffsetDateTime::now_utc();
    if event_time > now + time::Duration::seconds(state.max_future_skew_seconds) {
        return Err(ApiError::unprocessable(
            "event_time is too far in the future",
        ));
    }
    if event_time < now - time::Duration::days(state.max_event_age_days) {
        return Err(ApiError::unprocessable(
            "event_time is older than the accepted offline-delivery window",
        ));
    }
    validate_properties(&event.event_name, &event.properties)?;
    let properties = serde_json::to_string(&event.properties)
        .expect("serializing serde_json::Value cannot fail");
    if properties.len() > state.max_properties_bytes {
        return Err(ApiError::too_large(format!(
            "properties is {} bytes; the maximum is {}",
            properties.len(),
            state.max_properties_bytes
        )));
    }

    Ok(ClickHouseEvent {
        event_id: event.event_id,
        event_name: event.event_name,
        event_time,
        install_id: event.install_id,
        session_id: event.session_id,
        app_version: event.app_version,
        os: event.os,
        os_version: event.os_version,
        properties,
        country: country.into(),
        ingest_version: state.ingest_version.to_string(),
    })
}

/// The flexible JSON column is intentionally bounded by a typed v1 contract.
/// This prevents an instrumentation error from silently collecting prompts,
/// paths, or arbitrary machine metadata into `properties`.
fn validate_properties(event_name: &str, properties: &Value) -> Result<(), ApiError> {
    let Some(properties) = properties.as_object() else {
        return Err(ApiError::unprocessable("properties must be a JSON object"));
    };

    match event_name {
        "session_start" => allow_only(properties, &[]),
        "session_end" => {
            allow_only(properties, &["duration_ms"])?;
            required_u64(properties, "duration_ms")
        }
        "generation_requested" => {
            allow_only(
                properties,
                &["backend", "model", "steps", "width", "height", "sampler"],
            )?;
            required_enum(properties, "backend", &["sdcpp", "diffusers"])?;
            required_string(properties, "model", 256)?;
            required_u64(properties, "steps")?;
            required_u64(properties, "width")?;
            required_u64(properties, "height")?;
            required_string(properties, "sampler", 128)
        }
        "generation_completed" => {
            // `backend` is optional for compatibility with the original event plan,
            // but lets completion dashboards split results without joining events.
            allow_only(
                properties,
                &["duration_ms", "success", "error_kind", "backend"],
            )?;
            required_u64(properties, "duration_ms")?;
            required_bool(properties, "success")?;
            optional_nullable_enum(properties, "error_kind", &["oom", "model_load", "other"])?;
            optional_enum(properties, "backend", &["sdcpp", "diffusers"])
        }
        "model_loaded" => {
            allow_only(properties, &["model", "load_ms", "size_mb"])?;
            required_string(properties, "model", 256)?;
            required_u64(properties, "load_ms")?;
            required_u64(properties, "size_mb")
        }
        "feature_used" => {
            allow_only(properties, &["feature"])?;
            required_string(properties, "feature", 128)
        }
        _ => Err(ApiError::unprocessable(
            "event_name has no approved v1 properties contract",
        )),
    }
}

fn allow_only(
    properties: &serde_json::Map<String, Value>,
    allowed: &[&str],
) -> Result<(), ApiError> {
    if let Some(key) = properties
        .keys()
        .find(|key| !allowed.contains(&key.as_str()))
    {
        return Err(ApiError::unprocessable(format!(
            "property '{key}' is not permitted for this event"
        )));
    }
    Ok(())
}

fn required_u64(properties: &serde_json::Map<String, Value>, key: &str) -> Result<(), ApiError> {
    if properties.get(key).and_then(Value::as_u64).is_none() {
        return Err(ApiError::unprocessable(format!(
            "property '{key}' must be a non-negative integer"
        )));
    }
    Ok(())
}

fn required_bool(properties: &serde_json::Map<String, Value>, key: &str) -> Result<(), ApiError> {
    if properties.get(key).and_then(Value::as_bool).is_none() {
        return Err(ApiError::unprocessable(format!(
            "property '{key}' must be a boolean"
        )));
    }
    Ok(())
}

fn required_string(
    properties: &serde_json::Map<String, Value>,
    key: &str,
    max_bytes: usize,
) -> Result<(), ApiError> {
    let Some(value) = properties.get(key).and_then(Value::as_str) else {
        return Err(ApiError::unprocessable(format!(
            "property '{key}' must be a string"
        )));
    };
    if value.is_empty() || value.len() > max_bytes {
        return Err(ApiError::unprocessable(format!(
            "property '{key}' must contain 1 to {max_bytes} bytes"
        )));
    }
    Ok(())
}

fn required_enum(
    properties: &serde_json::Map<String, Value>,
    key: &str,
    allowed: &[&str],
) -> Result<(), ApiError> {
    let Some(value) = properties.get(key).and_then(Value::as_str) else {
        return Err(ApiError::unprocessable(format!(
            "property '{key}' must be a string"
        )));
    };
    if !allowed.contains(&value) {
        return Err(ApiError::unprocessable(format!(
            "property '{key}' has an invalid value"
        )));
    }
    Ok(())
}

fn optional_enum(
    properties: &serde_json::Map<String, Value>,
    key: &str,
    allowed: &[&str],
) -> Result<(), ApiError> {
    match properties.get(key) {
        None => Ok(()),
        Some(_) => required_enum(properties, key, allowed),
    }
}

fn optional_nullable_enum(
    properties: &serde_json::Map<String, Value>,
    key: &str,
    allowed: &[&str],
) -> Result<(), ApiError> {
    if properties.get(key).is_some_and(Value::is_null) {
        return Ok(());
    }
    optional_enum(properties, key, allowed)
}

fn country_from_headers(headers: &HeaderMap, state: &AppState) -> String {
    if !state.trust_cloudflare_headers {
        return String::new();
    }
    let Some(country) = headers
        .get("cf-ipcountry")
        .and_then(|value| value.to_str().ok())
    else {
        return String::new();
    };
    let country = country.trim();
    if country.len() == 2 && country.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        country.to_ascii_uppercase()
    } else {
        String::new()
    }
}

#[derive(Debug)]
struct Config {
    bind_addr: SocketAddr,
    clickhouse_url: String,
    clickhouse_database: String,
    clickhouse_user: String,
    clickhouse_password: Option<String>,
    ingest_key: String,
    allowed_event_names: Option<HashSet<String>>,
    max_batch_events: usize,
    max_body_bytes: usize,
    max_properties_bytes: usize,
    max_event_age_days: i64,
    max_future_skew_seconds: i64,
    trust_cloudflare_headers: bool,
    ingest_version: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let ingest_key = required_env("INGEST_KEY")?;
        if ingest_key.len() < 16 {
            return Err("INGEST_KEY must be at least 16 bytes".into());
        }
        let allowed_event_names =
            env::var("ALLOWED_EVENT_NAMES").unwrap_or_else(|_| DEFAULT_EVENT_NAMES.into());
        let allowed_event_names = parse_allowlist(&allowed_event_names);

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
            ingest_key,
            allowed_event_names,
            max_batch_events: positive_env("MAX_BATCH_EVENTS", 200)?,
            max_body_bytes: positive_env("MAX_BODY_BYTES", 1_048_576)?,
            max_properties_bytes: positive_env("MAX_PROPERTIES_BYTES", 16_384)?,
            max_event_age_days: positive_i64_env("MAX_EVENT_AGE_DAYS", 190)?,
            max_future_skew_seconds: positive_i64_env("MAX_FUTURE_SKEW_SECONDS", 300)?,
            trust_cloudflare_headers: bool_env("TRUST_CLOUDFLARE_HEADERS", false)?,
            ingest_version: env_or("INGEST_VERSION", env!("CARGO_PKG_VERSION")),
        })
    }
}

fn clickhouse_client(config: &Config) -> Client {
    let client = Client::default()
        // ClickHouse async inserts buffer the raw HTTP body. Native LZ4 framing can then be
        // parsed as RowBinary when the buffered insert is flushed, corrupting the row stream.
        // Telemetry batches are small, so send uncompressed RowBinary instead.
        .with_compression(clickhouse::Compression::None)
        .with_url(&config.clickhouse_url)
        .with_database(&config.clickhouse_database)
        .with_user(&config.clickhouse_user)
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "1");
    match &config.clickhouse_password {
        Some(password) => client.with_password(password),
        None => client,
    }
}

fn parse_allowlist(value: &str) -> Option<HashSet<String>> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>();
    (!values.is_empty()).then_some(values)
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is required"))
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}

fn positive_env(name: &str, default: usize) -> Result<usize, String> {
    let value = env::var(name)
        .ok()
        .map_or(Ok(default), |value| value.parse::<usize>());
    match value {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!("{name} must be a positive integer")),
    }
}

fn positive_i64_env(name: &str, default: i64) -> Result<i64, String> {
    let value = env::var(name)
        .ok()
        .map_or(Ok(default), |value| value.parse::<i64>());
    match value {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!("{name} must be a positive integer")),
    }
}

fn bool_env(name: &str, default: bool) -> Result<bool, String> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| format!("{name} must be true or false"))
    })
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .json()
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    warn!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> AppState {
        AppState {
            clickhouse: Client::default(),
            ingest_key: Arc::from("this-is-a-test-key"),
            allowed_event_names: Some(Arc::new(["session_start".to_string()].into())),
            max_batch_events: 200,
            max_properties_bytes: 100,
            max_event_age_days: 190,
            max_future_skew_seconds: 300,
            trust_cloudflare_headers: false,
            ingest_version: Arc::from("test"),
        }
    }

    fn event() -> IncomingEvent {
        IncomingEvent {
            event_id: Uuid::nil(),
            event_name: "session_start".into(),
            event_time: OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
            install_id: Uuid::nil(),
            session_id: Uuid::nil(),
            app_version: "0.1.0".into(),
            os: "macOS".into(),
            os_version: "15.0".into(),
            properties: serde_json::json!({}),
        }
    }

    #[test]
    fn validates_and_serializes_properties() {
        let row = validate_event(event(), &state(), "AU").expect("valid event");
        assert_eq!(row.properties, "{}");
        assert_eq!(row.country, "AU");
    }

    #[test]
    fn rejects_unknown_event_name() {
        let mut event = event();
        event.event_name = "prompt_captured".into();
        let error = validate_event(event, &state(), "").expect_err("must reject unknown event");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn empty_allowlist_means_unrestricted() {
        assert_eq!(parse_allowlist(""), None);
    }

    #[test]
    fn rejects_unapproved_properties() {
        let mut event = event();
        event.properties = serde_json::json!({ "prompt": "do not collect this" });
        let error = validate_event(event, &state(), "").expect_err("must reject unknown property");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn only_uses_cloudflare_country_when_explicitly_trusted() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-ipcountry", "au".parse().unwrap());
        assert_eq!(country_from_headers(&headers, &state()), "");
        let mut trusted = state();
        trusted.trust_cloudflare_headers = true;
        assert_eq!(country_from_headers(&headers, &trusted), "AU");
    }
}
