mod auth;
mod cli;
mod config;
mod contract;
mod dashboard;
mod error;
mod event;
mod manifest;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::HeaderMap,
    routing::{get, post},
};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::signal;
use tower_http::{
    decompression::RequestDecompressionLayer, limit::RequestBodyLimitLayer, trace::TraceLayer,
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::{
    auth::ProducerRegistry,
    config::{Config, Limits},
    error::{ApiError, EventRejection},
    event::{EventRow, IncomingEvent},
};

const INSERT_TABLE: &str = "events";

#[derive(Clone)]
struct AppState {
    clickhouse: Client,
    producers: Arc<ProducerRegistry>,
    limits: Arc<Limits>,
    max_batch_events: usize,
    trust_cloudflare_headers: bool,
    ingest_version: Arc<str>,
}

#[derive(Debug, Serialize)]
struct AcceptedResponse {
    accepted: usize,
    rejected: Vec<EventRejection>,
}

#[derive(Debug, Deserialize, Row)]
struct HealthRow {
    value: u8,
}

#[tokio::main]
async fn main() {
    let registry = Box::leak(Box::new(
        manifest::Registry::load(std::path::Path::new(&config::manifest_dir())).unwrap_or_else(
            |message| {
                eprintln!("invalid tenant manifests: {message}");
                std::process::exit(2);
            },
        ),
    ));
    match cli::Mode::from_args(registry).unwrap_or_else(|message| {
        eprintln!("{message}");
        std::process::exit(2);
    }) {
        cli::Mode::Serve => serve(registry).await,
        cli::Mode::Dashboard { tenant } => dashboard::run(registry, tenant).await,
    }
}

async fn serve(registry: &'static manifest::Registry) {
    init_logging();
    let config = Config::from_env().unwrap_or_else(|message| {
        error!(%message, "invalid configuration");
        std::process::exit(2);
    });
    let producers =
        ProducerRegistry::from_pairs(&config.ingest_keys, registry).unwrap_or_else(|message| {
            error!(%message, "invalid ingest-key registry");
            std::process::exit(2);
        });
    let state = AppState {
        clickhouse: config.clickhouse.client(),
        producers: Arc::new(producers),
        limits: Arc::new(config.limits),
        max_batch_events: config.max_batch_events,
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
    info!(address = %config.bind_addr, database = %config.clickhouse.database, max_batch_events = config.max_batch_events, trust_cloudflare_headers = config.trust_cloudflare_headers, "telemetry ingest service listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

fn router(state: AppState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route(
            "/v2/events",
            post(ingest_events)
                // This limit is inside decompression, so it caps decoded bytes as well as
                // guarding the JSON extractor from a compressed-body expansion bomb.
                .layer::<_, std::convert::Infallible>(RequestBodyLimitLayer::new(max_body_bytes))
                .layer::<_, std::convert::Infallible>(
                    RequestDecompressionLayer::new().gzip(true).zstd(true),
                ),
        )
        // Keep the existing limit outside decompression too: compressed bytes are bounded before
        // the decoder runs, while the route-local limit above bounds the decompressed body.
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
    let producer = state.producers.authenticate(&headers)?;
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
    let country = country_from_headers(&headers, &state);
    let mut rows = Vec::with_capacity(events.len());
    let mut rejected = Vec::new();
    for (index, event) in events.into_iter().enumerate() {
        match event.validate(producer, &state.limits, &country, &state.ingest_version) {
            Ok(row) => rows.push(row),
            Err(error) => rejected.push(EventRejection::new(index, error)),
        }
    }
    let accepted = rows.len();
    if !rows.is_empty() {
        insert_rows(&state, &rows).await?;
    }
    let mut event_names = rows
        .iter()
        .map(|event| event.event_name.as_str())
        .collect::<Vec<_>>();
    event_names.sort_unstable();
    event_names.dedup();
    info!(producer = producer.name, accepted, rejected = rejected.len(), event_names = ?event_names, country = %country, "processed telemetry batch");
    Ok(Json(AcceptedResponse { accepted, rejected }))
}

async fn insert_rows(state: &AppState, rows: &[EventRow]) -> Result<(), ApiError> {
    // wait_for_async_insert=1 means this only resolves after ClickHouse confirms the buffered
    // async insert. Returning 200 before then would violate the durability contract.
    let write_started = Instant::now();
    let mut insert = state.clickhouse.insert(INSERT_TABLE).map_err(|error| {
        error!(%error, "failed to create ClickHouse insert");
        ApiError::unavailable()
    })?;
    for row in rows {
        insert.write(row).await.map_err(|error| {
            error!(%error, "failed to write ClickHouse insert row");
            ApiError::unavailable()
        })?;
    }
    insert.end().await.map_err(|error| {
        error!(%error, "ClickHouse did not acknowledge async insert");
        ApiError::unavailable()
    })?;
    debug!(
        accepted = rows.len(),
        clickhouse_write_ms = write_started.elapsed().as_millis(),
        "ClickHouse accepted telemetry rows"
    );
    Ok(())
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
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {}, }
    warn!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        auth::ProducerRegistry,
        event::{IncomingResource, IncomingSubject},
        manifest::{Registry, Tenant},
    };
    use axum::http::Request;
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};
    use tower::ServiceExt;
    use uuid::Uuid;

    fn state() -> AppState {
        AppState {
            clickhouse: Client::default(),
            producers: Arc::new(
                ProducerRegistry::from_pairs("planar:this-is-a-test-key", registry()).unwrap(),
            ),
            limits: Arc::new(Limits {
                max_attributes_bytes: 100,
                max_event_age_days: 190,
                max_future_skew_seconds: 300,
            }),
            max_batch_events: 200,
            trust_cloudflare_headers: false,
            ingest_version: Arc::from("test"),
        }
    }
    fn event() -> IncomingEvent {
        IncomingEvent {
            event_id: Uuid::nil(),
            event_name: "session_start".into(),
            schema_version: 1,
            occurred_at: OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
            subject: IncomingSubject {
                kind: "install".into(),
                id: Uuid::nil().to_string(),
            },
            session_id: Some("session".into()),
            resource: IncomingResource {
                service_name: "planar".into(),
                service_version: "0.1.0".into(),
                platform: Some("macOS".into()),
                platform_version: Some("15.0".into()),
            },
            attributes: serde_json::json!({}),
        }
    }
    fn registry() -> &'static Registry {
        Box::leak(Box::new(
            Registry::load(std::path::Path::new("tenants")).unwrap(),
        ))
    }
    fn producer() -> &'static Tenant {
        registry().get("planar").unwrap()
    }
    #[test]
    fn valid_event_round_trips_to_row() {
        let row = event()
            .validate(producer(), &state().limits, "AU", "test")
            .unwrap();
        assert_eq!(row.attributes, "{}");
        assert_eq!(row.country, "AU");
    }
    #[test]
    fn rejects_unknown_contracts() {
        let mut unknown_name = event();
        unknown_name.event_name = "unknown".into();
        assert_eq!(
            unknown_name
                .validate(producer(), &state().limits, "", "test")
                .unwrap_err()
                .code,
            "unknown_contract"
        );
        let mut unknown_version = event();
        unknown_version.schema_version = 2;
        assert_eq!(
            unknown_version
                .validate(producer(), &state().limits, "", "test")
                .unwrap_err()
                .code,
            "unknown_contract"
        );
    }
    #[test]
    fn rejects_undeclared_and_nested_attributes() {
        let mut undeclared = event();
        undeclared.attributes = serde_json::json!({ "prompt": "do not collect this" });
        assert_eq!(
            undeclared
                .validate(producer(), &state().limits, "", "test")
                .unwrap_err()
                .code,
            "invalid_attributes"
        );
        let mut nested = event();
        nested.event_name = "session_end".into();
        nested.attributes = serde_json::json!({ "duration_ms": { "nested": true } });
        assert_eq!(
            nested
                .validate(producer(), &state().limits, "", "test")
                .unwrap_err()
                .code,
            "invalid_attributes"
        );
    }
    #[test]
    fn validates_subject_kind_and_uuid() {
        let mut unknown_kind = event();
        unknown_kind.subject.kind = "user".into();
        assert_eq!(
            unknown_kind
                .validate(producer(), &state().limits, "", "test")
                .unwrap_err()
                .code,
            "invalid_envelope"
        );
        let mut invalid_uuid = event();
        invalid_uuid.subject.id = "not-a-uuid".into();
        assert_eq!(
            invalid_uuid
                .validate(producer(), &state().limits, "", "test")
                .unwrap_err()
                .code,
            "invalid_envelope"
        );
    }
    #[test]
    fn rejects_unknown_envelope_fields() {
        let json = serde_json::json!({ "event_id": Uuid::nil(), "event_name": "session_start", "schema_version": 1, "occurred_at": "2026-08-05T12:34:56Z", "occurredAt": "bad", "subject": { "kind": "install", "id": Uuid::nil() }, "resource": { "service_name": "planar", "service_version": "0.1.0" }, "attributes": {} });
        assert!(serde_json::from_value::<IncomingEvent>(json).is_err());
    }
    #[tokio::test]
    async fn gzip_request_is_decompressed_before_json_parsing() {
        let body = gzip(b"not json");
        let request = Request::builder()
            .method("POST")
            .uri("/v2/events")
            .header("authorization", "Bearer this-is-a-test-key")
            .header("content-encoding", "gzip")
            .body(axum::body::Body::from(body))
            .unwrap();

        let response = router(state(), 1024).oneshot(request).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }
    #[tokio::test]
    async fn zstd_request_is_decompressed_before_json_parsing() {
        let body = zstd::stream::encode_all(b"not json".as_slice(), 0).unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/v2/events")
            .header("authorization", "Bearer this-is-a-test-key")
            .header("content-encoding", "zstd")
            .body(axum::body::Body::from(body))
            .unwrap();

        let response = router(state(), 1024).oneshot(request).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }
    #[tokio::test]
    async fn decompressed_request_body_is_limited_after_decoding() {
        let body = gzip(&vec![b' '; 512]);
        let request = Request::builder()
            .method("POST")
            .uri("/v2/events")
            .header("authorization", "Bearer this-is-a-test-key")
            .header("content-encoding", "gzip")
            .body(axum::body::Body::from(body))
            .unwrap();

        let response = router(state(), 128).oneshot(request).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
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

    fn gzip(body: &[u8]) -> Vec<u8> {
        use std::io::Write;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }
}
