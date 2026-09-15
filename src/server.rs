//! Axum router and standalone server lifecycle for telemetry ingestion.

use std::{sync::Arc, time::Duration};

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

use crate::{
    auth::ProducerRegistry,
    batcher::BatchWriter,
    config::{Config, Limits},
    error::{ApiError, EventRejection},
    event::IncomingEvent,
    manifest::Registry,
};

/// Shared state required by the ingest and health endpoints.
#[derive(Clone)]
pub struct AppState {
    clickhouse: Client,
    writer: Arc<BatchWriter>,
    producers: Arc<ProducerRegistry>,
    limits: Arc<Limits>,
    max_batch_events: usize,
    trust_cloudflare_headers: bool,
    ingest_version: Arc<str>,
}

impl AppState {
    /// Builds server state from a ClickHouse client and validated application settings.
    pub fn new(
        clickhouse: Client,
        writer: Arc<BatchWriter>,
        producers: Arc<ProducerRegistry>,
        limits: Limits,
        max_batch_events: usize,
        trust_cloudflare_headers: bool,
        ingest_version: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            clickhouse,
            writer,
            producers,
            limits: Arc::new(limits),
            max_batch_events,
            trust_cloudflare_headers,
            ingest_version: ingest_version.into(),
        }
    }
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

/// Runs the standalone HTTP service using environment-based configuration.
pub async fn serve(registry: Arc<Registry>) -> Result<(), String> {
    let config = Config::from_env()?;
    let producers = ProducerRegistry::from_pairs(&config.ingest_keys, Arc::clone(&registry))?;
    let clickhouse = config.clickhouse.client();
    let writer = BatchWriter::new(
        clickhouse.clone(),
        &config.wal_path,
        config.max_insert_batch_events,
        Duration::from_millis(config.batch_wait_ms),
    )?;
    let writer_task = writer.start();
    let state = AppState::new(
        clickhouse,
        Arc::clone(&writer),
        Arc::new(producers),
        config.limits,
        config.max_batch_events,
        config.trust_cloudflare_headers,
        config.ingest_version.clone(),
    );
    let app = router(state, config.max_body_bytes);
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|error| format!("failed to bind listener: {error}"))?;
    info!(address = %config.bind_addr, database = %config.clickhouse.database, "telemetry ingest service listening");
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|error| format!("server error: {error}"));
    writer
        .drain(writer_task, Duration::from_millis(config.shutdown_drain_ms))
        .await;
    result
}

/// Creates the authenticated telemetry router for embedding in another Axum application.
pub fn router(state: AppState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route(
            "/v2/events",
            post(ingest_events)
                // This limit is inside decompression, so it caps decoded bytes as well as guarding
                // the JSON extractor from a compressed-body expansion bomb.
                .layer::<_, std::convert::Infallible>(RequestBodyLimitLayer::new(max_body_bytes))
                .layer::<_, std::convert::Infallible>(
                    RequestDecompressionLayer::new().gzip(true).zstd(true),
                ),
        )
        // The outer limit bounds compressed bytes before the decoder runs.
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
    let mut event_names = rows
        .iter()
        .map(|event| event.event_name.clone())
        .collect::<Vec<_>>();
    event_names.sort_unstable();
    event_names.dedup();
    if !rows.is_empty() {
        state.writer.enqueue(rows).await.map_err(|message| {
            error!(%message, "could not durably append telemetry WAL");
            ApiError::unavailable()
        })?;
    }
    info!(producer = producer.name, accepted, rejected = rejected.len(), event_names = ?event_names, country = %country, "queued durable telemetry batch");
    Ok(Json(AcceptedResponse { accepted, rejected }))
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
        auth::generate_secret,
        event::{IncomingResource, IncomingSubject},
        manifest::Registry,
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use std::{
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tower::ServiceExt;
    use uuid::Uuid;

    fn state() -> AppState {
        let registry = Arc::new(
            Registry::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tenants"))).unwrap(),
        );
        let tenant = registry.first().unwrap().name.clone();
        let secret = generate_secret();
        let producers = Arc::new(
            ProducerRegistry::from_pairs(&format!("{tenant}:{secret}"), registry).unwrap(),
        );
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let suffix = NEXT.fetch_add(1, Ordering::Relaxed);
        let writer = BatchWriter::new(
            Client::default(),
            std::env::temp_dir().join(format!(
                "peak-server-test-{}-{suffix}.wal",
                std::process::id()
            )),
            200,
            Duration::from_secs(5),
        )
        .unwrap();
        AppState::new(
            Client::default(),
            writer,
            producers,
            Limits {
                max_attributes_bytes: 100,
                max_event_age_days: 190,
                max_future_skew_seconds: 300,
            },
            200,
            false,
            "test",
        )
    }

    fn event() -> IncomingEvent {
        IncomingEvent {
            event_id: Uuid::nil(),
            event_name: "session_start".into(),
            schema_version: 1,
            occurred_at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            subject: IncomingSubject {
                kind: "install".into(),
                id: Uuid::nil().to_string(),
            },
            session_id: Some("session".into()),
            resource: IncomingResource {
                service_name: "test-service".into(),
                service_version: "0.1.0".into(),
                platform: None,
                platform_version: None,
            },
            attributes: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn rejects_missing_authentication() {
        let response = router(state(), 1024)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/events")
                    .body(Body::from("[]"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_invalid_authentication() {
        let response = router(state(), 1024)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/events")
                    .header("authorization", "Bearer invalid")
                    .body(Body::from("[]"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn event_fixture_is_validly_shaped() {
        let _ = event();
    }
}
