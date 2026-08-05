use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use tracing::warn;

/// A batch-level failure. The whole request is rejected and nothing is stored.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: "a valid Bearer token is required".into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_payload",
            message: message.into(),
        }
    }

    pub fn too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "payload_too_large",
            message: message.into(),
        }
    }

    pub fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "storage_unavailable",
            message: "telemetry storage is temporarily unavailable; retry this batch".into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Keep diagnostics useful without leaking bearer tokens, payloads, IPs, or subject IDs.
        warn!(status = %self.status, code = self.code, "telemetry request rejected");
        (
            self.status,
            Json(serde_json::json!({ "error": self.code, "message": self.message })),
        )
            .into_response()
    }
}

/// A single-event failure. The rest of the batch is still stored, and the event is
/// reported back by index so the client can drop exactly the bad event.
#[derive(Debug)]
pub struct EventError {
    pub code: &'static str,
    pub message: String,
}

impl EventError {
    /// The generic envelope is malformed: bad timestamp, oversized field, unknown subject kind.
    pub fn invalid_envelope(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_envelope",
            message: message.into(),
        }
    }

    /// No approved contract exists for this `(producer, event_name, schema_version)`.
    /// Distinct from `invalid_attributes` so a client that shipped ahead of the server
    /// is distinguishable from a client sending garbage.
    pub fn unknown_contract(message: impl Into<String>) -> Self {
        Self {
            code: "unknown_contract",
            message: message.into(),
        }
    }

    /// The envelope is fine but `attributes` violates its approved contract.
    pub fn invalid_attributes(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_attributes",
            message: message.into(),
        }
    }

    pub fn too_large(message: impl Into<String>) -> Self {
        Self {
            code: "payload_too_large",
            message: message.into(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct EventRejection {
    pub index: usize,
    pub code: &'static str,
    pub message: String,
}

impl EventRejection {
    pub fn new(index: usize, error: EventError) -> Self {
        Self {
            index,
            code: error.code,
            message: error.message,
        }
    }
}
