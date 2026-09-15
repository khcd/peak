//! Embeddable telemetry ingestion backed by ClickHouse.
//!
//! The `server` feature exposes the validated ingest router for applications that want to mount
//! peak inside their own Axum service:
//!
//! ```no_run
//! # #[cfg(feature = "server")]
//! # async fn example(state: peak::server::AppState) {
//! let app = axum::Router::new()
//!     .nest("/telemetry", peak::server::router(state, 1_048_576));
//! # let _ = app;
//! # }
//! ```
#![warn(missing_docs)]

pub mod auth;
#[cfg(feature = "server")]
pub mod batcher;
pub mod config;
pub mod contract;
#[cfg(feature = "dashboard")]
pub mod dashboard;
pub mod error;
pub mod event;
pub mod manifest;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub mod wal;

pub use auth::ProducerRegistry;
pub use config::{Config, Limits};
pub use error::ApiError;
pub use event::IncomingEvent;
pub use manifest::{Registry, Tenant};
#[cfg(feature = "server")]
pub use server::AppState;
