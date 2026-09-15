#[cfg(feature = "cli")]
mod cli;

#[cfg(feature = "cli")]
use std::sync::Arc;

#[cfg(feature = "cli")]
use peak::{auth, config, dashboard, manifest};

#[cfg(feature = "cli")]
use tracing_subscriber::EnvFilter;

#[cfg(feature = "cli")]
#[tokio::main]
async fn main() {
    init_logging();
    let registry = Arc::new(
        manifest::Registry::load(std::path::Path::new(&config::manifest_dir())).unwrap_or_else(
            |message| {
                eprintln!("invalid tenant manifests: {message}");
                std::process::exit(2);
            },
        ),
    );
    // The CLI/dashboard retain process-local references; the embedded server owns its registry
    // through `Arc` and therefore has no public `'static` requirement.
    let cli_registry = Box::leak(Box::new(
        manifest::Registry::load(std::path::Path::new(&config::manifest_dir())).unwrap_or_else(
            |message| {
                eprintln!("invalid tenant manifests: {message}");
                std::process::exit(2);
            },
        ),
    ));
    match cli::Mode::from_args(cli_registry).unwrap_or_else(|message| {
        eprintln!("{message}");
        std::process::exit(2);
    }) {
        cli::Mode::Serve => peak::server::serve(registry)
            .await
            .unwrap_or_else(|message| {
                eprintln!("{message}");
                std::process::exit(2);
            }),
        cli::Mode::Dashboard { tenant } => dashboard::run(cli_registry, tenant).await,
        cli::Mode::Keygen { tenant } => {
            let secret = auth::generate_secret();
            println!("{}:{secret}", tenant.name);
            eprintln!("append this entry to INGEST_KEYS and restart the service");
        }
    }
}

#[cfg(feature = "cli")]
fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .json()
        .init();
}

#[cfg(not(feature = "cli"))]
fn main() {}
