//! `strom-server` process entrypoint.

use std::error::Error;
use std::sync::Arc;

use clap::Parser as _;
use strom_db::{CloseOutcome, Db};
use strom_server::config::ServerConfig;
use strom_server::router;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = ServerConfig::parse();
    let store = config.build_store()?;
    let db = Arc::new(Db::open(store).await?);
    tracing::info!(partition = %db.partition_id(), "opened partition");
    let app = router(Arc::clone(&db));
    let listener = TcpListener::bind(config.bind)
        .await
        .map_err(|error| format!("bind {}: {error}", config.bind))?;
    tracing::info!(%config.bind, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|error| format!("serve: {error}"))?;

    let db = Arc::into_inner(db).expect("router released its Db Arc after serve stopped");
    let outcome = db.close().await;
    match &outcome {
        CloseOutcome::Shutdown => tracing::info!(?outcome, "db closed"),
        CloseOutcome::Fenced
        | CloseOutcome::Poisoned { .. }
        | CloseOutcome::Contradiction { .. } => {
            tracing::error!(?outcome, "db closed with a non-clean outcome");
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl_c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
