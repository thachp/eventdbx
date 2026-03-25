use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use axum::{Json, Router, middleware::from_fn, response::IntoResponse, routing::get};
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

use super::{
    cli_proxy,
    config::Config,
    error::{EventError, Result},
    observability,
    schema::SchemaManager,
    service::CoreContext,
    store::EventStore,
    tenant::{CoreProvider, StaticCoreProvider},
    token::TokenManager,
};

pub async fn run(config: Config, config_path: PathBuf) -> Result<()> {
    observability::init()
        .map_err(|err| EventError::Config(format!("failed to initialise observability: {err}")))?;

    let config_snapshot = config.clone();
    let shared_config = Arc::new(RwLock::new(config));
    let config_path = Arc::new(config_path);

    let encryption = config_snapshot.encryption_key()?;
    let jwt_config = config_snapshot.jwt_manager_config()?;
    let tokens = Arc::new(TokenManager::load(
        jwt_config,
        config_snapshot.tokens_path(),
        config_snapshot.jwt_revocations_path(),
        encryption.clone(),
    )?);
    let store = Arc::new(EventStore::open(
        config_snapshot.event_store_path(),
        encryption,
        config_snapshot.snowflake_worker_id,
    )?);
    let schemas = Arc::new(SchemaManager::load(config_snapshot.schema_store_path())?);
    let core = CoreContext::new(
        Arc::clone(&tokens),
        schemas,
        store,
        config_snapshot.restrict,
        config_snapshot.list_page_size,
        config_snapshot.page_limit,
        config_snapshot.active_domain().to_string(),
        None,
        None,
        Arc::new(crate::tenant_store::TenantAssignmentStore::open(
            config_snapshot.tenant_meta_path(),
        )?),
        config_snapshot.reference_default_depth,
        config_snapshot.reference_max_depth,
    );
    let core_provider = Arc::new(StaticCoreProvider::new(core));

    let cli_bind_addr = config_snapshot.socket.bind_addr.clone();
    let cli_proxy_handle = cli_proxy::start(
        &cli_bind_addr,
        Arc::clone(&config_path),
        Arc::clone(&tokens),
        Arc::clone(&core_provider) as Arc<dyn CoreProvider>,
        Arc::clone(&shared_config),
    )
    .await
    .map_err(|err| EventError::Config(format!("failed to start CLI proxy: {err}")))?;

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(observability::metrics_handler));

    let app = app
        .layer(from_fn(observability::track_http_metrics))
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], config_snapshot.port));
    info!(
        "Starting EventDBX server on {addr} (restrict={})",
        config_snapshot.restrict
    );

    let listener = TcpListener::bind(addr).await?;
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| EventError::Storage(err.to_string()));

    cli_proxy_handle.abort();

    result?;

    Ok(())
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
