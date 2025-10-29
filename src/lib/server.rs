use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, OnceLock, RwLock},
};

use axum::{
    Json, Router, http::HeaderMap, middleware::from_fn, response::IntoResponse, routing::get,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{net::TcpListener, sync::RwLock as AsyncRwLock};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

use super::{
    admin, cli_proxy,
    config::Config,
    error::{EventError, Result},
    observability,
    schema::SchemaManager,
    service::CoreContext,
    store::EventStore,
    token::TokenManager,
};

static CLI_PROXY_ADDR: OnceLock<Arc<AsyncRwLock<String>>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct AppState {
    core: CoreContext,
    _config_path: Arc<PathBuf>,
}

pub(crate) async fn run_cli_command(args: Vec<String>) -> Result<cli_proxy::CliCommandResult> {
    let addr_lock = CLI_PROXY_ADDR
        .get()
        .ok_or_else(|| EventError::Config("CLI proxy address not configured".to_string()))?;
    let bind_addr = {
        let guard = addr_lock.read().await;
        guard.clone()
    };
    let connect_addr = normalize_cli_connect_addr(&bind_addr);

    let result = cli_proxy::invoke(&args, &connect_addr)
        .await
        .map_err(|err| EventError::Storage(err.to_string()))?;
    if result.exit_code != 0 {
        let message = if !result.stderr.trim().is_empty() {
            result.stderr.trim().to_string()
        } else if !result.stdout.trim().is_empty() {
            result.stdout.trim().to_string()
        } else {
            format!("exit code {}", result.exit_code)
        };
        let lowered = message.to_lowercase();
        if lowered.contains("aggregate not found") {
            return Err(EventError::AggregateNotFound);
        }
        if lowered.contains("schema not found") {
            return Err(EventError::SchemaNotFound);
        }
        return Err(EventError::Storage(format!(
            "CLI command {:?} failed: {message}",
            args
        )));
    }
    Ok(result)
}

fn normalize_cli_connect_addr(bind_addr: &str) -> String {
    if let Ok(addr) = bind_addr.parse::<SocketAddr>() {
        match addr.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => format!("127.0.0.1:{}", addr.port()),
            IpAddr::V6(ip) if ip.is_unspecified() => format!("[::1]:{}", addr.port()),
            _ => addr.to_string(),
        }
    } else {
        bind_addr.to_string()
    }
}

pub(crate) async fn run_cli_json<T>(args: Vec<String>) -> Result<T>
where
    T: DeserializeOwned,
{
    let result = run_cli_command(args).await?;
    serde_json::from_str(&result.stdout).map_err(|err| EventError::Serialization(err.to_string()))
}

impl AppState {
    pub(crate) fn tokens(&self) -> Arc<TokenManager> {
        self.core.tokens()
    }

    pub(crate) fn schemas(&self) -> Arc<SchemaManager> {
        self.core.schemas()
    }
}

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
        encryption.clone(),
        config_snapshot.snowflake_worker_id,
    )?);
    let schemas = Arc::new(SchemaManager::load(config_snapshot.schema_store_path())?);

    let core = CoreContext::new(
        Arc::clone(&tokens),
        Arc::clone(&schemas),
        Arc::clone(&store),
        config_snapshot.restrict,
        config_snapshot.list_page_size,
        config_snapshot.page_limit,
    );

    let state = AppState {
        core: core.clone(),
        _config_path: Arc::clone(&config_path),
    };

    let cli_bind_addr = config_snapshot.socket.bind_addr.clone();
    let addr_store =
        Arc::clone(CLI_PROXY_ADDR.get_or_init(|| Arc::new(AsyncRwLock::new(String::new()))));
    {
        let mut guard = addr_store.write().await;
        *guard = cli_bind_addr.clone();
    }

    let cli_proxy_handle = cli_proxy::start(
        &cli_bind_addr,
        Arc::clone(&config_path),
        core.clone(),
        Arc::clone(&shared_config),
    )
    .await
    .map_err(|err| EventError::Config(format!("failed to start CLI proxy: {err}")))?;

    let mut app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(observability::metrics_handler));

    let admin_config = config_snapshot.admin.clone();
    let mut admin_handle = None;
    if admin_config.enabled {
        let admin_router = admin::build_router(state.clone(), admin_config.clone());
        if let Some(port) = admin_config.port {
            let bind_addr = format!("{}:{}", admin_config.bind_addr, port);
            let admin_addr: SocketAddr = bind_addr.parse().map_err(|err| {
                EventError::Config(format!(
                    "invalid admin bind address {}:{} - {}",
                    admin_config.bind_addr, port, err
                ))
            })?;
            let listener = TcpListener::bind(admin_addr).await.map_err(|err| {
                EventError::Config(format!(
                    "failed to bind admin API listener on {}: {}",
                    admin_addr, err
                ))
            })?;
            info!("Starting admin API server on {}", admin_addr);
            let admin_app = admin_router.clone().layer(TraceLayer::new_for_http());
            admin_handle = Some(tokio::spawn(async move {
                if let Err(err) = axum::serve(listener, admin_app)
                    .with_graceful_shutdown(shutdown_signal())
                    .await
                {
                    warn!("admin API server failed: {}", err);
                } else {
                    info!("admin API server stopped");
                }
            }));
        } else {
            app = app.merge(admin_router);
        }
    }

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

    if let Some(handle) = admin_handle {
        handle.abort();
    }
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

pub(crate) fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?;
    let value = value.to_str().ok()?;
    if let Some(token) = value.strip_prefix("Bearer ") {
        Some(token.trim().to_string())
    } else {
        None
    }
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
