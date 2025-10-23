use std::{
    collections::BTreeMap,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, OnceLock, RwLock},
};

use async_graphql::http::GraphQLPlaygroundConfig;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{RwLock as AsyncRwLock, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

use super::{
    admin,
    api_grpc::GrpcApi,
    cli_proxy,
    config::Config,
    error::{EventError, Result},
    graphql::{EventSchema, GraphqlState, build_schema},
    plugin::PluginManager,
    replication_capnp_client::decode_public_key_bytes,
    schema::SchemaManager,
    store::{ActorClaims, AggregateState, AppendEvent, EventRecord, EventStore},
    token::{AccessKind, TokenManager},
};

static CLI_PROXY_ADDR: OnceLock<Arc<AsyncRwLock<String>>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct AppState {
    tokens: Arc<TokenManager>,
    schemas: Arc<SchemaManager>,
    config: Arc<RwLock<Config>>,
    _config_path: Arc<PathBuf>,
    store: Arc<EventStore>,
    restrict: bool,
    list_page_size: usize,
    page_limit: usize,
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
        Arc::clone(&self.tokens)
    }

    pub(crate) fn schemas(&self) -> Arc<SchemaManager> {
        Arc::clone(&self.schemas)
    }

    pub(crate) fn config(&self) -> Arc<RwLock<Config>> {
        Arc::clone(&self.config)
    }

    pub(crate) fn restrict(&self) -> bool {
        self.restrict
    }

    pub(crate) fn list_page_size(&self) -> usize {
        self.list_page_size
    }

    pub(crate) fn page_limit(&self) -> usize {
        self.page_limit
    }

    pub(crate) fn is_hidden_aggregate(&self, aggregate_type: &str) -> bool {
        self.schemas
            .get(aggregate_type)
            .map(|schema| schema.hidden)
            .unwrap_or(false)
    }

    pub(crate) fn sanitize_aggregate(&self, mut aggregate: AggregateState) -> AggregateState {
        aggregate.state = self.filter_state_map(&aggregate.aggregate_type, aggregate.state);
        aggregate
    }

    pub(crate) fn store(&self) -> Arc<EventStore> {
        Arc::clone(&self.store)
    }

    fn filter_state_map(
        &self,
        aggregate_type: &str,
        mut state: BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        if let Ok(schema) = self.schemas.get(aggregate_type) {
            if !schema.hidden_fields.is_empty() {
                state.retain(|key, _| !schema.hidden_fields.contains(key));
            }
        }
        state
    }
}

pub async fn run(config: Config, config_path: PathBuf) -> Result<()> {
    let config_snapshot = config.clone();
    let shared_config = Arc::new(RwLock::new(config));
    let config_path = Arc::new(config_path);

    let encryption = config_snapshot.encryption_key()?;
    let tokens = Arc::new(TokenManager::load(
        config_snapshot.tokens_path(),
        encryption.clone(),
    )?);
    let store = Arc::new(EventStore::open(
        config_snapshot.event_store_path(),
        encryption.clone(),
    )?);
    let local_public_key = Arc::new(
        decode_public_key_bytes(
            &config_snapshot
                .load_public_key()
                .map_err(|err| EventError::Config(err.to_string()))?,
        )
        .map_err(|err| EventError::Config(err.to_string()))?,
    );
    let schemas = Arc::new(SchemaManager::load(config_snapshot.schema_store_path())?);

    let state = AppState {
        tokens: Arc::clone(&tokens),
        schemas: Arc::clone(&schemas),
        config: Arc::clone(&shared_config),
        _config_path: Arc::clone(&config_path),
        store: Arc::clone(&store),
        restrict: config_snapshot.restrict,
        list_page_size: config_snapshot.list_page_size,
        page_limit: config_snapshot.page_limit,
    };

    let api = config_snapshot.api.clone();
    let grpc_enabled = api.grpc_enabled();
    let mut grpc_handle = None;
    let mut shared_grpc = false;

    if grpc_enabled {
        let parsed: SocketAddr = config_snapshot.grpc.bind_addr.parse().map_err(|err| {
            EventError::Config(format!(
                "invalid gRPC bind address {}: {}",
                config_snapshot.grpc.bind_addr, err
            ))
        })?;
        shared_grpc = parsed.port() == config_snapshot.port;

        if !shared_grpc {
            let grpc_state = state.clone();
            let addr = parsed;
            grpc_handle = Some(tokio::spawn(async move {
                info!("Starting gRPC API server on {}", addr);
                if let Err(err) = tonic::transport::Server::builder()
                    .add_service(GrpcApi::new(grpc_state).into_server())
                    .serve_with_shutdown(addr, async {
                        shutdown_signal().await;
                    })
                    .await
                {
                    warn!("gRPC API server failed: {}", err);
                } else {
                    info!("gRPC API server stopped");
                }
            }));
        }
    }

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
        Arc::clone(&store),
        Arc::clone(&schemas),
        Arc::clone(&local_public_key),
    )
    .await
    .map_err(|err| EventError::Config(format!("failed to start CLI proxy: {err}")))?;

    if !api.rest_enabled() && !api.graphql_enabled() && !grpc_enabled {
        return Err(EventError::Config(
            "at least one API surface (REST, GraphQL, or gRPC) must be enabled".to_string(),
        ));
    }

    let mut app = Router::new();

    if api.rest_enabled() {
        let rest_router = Router::new()
            .route("/health", get(health))
            .route("/v1/aggregates", get(list_aggregates))
            .route(
                "/v1/aggregates/{aggregate_type}/{aggregate_id}",
                get(get_aggregate),
            )
            .route(
                "/v1/aggregates/{aggregate_type}/{aggregate_id}/events",
                get(list_events),
            )
            .route("/v1/events", post(append_event_global))
            .route(
                "/v1/aggregates/{aggregate_type}/{aggregate_id}/verify",
                get(verify_aggregate),
            )
            .with_state(state.clone());
        app = app.merge(rest_router);
    }

    if api.graphql_enabled() {
        let graphql_state = GraphqlState::new(state.clone());
        let graphql_schema = build_schema(graphql_state);
        let graphql_router = Router::new()
            .route("/graphql", get(graphql_playground).post(graphql_handler))
            .route("/graphql/playground", get(graphql_playground))
            .layer(Extension(graphql_schema));
        app = app.merge(graphql_router);
    }

    let admin_config = config_snapshot.admin.clone();
    if admin_config.enabled && !config_snapshot.admin_master_key_configured() {
        return Err(EventError::Config(
            "admin API requires a configured master key (`eventdbx config --admin-master-key`)"
                .to_string(),
        ));
    }

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

    let app = app.layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], config_snapshot.port));
    info!(
        "Starting EventDBX server on {addr} (restrict={})",
        config_snapshot.restrict
    );
    if shared_grpc {
        info!(
            "Starting gRPC API server on shared HTTP listener (port {})",
            config_snapshot.port
        );
    }

    let listener = TcpListener::bind(addr).await?;
    let result = if shared_grpc {
        serve_shared(listener, app, state.clone()).await
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|err| EventError::Storage(err.to_string()))
    };

    if let Some(handle) = grpc_handle {
        handle.abort();
    }
    if let Some(handle) = admin_handle {
        handle.abort();
    }
    cli_proxy_handle.abort();

    result?;

    Ok(())
}

async fn serve_shared(listener: TcpListener, app: Router, grpc_state: AppState) -> Result<()> {
    let (grpc_tx, grpc_rx) = mpsc::channel::<TcpStream>(64);

    let http_listener = SharedHttpListener::new(listener, grpc_tx);

    let grpc_future = async {
        let incoming = ReceiverStream::new(grpc_rx).map(|stream| Ok::<_, io::Error>(stream));
        tonic::transport::Server::builder()
            .add_service(GrpcApi::new(grpc_state).into_server())
            .serve_with_incoming_shutdown(incoming, async {
                shutdown_signal().await;
            })
            .await
    };

    let http_future = axum::serve(http_listener, app).with_graceful_shutdown(shutdown_signal());

    let (grpc_result, http_result) = tokio::join!(grpc_future, http_future);

    match grpc_result {
        Ok(_) => info!("gRPC API server stopped"),
        Err(err) => {
            warn!("gRPC API server failed: {}", err);
            return Err(EventError::Storage(err.to_string()));
        }
    }

    http_result.map_err(|err| EventError::Storage(err.to_string()))
}

struct SharedHttpListener {
    inner: TcpListener,
    grpc_tx: mpsc::Sender<TcpStream>,
}

impl SharedHttpListener {
    fn new(inner: TcpListener, grpc_tx: mpsc::Sender<TcpStream>) -> Self {
        Self { inner, grpc_tx }
    }
}

impl axum::serve::Listener for SharedHttpListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send {
        let listener = &self.inner;
        let grpc_sender = self.grpc_tx.clone();

        async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            warn!("failed to set TCP_NODELAY on incoming connection: {err}");
                        }

                        if is_grpc_connection(&stream).await {
                            if let Err(err) = grpc_sender.send(stream).await {
                                warn!("dropping gRPC connection: {}", err);
                            }
                            continue;
                        }

                        return (stream, addr);
                    }
                    Err(err) => {
                        warn!("failed to accept connection: {}", err);
                        continue;
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

async fn is_grpc_connection(stream: &TcpStream) -> bool {
    const PREFACE: &[u8] = b"PRI";
    let mut buf = [0u8; 3];
    match stream.peek(&mut buf).await {
        Ok(n) if n >= PREFACE.len() => &buf[..PREFACE.len()] == PREFACE,
        Ok(_) => false,
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            false
        }
        Err(err) => {
            warn!("failed to peek connection bytes: {}", err);
            false
        }
    }
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
}

#[derive(Deserialize, Default)]
struct AggregatesQuery {
    #[serde(default)]
    skip: Option<usize>,
    #[serde(default)]
    take: Option<usize>,
}

async fn list_aggregates(
    State(state): State<AppState>,
    Query(params): Query<AggregatesQuery>,
) -> Result<Json<Vec<AggregateState>>> {
    let skip = params.skip.unwrap_or(0);
    let mut take = params.take.unwrap_or(state.list_page_size());
    if take == 0 {
        return Ok(Json(Vec::new()));
    }
    if take > state.page_limit() {
        take = state.page_limit();
    }
    let args = vec![
        "aggregate".to_string(),
        "list".to_string(),
        "--skip".to_string(),
        skip.to_string(),
        "--take".to_string(),
        take.to_string(),
        "--json".to_string(),
    ];

    let aggregates: Vec<AggregateState> = run_cli_json(args).await?;
    let page = aggregates
        .into_iter()
        .filter(|aggregate| !state.is_hidden_aggregate(&aggregate.aggregate_type))
        .map(|aggregate| state.sanitize_aggregate(aggregate))
        .collect();
    Ok(Json(page))
}

async fn get_aggregate(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<AggregateState>> {
    if state.is_hidden_aggregate(&aggregate_type) {
        return Err(EventError::AggregateNotFound);
    }
    let args = vec![
        "aggregate".to_string(),
        "get".to_string(),
        aggregate_type.clone(),
        aggregate_id.clone(),
    ];

    let mut aggregate: AggregateState = run_cli_json(args).await?;
    aggregate = state.sanitize_aggregate(aggregate);
    Ok(Json(aggregate))
}

#[derive(Deserialize, Default)]
struct EventsQuery {
    #[serde(default)]
    skip: Option<usize>,
    #[serde(default)]
    take: Option<usize>,
}

async fn list_events(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
    Query(params): Query<EventsQuery>,
) -> Result<Json<Vec<EventRecord>>> {
    if state.is_hidden_aggregate(&aggregate_type) {
        return Err(EventError::AggregateNotFound);
    }
    let skip = params.skip.unwrap_or(0);
    let mut take = params.take.unwrap_or(state.page_limit());
    if take == 0 {
        return Ok(Json(Vec::new()));
    }
    if take > state.page_limit() {
        take = state.page_limit();
    }

    let args = vec![
        "aggregate".to_string(),
        "replay".to_string(),
        aggregate_type.clone(),
        aggregate_id.clone(),
        "--skip".to_string(),
        skip.to_string(),
        "--take".to_string(),
        take.to_string(),
        "--json".to_string(),
    ];

    let events: Vec<EventRecord> = run_cli_json(args).await?;
    Ok(Json(events))
}

async fn graphql_handler(
    Extension(schema): Extension<EventSchema>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> GraphQLResponse {
    let request = request.into_inner().data(headers);
    schema.execute(request).await.into()
}

async fn graphql_playground() -> impl IntoResponse {
    Html(async_graphql::http::playground_source(
        GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

#[derive(Deserialize)]
struct AppendEventGlobalRequest {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Value,
}

async fn append_event_global(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AppendEventGlobalRequest>,
) -> Result<Json<EventRecord>> {
    let token = extract_bearer_token(&headers).ok_or(EventError::Unauthorized)?;
    let grant = state.tokens().authorize(&token, AccessKind::Write)?;

    let AppendEventGlobalRequest {
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
    } = request;

    if state.restrict() {
        state
            .schemas()
            .validate_event(&aggregate_type, &event_type, &payload)?;
    }

    let issued_by: Option<ActorClaims> = Some(grant.into());
    let store = state.store();
    let schemas = state.schemas();

    let append_input = AppendEvent {
        aggregate_type: aggregate_type.clone(),
        aggregate_id: aggregate_id.clone(),
        event_type: event_type.clone(),
        payload: payload.clone(),
        issued_by,
    };

    let record = tokio::task::spawn_blocking({
        let store = Arc::clone(&store);
        move || store.append(append_input)
    })
    .await
    .map_err(|err| EventError::Storage(format!("failed to append event: {err}")))??;

    if schemas.should_snapshot(&record.aggregate_type, record.version) {
        let aggregate_type = record.aggregate_type.clone();
        let aggregate_id = record.aggregate_id.clone();
        let version = record.version;
        let snapshot_result = tokio::task::spawn_blocking({
            let store = Arc::clone(&store);
            move || {
                store.create_snapshot(
                    &aggregate_type,
                    &aggregate_id,
                    Some(format!("auto snapshot v{}", version)),
                )
            }
        })
        .await
        .map_err(|err| EventError::Storage(format!("failed to create auto snapshot: {err}")))?;

        if let Err(err) = snapshot_result {
            warn!(
                target: "server",
                "failed to create auto snapshot for {}::{} v{}: {}",
                record.aggregate_type,
                record.aggregate_id,
                record.version,
                err
            );
        }
    }

    let plugins = {
        let config = state.config();
        let guard = config.read().unwrap();
        PluginManager::from_config(&*guard)?
    };

    if !plugins.is_empty() {
        let schema = schemas.get(&record.aggregate_type).ok();
        let aggregate_type = record.aggregate_type.clone();
        let aggregate_id = record.aggregate_id.clone();
        let state_result = tokio::task::spawn_blocking({
            let store = Arc::clone(&store);
            move || store.get_aggregate_state(&aggregate_type, &aggregate_id)
        })
        .await
        .map_err(|err| EventError::Storage(format!("failed to load aggregate state: {err}")))?;

        match state_result {
            Ok(current_state) => {
                if let Err(err) = plugins.notify_event(&record, &current_state, schema.as_ref()) {
                    error!("plugin notification failed: {err}");
                }
            }
            Err(err) => {
                warn!(
                    "plugin notification skipped (failed to load state for {}::{}): {}",
                    record.aggregate_type, record.aggregate_id, err
                );
            }
        }
    }

    Ok(Json(record))
}

async fn verify_aggregate(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<VerifyResponse>> {
    if state.is_hidden_aggregate(&aggregate_type) {
        return Err(EventError::AggregateNotFound);
    }
    let args = vec![
        "aggregate".to_string(),
        "verify".to_string(),
        aggregate_type.clone(),
        aggregate_id.clone(),
        "--json".to_string(),
    ];
    let response: VerifyResponse = run_cli_json(args).await?;
    Ok(Json(response))
}

#[derive(Serialize, Deserialize)]
struct VerifyResponse {
    merkle_root: String,
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
