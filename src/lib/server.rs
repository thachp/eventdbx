use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, RwLock},
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
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

use super::{
    admin,
    api_grpc::GrpcApi,
    cli_proxy,
    config::Config,
    error::{EventError, Result},
    graphql::{EventSchema, GraphqlState, build_schema},
    schema::SchemaManager,
    store::{self, AggregateState, EventRecord},
    token::{AccessKind, TokenManager},
};

#[derive(Clone)]
pub(crate) struct AppState {
    tokens: Arc<TokenManager>,
    schemas: Arc<SchemaManager>,
    config: Arc<RwLock<Config>>,
    _config_path: Arc<PathBuf>,
    restrict: bool,
    list_page_size: usize,
    page_limit: usize,
    hidden_aggregates: Arc<HashSet<String>>,
    hidden_fields: Arc<HashMap<String, HashSet<String>>>,
}

pub(crate) async fn run_cli_command(args: Vec<String>) -> Result<cli_proxy::CliCommandResult> {
    let result = cli_proxy::invoke(&args)
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
        self.hidden_aggregates.contains(aggregate_type)
    }

    pub(crate) fn sanitize_aggregate(&self, mut aggregate: AggregateState) -> AggregateState {
        aggregate.state = self.filter_state_map(&aggregate.aggregate_type, aggregate.state);
        aggregate
    }

    fn filter_state_map(
        &self,
        aggregate_type: &str,
        mut state: BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        if let Some(fields) = self.hidden_fields.get(aggregate_type) {
            state.retain(|key, _| !fields.contains(key));
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
    let schemas = Arc::new(SchemaManager::load(config_snapshot.schema_store_path())?);

    let hidden_aggregates: HashSet<String> = config_snapshot
        .hidden_aggregate_types
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let hidden_fields: HashMap<String, HashSet<String>> = config_snapshot
        .hidden_fields
        .iter()
        .map(|(aggregate, fields)| {
            let field_set: HashSet<String> = fields
                .iter()
                .map(|field| field.trim().to_string())
                .filter(|field| !field.is_empty())
                .collect();
            (aggregate.trim().to_string(), field_set)
        })
        .filter(|(_, fields)| !fields.is_empty())
        .collect();

    let state = AppState {
        tokens: Arc::clone(&tokens),
        schemas: Arc::clone(&schemas),
        config: Arc::clone(&shared_config),
        _config_path: Arc::clone(&config_path),
        restrict: config_snapshot.restrict,
        list_page_size: config_snapshot.list_page_size,
        page_limit: config_snapshot.page_limit,
        hidden_aggregates: Arc::new(hidden_aggregates),
        hidden_fields: Arc::new(hidden_fields),
    };

    let api_mode = config_snapshot.api_mode;
    let grpc_enabled = config_snapshot.grpc.enabled || api_mode.grpc_enabled();
    let grpc_bind_addr = config_snapshot.grpc.bind_addr.clone();
    let grpc_state = state.clone();
    let grpc_handle = if grpc_enabled {
        let grpc_addr: SocketAddr = grpc_bind_addr.parse().map_err(|err| {
            EventError::Config(format!(
                "invalid gRPC bind address {}: {}",
                config_snapshot.grpc.bind_addr, err
            ))
        })?;
        Some(tokio::spawn(async move {
            info!("Starting gRPC API server on {}", grpc_addr);
            if let Err(err) = tonic::transport::Server::builder()
                .add_service(GrpcApi::new(grpc_state).into_server())
                .serve_with_shutdown(grpc_addr, async {
                    shutdown_signal().await;
                })
                .await
            {
                warn!("gRPC API server failed: {}", err);
            } else {
                info!("gRPC API server stopped");
            }
        }))
    } else {
        None
    };

    let cli_proxy_handle = cli_proxy::start(Arc::clone(&config_path))
        .await
        .map_err(|err| EventError::Config(format!("failed to start CLI proxy: {err}")))?;

    if !api_mode.rest_enabled() && !api_mode.graphql_enabled() && !grpc_enabled {
        return Err(EventError::Config(
            "at least one API surface (REST, GraphQL, or gRPC) must be enabled".to_string(),
        ));
    }

    let mut app = Router::new();

    if api_mode.rest_enabled() {
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

    if api_mode.graphql_enabled() {
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

    let listener = TcpListener::bind(addr).await?;
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    if let Some(handle) = grpc_handle {
        handle.abort();
    }
    if let Some(handle) = admin_handle {
        handle.abort();
    }
    cli_proxy_handle.abort();

    result.map_err(|err| EventError::Storage(err.to_string()))?;

    Ok(())
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
    state.tokens().authorize(&token, AccessKind::Write)?;

    let payload_map = store::payload_to_map(&request.payload);
    if state.restrict() {
        state.schemas().validate_event(
            &request.aggregate_type,
            &request.event_type,
            &payload_map,
        )?;
    }

    let payload_json = serde_json::to_string(&request.payload)
        .map_err(|err| EventError::Serialization(err.to_string()))?;
    let args = vec![
        "aggregate".to_string(),
        "apply".to_string(),
        request.aggregate_type.clone(),
        request.aggregate_id.clone(),
        request.event_type.clone(),
        "--payload".to_string(),
        payload_json,
    ];

    let record: EventRecord = run_cli_json(args).await?;
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
