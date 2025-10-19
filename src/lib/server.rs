use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    net::TcpListener,
    sync::Notify,
    time::{Duration, sleep},
};
use tokio_stream::wrappers::TcpListenerStream;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use uuid::Uuid;

use super::{
    api_grpc::GrpcApi,
    config::Config,
    error::{EventError, Result},
    graphql::{EventSchema, GraphqlState, build_schema},
    plugin::PluginManager,
    replication::{
        ReplicationManager, ReplicationService, proto::replication_server::ReplicationServer,
    },
    schema::SchemaManager,
    store::{self, AggregateState, AppendEvent, EventRecord, EventStore},
    token::{AccessKind, TokenManager},
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Arc<EventStore>,
    pub(crate) tokens: Arc<TokenManager>,
    pub(crate) schemas: Arc<SchemaManager>,
    pub(crate) restrict: bool,
    pub(crate) list_page_size: usize,
    pub(crate) page_limit: usize,
    plugins: PluginManager,
    retry_queue: Arc<PluginRetryQueue>,
    plugin_max_attempts: u32,
    replication: Option<ReplicationManager>,
    hidden_aggregates: Arc<HashSet<String>>,
    hidden_fields: Arc<HashMap<String, HashSet<String>>>,
}

impl AppState {
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

struct PluginRetryQueue {
    pending: Mutex<VecDeque<PendingNotification>>,
    dead: Mutex<Vec<DeadNotification>>,
    notify: Notify,
    path: PathBuf,
}

impl PluginRetryQueue {
    fn new(path: PathBuf) -> Self {
        let queue = Self {
            pending: Mutex::new(VecDeque::new()),
            dead: Mutex::new(Vec::new()),
            notify: Notify::new(),
            path,
        };
        let _ = queue.persist();
        queue
    }

    fn push_pending(&self, notification: PendingNotification) {
        {
            let mut queue = self.pending.lock().expect("queue mutex poisoned");
            queue.push_back(notification);
        }
        self.notify.notify_one();
        let _ = self.persist();
    }

    fn schedule_retry(&self, record: EventRecord, attempt: u32, max_attempts: u32) {
        if attempt > max_attempts {
            warn!(
                aggregate_type = %record.aggregate_type,
                aggregate_id = %record.aggregate_id,
                event_type = %record.event_type,
                attempts = attempt,
                "plugin notification marked as dead after exceeding max attempts"
            );
            let mut dead = self.dead.lock().expect("dead queue mutex poisoned");
            dead.push(DeadNotification {
                record,
                attempts: attempt,
            });
            let _ = self.persist();
        } else {
            self.push_pending(PendingNotification { record, attempt });
        }
    }

    fn stats(&self) -> QueueStatus {
        let (pending_events, pending_count) = {
            let queue = self.pending.lock().expect("queue mutex poisoned");
            let items = queue
                .iter()
                .map(|item| QueueEntry {
                    event_id: item.record.metadata.event_id,
                    aggregate_type: item.record.aggregate_type.clone(),
                    aggregate_id: item.record.aggregate_id.clone(),
                    event_type: item.record.event_type.clone(),
                    attempts: item.attempt,
                })
                .collect::<Vec<_>>();
            (items, queue.len())
        };

        let (dead_events, dead_count) = {
            let queue = self.dead.lock().expect("dead queue mutex poisoned");
            let items = queue
                .iter()
                .map(|item| QueueEntry {
                    event_id: item.record.metadata.event_id,
                    aggregate_type: item.record.aggregate_type.clone(),
                    aggregate_id: item.record.aggregate_id.clone(),
                    event_type: item.record.event_type.clone(),
                    attempts: item.attempts,
                })
                .collect::<Vec<_>>();
            (items, queue.len())
        };

        QueueStatus {
            pending: pending_count,
            dead: dead_count,
            pending_events,
            dead_events,
        }
    }

    async fn pop(&self) -> PendingNotification {
        loop {
            if let Some(notification) = {
                let mut queue = self.pending.lock().expect("queue mutex poisoned");
                let item = queue.pop_front();
                if item.is_some() {
                    let _ = self.persist();
                }
                item
            } {
                return notification;
            }
            self.notify.notified().await;
        }
    }

    fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let status = self.stats();
        let payload = serde_json::to_string_pretty(&status)?;
        fs::write(&self.path, payload)?;
        Ok(())
    }
}

#[derive(Clone)]
struct PendingNotification {
    record: EventRecord,
    attempt: u32,
}

#[allow(dead_code)]
struct DeadNotification {
    record: EventRecord,
    attempts: u32,
}

#[derive(Serialize)]
struct QueueStatus {
    pending: usize,
    dead: usize,
    pending_events: Vec<QueueEntry>,
    dead_events: Vec<QueueEntry>,
}

#[derive(Serialize)]
struct QueueEntry {
    event_id: Uuid,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    attempts: u32,
}

pub async fn run(config: Config, plugins: PluginManager) -> Result<()> {
    let encryption = config.encryption_key()?;
    let store = Arc::new(EventStore::open(
        config.event_store_path(),
        encryption.clone(),
    )?);
    let tokens = Arc::new(TokenManager::load(
        config.tokens_path(),
        encryption.clone(),
    )?);
    let schemas = Arc::new(SchemaManager::load(config.schema_store_path())?);
    let retry_queue = Arc::new(PluginRetryQueue::new(config.plugin_queue_path()));
    let replication_service = ReplicationService::new(Arc::clone(&store));
    let replication_manager = if config.remotes.is_empty() {
        None
    } else {
        Some(ReplicationManager::from_config(&config))
    };
    let replication_addr: SocketAddr = config.replication.bind_addr.parse().map_err(|err| {
        EventError::Config(format!(
            "invalid replication bind address {}: {}",
            config.replication.bind_addr, err
        ))
    })?;

    match TcpListener::bind(replication_addr).await {
        Ok(listener) => {
            let service = ReplicationServer::new(replication_service.clone());
            info!(
                "Replication service listening on {}",
                config.replication.bind_addr
            );
            tokio::spawn(async move {
                let incoming = TcpListenerStream::new(listener);
                if let Err(err) = tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, async {
                        shutdown_signal().await;
                    })
                    .await
                {
                    tracing::error!("replication server failed: {}", err);
                }
            });
        }
        Err(err) => {
            warn!(
                "replication listener disabled ({}): {}",
                config.replication.bind_addr, err
            );
        }
    }
    let hidden_aggregates: HashSet<String> = config
        .hidden_aggregate_types
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let hidden_fields: HashMap<String, HashSet<String>> = config
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
        store: Arc::clone(&store),
        tokens,
        schemas: Arc::clone(&schemas),
        restrict: config.restrict,
        plugins: plugins.clone(),
        list_page_size: config.list_page_size,
        page_limit: config.page_limit,
        retry_queue,
        plugin_max_attempts: config.plugin_max_attempts,
        replication: replication_manager.clone(),
        hidden_aggregates: Arc::new(hidden_aggregates),
        hidden_fields: Arc::new(hidden_fields),
    };

    start_plugin_retry_worker(state.clone());

    let grpc_enabled = config.grpc.enabled;
    let grpc_bind_addr = config.grpc.bind_addr.clone();
    let grpc_handle = if grpc_enabled {
        let grpc_addr: SocketAddr = grpc_bind_addr.parse().map_err(|err| {
            EventError::Config(format!(
                "invalid gRPC bind address {}: {}",
                config.grpc.bind_addr, err
            ))
        })?;
        let grpc_state = state.clone();
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

    let api_mode = config.api_mode;
    if !api_mode.rest_enabled() && !api_mode.graphql_enabled() {
        return Err(EventError::Config(
            "at least one API surface (REST or GraphQL) must be enabled".to_string(),
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

    let app = app.layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!(
        "Starting EventDBX server on {addr} (restrict={})",
        config.restrict
    );

    let listener = TcpListener::bind(addr).await?;
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    if let Some(handle) = grpc_handle {
        handle.abort();
    }

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
    let mut take = params.take.unwrap_or(state.list_page_size);
    if take == 0 {
        return Ok(Json(Vec::new()));
    }
    if take > state.page_limit {
        take = state.page_limit;
    }
    let mut aggregates = state.store.aggregates();
    aggregates.retain(|aggregate| !state.is_hidden_aggregate(&aggregate.aggregate_type));
    let page = aggregates
        .into_iter()
        .skip(skip)
        .take(take)
        .map(|aggregate| state.sanitize_aggregate(aggregate))
        .collect::<Vec<_>>();
    Ok(Json(page))
}

async fn get_aggregate(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<AggregateState>> {
    if state.is_hidden_aggregate(&aggregate_type) {
        return Err(EventError::AggregateNotFound);
    }
    let aggregate = state
        .store
        .get_aggregate_state(&aggregate_type, &aggregate_id)?;
    Ok(Json(state.sanitize_aggregate(aggregate)))
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
    let mut take = params.take.unwrap_or(state.page_limit);
    if take == 0 {
        return Ok(Json(Vec::new()));
    }
    if take > state.page_limit {
        take = state.page_limit;
    }

    let events = state.store.list_events(&aggregate_type, &aggregate_id)?;
    let filtered: Vec<EventRecord> = events.into_iter().skip(skip).take(take).collect();
    Ok(Json(filtered))
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
    let claims = state.tokens.authorize(&token, AccessKind::Write)?.into();

    let payload_map = store::payload_to_map(&request.payload);
    if state.restrict {
        state
            .schemas
            .validate_event(&request.aggregate_type, &request.event_type, &payload_map)?;
    }

    let record = state.store.append(AppendEvent {
        aggregate_type: request.aggregate_type,
        aggregate_id: request.aggregate_id,
        event_type: request.event_type,
        payload: request.payload,
        issued_by: Some(claims),
    })?;

    notify_plugins(&state, &record);
    replicate_events(&state, std::slice::from_ref(&record));

    Ok(Json(record))
}

async fn verify_aggregate(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<VerifyResponse>> {
    let merkle_root = state.store.verify(&aggregate_type, &aggregate_id)?;
    Ok(Json(VerifyResponse { merkle_root }))
}

#[derive(Serialize)]
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

pub(crate) fn replicate_events(state: &AppState, records: &[EventRecord]) {
    if let Some(manager) = &state.replication {
        manager.enqueue(records);
    }
}

fn start_plugin_retry_worker(state: AppState) {
    tokio::spawn(async move {
        loop {
            let PendingNotification { record, attempt } = state.retry_queue.pop().await;
            let delay = backoff_delay(attempt);
            sleep(delay).await;

            match dispatch_plugin_notification(&state, &record).await {
                Ok(_) => {
                    // success, nothing else to do
                }
                Err(EventError::AggregateNotFound) => {
                    warn!(
                        aggregate_type = %record.aggregate_type,
                        aggregate_id = %record.aggregate_id,
                        event_type = %record.event_type,
                        "plugin retry skipped: aggregate no longer exists"
                    );
                }
                Err(err) => {
                    warn!(
                        aggregate_type = %record.aggregate_type,
                        aggregate_id = %record.aggregate_id,
                        event_type = %record.event_type,
                        attempt = attempt,
                        "plugin notification retry failed: {}",
                        err
                    );

                    state.retry_queue.schedule_retry(
                        record,
                        attempt + 1,
                        state.plugin_max_attempts,
                    );
                }
            }
        }
    });
}

fn backoff_delay(attempt: u32) -> Duration {
    match attempt {
        0 | 1 => Duration::from_secs(1),
        _ => {
            let capped = (attempt - 1).min(6);
            Duration::from_secs(2u64.pow(capped))
        }
    }
}

async fn dispatch_plugin_notification(state: &AppState, record: &EventRecord) -> Result<()> {
    let aggregate_state = state
        .store
        .get_aggregate_state(&record.aggregate_type, &record.aggregate_id)?;
    let schema = state.schemas.get(&record.aggregate_type).ok();
    let plugins = state.plugins.clone();
    let record_clone = record.clone();

    tokio::task::spawn_blocking(move || {
        let schema_ref = schema.as_ref();
        plugins.notify_event(&record_clone, &aggregate_state, schema_ref)
    })
    .await
    .map_err(|err| EventError::Storage(err.to_string()))??;

    Ok(())
}

pub(crate) fn notify_plugins(state: &AppState, record: &EventRecord) {
    if state.plugins.is_empty() {
        return;
    }

    let state_clone = state.clone();
    let record_clone = record.clone();
    tokio::spawn(async move {
        match dispatch_plugin_notification(&state_clone, &record_clone).await {
            Ok(_) => {}
            Err(EventError::AggregateNotFound) => {
                warn!(
                    aggregate_type = %record_clone.aggregate_type,
                    aggregate_id = %record_clone.aggregate_id,
                    event_type = %record_clone.event_type,
                    "plugin notification skipped: aggregate no longer exists"
                );
            }
            Err(err) => {
                warn!(
                    aggregate_type = %record_clone.aggregate_type,
                    aggregate_id = %record_clone.aggregate_id,
                    event_type = %record_clone.event_type,
                    "plugin notification failed: {} (queued for retry)",
                    err
                );
                state_clone.retry_queue.schedule_retry(
                    record_clone,
                    2,
                    state_clone.plugin_max_attempts,
                );
            }
        }
    });
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
