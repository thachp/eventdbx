use std::{
    collections::VecDeque,
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    net::TcpListener,
    sync::Notify,
    time::{Duration, sleep},
};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use uuid::Uuid;

use super::{
    config::Config,
    error::{EventError, Result},
    plugin::PluginManager,
    replication::{
        ReplicationManager, ReplicationService, proto::replication_server::ReplicationServer,
    },
    schema::SchemaManager,
    store::{self, AggregateState, AppendEvent, EventRecord, EventStore},
    token::{AccessKind, TokenManager},
};

#[derive(Clone)]
struct AppState {
    store: Arc<EventStore>,
    tokens: Arc<TokenManager>,
    schemas: Arc<SchemaManager>,
    restrict: bool,
    plugins: PluginManager,
    list_page_size: usize,
    page_limit: usize,
    retry_queue: Arc<PluginRetryQueue>,
    plugin_max_attempts: u32,
    replication: Option<ReplicationManager>,
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
    let store = Arc::new(EventStore::open(config.event_store_path())?);
    let tokens = Arc::new(TokenManager::load(config.tokens_path())?);
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

    let replication_server = tonic::transport::Server::builder()
        .add_service(ReplicationServer::new(replication_service.clone()))
        .serve_with_shutdown(replication_addr, async {
            shutdown_signal().await;
        });

    tokio::spawn(async move {
        if let Err(err) = replication_server.await {
            tracing::error!("replication server failed: {}", err);
        }
    });
    let state = AppState {
        store,
        tokens,
        schemas,
        restrict: config.restrict,
        plugins,
        list_page_size: config.list_page_size,
        page_limit: config.page_limit,
        retry_queue,
        plugin_max_attempts: config.plugin_max_attempts,
        replication: replication_manager.clone(),
    };

    start_plugin_retry_worker(state.clone());

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/aggregates", get(list_aggregates))
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id",
            get(get_aggregate),
        )
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id/events",
            get(list_events),
        )
        .route("/v1/events", post(append_event_global))
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id/verify",
            get(verify_aggregate),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!(
        "Starting EventDBX server on {addr} (restrict={})",
        config.restrict
    );

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| EventError::Storage(err.to_string()))?;

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
    let aggregates = state.store.aggregates_paginated(skip, Some(take));
    Ok(Json(aggregates))
}

async fn get_aggregate(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<AggregateState>> {
    let aggregate = state
        .store
        .get_aggregate_state(&aggregate_type, &aggregate_id)?;
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

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?;
    let value = value.to_str().ok()?;
    if let Some(token) = value.strip_prefix("Bearer ") {
        Some(token.trim().to_string())
    } else {
        None
    }
}

fn replicate_events(state: &AppState, records: &[EventRecord]) {
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

fn notify_plugins(state: &AppState, record: &EventRecord) {
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
