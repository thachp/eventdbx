use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

use super::{
    config::Config,
    error::{EventfulError, Result},
    plugin::PluginManager,
    schema::{AggregateSchema, SchemaManager},
    store::{AggregateState, AppendEvent, EventRecord, EventStore},
    token::{AccessKind, TokenManager},
};

#[derive(Clone)]
struct AppState {
    store: Arc<EventStore>,
    tokens: Arc<TokenManager>,
    schemas: Arc<SchemaManager>,
    restrict: bool,
    plugins: PluginManager,
}

pub async fn run(config: Config, plugins: PluginManager) -> Result<()> {
    let store = Arc::new(EventStore::open(config.event_store_path())?);
    let tokens = Arc::new(TokenManager::load(config.tokens_path())?);
    let schemas = Arc::new(SchemaManager::load(config.schema_store_path())?);
    let state = AppState {
        store,
        tokens,
        schemas,
        restrict: config.restrict,
        plugins,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/aggregates", get(list_aggregates))
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id",
            get(get_aggregate),
        )
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id/events",
            post(append_event).get(list_events),
        )
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id/events/recent",
            get(list_recent_events),
        )
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id/verify",
            get(verify_aggregate),
        )
        .route("/v1/schemas", get(list_schemas))
        .route("/v1/schemas/:aggregate", get(get_schema))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!(
        "Starting EventDB server on {addr} (restrict={})",
        config.restrict
    );

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| EventfulError::Storage(err.to_string()))?;

    Ok(())
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
}

async fn list_aggregates(State(state): State<AppState>) -> Result<Json<Vec<AggregateState>>> {
    Ok(Json(state.store.aggregates()))
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

async fn list_events(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
) -> Result<Json<Vec<EventRecord>>> {
    let events = state.store.list_events(&aggregate_type, &aggregate_id)?;
    Ok(Json(events))
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    take: Option<usize>,
}

async fn list_recent_events(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
    Query(params): Query<EventsQuery>,
) -> Result<Json<Vec<EventRecord>>> {
    let mut events = state.store.list_events(&aggregate_type, &aggregate_id)?;

    let take = params.take.unwrap_or(10);
    if take == 0 {
        return Ok(Json(Vec::new()));
    }

    if events.len() > take {
        events = events.split_off(events.len() - take);
    }
    events.reverse();

    Ok(Json(events))
}

#[derive(Deserialize)]
struct AppendEventRequest {
    event_type: String,
    payload: BTreeMap<String, String>,
}

async fn append_event(
    State(state): State<AppState>,
    Path((aggregate_type, aggregate_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<AppendEventRequest>,
) -> Result<Json<EventRecord>> {
    let token = extract_bearer_token(&headers).ok_or(EventfulError::Unauthorized)?;
    let claims = state.tokens.authorize(&token, AccessKind::Write)?.into();

    if state.restrict {
        state
            .schemas
            .validate_event(&aggregate_type, &request.event_type, &request.payload)?;
    }

    let record = state.store.append(AppendEvent {
        aggregate_type,
        aggregate_id,
        event_type: request.event_type,
        payload: request.payload,
        issued_by: Some(claims),
    })?;

    let plugins = state.plugins.clone();
    if !plugins.is_empty() {
        match state
            .store
            .get_aggregate_state(&record.aggregate_type, &record.aggregate_id)
        {
            Ok(current_state) => {
                let schema = state.schemas.get(&record.aggregate_type).ok();
                let record_clone = record.clone();
                tokio::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let schema_ref = schema.as_ref();
                        if let Err(err) =
                            plugins.notify_event(&record_clone, &current_state, schema_ref)
                        {
                            tracing::error!("plugin notification failed: {}", err);
                        }
                    })
                    .await;

                    if let Err(err) = result {
                        tracing::error!("plugin task join error: {}", err);
                    }
                });
            }
            Err(err) => {
                tracing::error!(
                    "failed to load aggregate state for plugin notification: {}",
                    err
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
    let merkle_root = state.store.verify(&aggregate_type, &aggregate_id)?;
    Ok(Json(VerifyResponse { merkle_root }))
}

#[derive(Serialize)]
struct VerifyResponse {
    merkle_root: String,
}

async fn list_schemas(State(state): State<AppState>) -> Result<Json<Vec<AggregateSchema>>> {
    Ok(Json(state.schemas.list()))
}

async fn get_schema(
    State(state): State<AppState>,
    Path(aggregate): Path<String>,
) -> Result<Json<AggregateSchema>> {
    let schema = state.schemas.get(&aggregate)?;
    Ok(Json(schema))
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
