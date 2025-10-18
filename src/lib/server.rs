use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

use super::{
    config::Config,
    error::{EventError, Result},
    plugin::PluginManager,
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
        list_page_size: config.list_page_size,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/aggregates", get(list_aggregates))
        .route(
            "/v1/aggregates/:aggregate_type/:aggregate_id",
            get(get_aggregate),
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
    let take = params.take.or(Some(state.list_page_size));
    let aggregates = state.store.aggregates_paginated(skip, take);
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

fn notify_plugins(state: &AppState, record: &EventRecord) {
    let plugins = state.plugins.clone();
    if plugins.is_empty() {
        return;
    }

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
