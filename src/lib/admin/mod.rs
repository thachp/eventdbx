use std::collections::BTreeMap;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use chrono::Utc;
use serde::Deserialize;

use crate::{
    config::{AdminApiConfig, Config, PluginConfig, PluginDefinition, RemoteConfig},
    error::{EventError, Result},
    plugin::PluginManager,
    replication::ReplicationManager,
    schema::{AggregateSchema, CreateSchemaInput, SchemaUpdate},
    server::{AppState, extract_bearer_token},
    token::{IssueTokenInput, TokenRecord},
};

const ADMIN_KEY_HEADER: &str = "x-admin-key";

pub(crate) fn build_router(state: AppState, _config: AdminApiConfig) -> Router {
    let auth_state = state.clone();

    let routes = Router::new()
        .route("/tokens", get(list_tokens).post(issue_token))
        .route("/tokens/{token}/revoke", post(revoke_token))
        .route("/tokens/{token}/refresh", post(refresh_token))
        .route("/schemas", get(list_schemas).post(create_schema))
        .route("/schemas/{aggregate}", get(get_schema).patch(update_schema))
        .route(
            "/schemas/{aggregate}/events/{event}",
            delete(remove_schema_event),
        )
        .route("/remotes", get(list_remotes))
        .route("/remotes/{name}", put(upsert_remote).delete(delete_remote))
        .route("/plugins", get(list_plugins))
        .route("/plugins/{name}", put(put_plugin).delete(delete_plugin))
        .route("/plugins/{name}/enable", post(enable_plugin))
        .route("/plugins/{name}/disable", post(disable_plugin))
        .with_state(state);

    let routes = routes.layer(middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let state = auth_state.clone();
            async move { authorize_admin(state, request, next).await }
        },
    ));

    Router::new().nest("/admin", routes)
}

async fn authorize_admin(state: AppState, request: Request<Body>, next: Next) -> Response {
    match check_admin_auth(&state, request.headers()) {
        Ok(true) => next.run(request).await,
        Ok(false) => EventError::Unauthorized.into_response(),
        Err(err) => err.into_response(),
    }
}

fn check_admin_auth(state: &AppState, headers: &HeaderMap) -> Result<bool> {
    let Some(candidate) = extract_admin_key(headers) else {
        return Ok(false);
    };

    let guard = state.config.read().expect("admin config lock poisoned");
    if !guard.admin_master_key_configured() {
        return Ok(false);
    }

    guard.verify_admin_master_key(&candidate)
}

fn extract_admin_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(ADMIN_KEY_HEADER) {
        if let Ok(key) = value.to_str() {
            let trimmed = key.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    extract_bearer_token(headers)
}

async fn list_tokens(State(state): State<AppState>) -> Result<Json<Vec<TokenRecord>>> {
    Ok(Json(state.tokens.list()))
}

#[derive(Debug, Deserialize)]
struct IssueTokenRequest {
    group: String,
    user: String,
    #[serde(default)]
    expiration_secs: Option<u64>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    keep_alive: bool,
}

async fn issue_token(
    State(state): State<AppState>,
    Json(request): Json<IssueTokenRequest>,
) -> Result<(StatusCode, Json<TokenRecord>)> {
    let record = state.tokens.issue(IssueTokenInput {
        group: request.group,
        user: request.user,
        expiration_secs: request.expiration_secs,
        limit: request.limit,
        keep_alive: request.keep_alive,
    })?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn revoke_token(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<StatusCode> {
    state.tokens.revoke(&token)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct RefreshTokenRequest {
    #[serde(default)]
    expiration_secs: Option<u64>,
    #[serde(default)]
    limit: Option<u64>,
}

async fn refresh_token(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Json(request): Json<RefreshTokenRequest>,
) -> Result<Json<TokenRecord>> {
    let record = state
        .tokens
        .refresh(&token, request.expiration_secs, request.limit)?;
    Ok(Json(record))
}

async fn list_schemas(State(state): State<AppState>) -> Result<Json<Vec<AggregateSchema>>> {
    Ok(Json(state.schemas.list()))
}

async fn get_schema(
    State(state): State<AppState>,
    Path(aggregate): Path<String>,
) -> Result<Json<AggregateSchema>> {
    Ok(Json(state.schemas.get(&aggregate)?))
}

#[derive(Debug, Deserialize)]
struct CreateSchemaRequest {
    aggregate: String,
    events: Vec<String>,
    #[serde(default)]
    snapshot_threshold: Option<u64>,
}

async fn create_schema(
    State(state): State<AppState>,
    Json(request): Json<CreateSchemaRequest>,
) -> Result<(StatusCode, Json<AggregateSchema>)> {
    let schema = state.schemas.create(CreateSchemaInput {
        aggregate: request.aggregate,
        events: request.events,
        snapshot_threshold: request.snapshot_threshold,
    })?;
    Ok((StatusCode::CREATED, Json(schema)))
}

#[derive(Debug, Deserialize)]
struct FieldLockRequest {
    field: String,
    lock: bool,
}

#[derive(Debug, Default, Deserialize)]
struct SchemaUpdateRequest {
    #[serde(default)]
    snapshot_threshold: Option<Option<u64>>,
    #[serde(default)]
    locked: Option<bool>,
    #[serde(default)]
    field_lock: Option<FieldLockRequest>,
    #[serde(default)]
    add_fields: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    remove_fields: Option<BTreeMap<String, Vec<String>>>,
}

async fn update_schema(
    State(state): State<AppState>,
    Path(aggregate): Path<String>,
    Json(request): Json<SchemaUpdateRequest>,
) -> Result<Json<AggregateSchema>> {
    let mut update = SchemaUpdate::default();
    if let Some(snapshot) = request.snapshot_threshold {
        update.snapshot_threshold = Some(snapshot);
    }
    if let Some(locked) = request.locked {
        update.locked = Some(locked);
    }
    if let Some(field_lock) = request.field_lock {
        update.field_lock = Some((field_lock.field, field_lock.lock));
    }
    if let Some(mut additions) = request.add_fields {
        update.event_add_fields.append(&mut additions);
    }
    if let Some(mut removals) = request.remove_fields {
        update.event_remove_fields.append(&mut removals);
    }
    let schema = state.schemas.update(&aggregate, update)?;
    Ok(Json(schema))
}

async fn remove_schema_event(
    State(state): State<AppState>,
    Path((aggregate, event)): Path<(String, String)>,
) -> Result<Json<AggregateSchema>> {
    let schema = state.schemas.remove_event(&aggregate, &event)?;
    Ok(Json(schema))
}

async fn list_remotes(
    State(state): State<AppState>,
) -> Result<Json<BTreeMap<String, RemoteConfig>>> {
    let remotes = state
        .config
        .read()
        .expect("config lock poisoned")
        .remotes
        .clone();
    Ok(Json(remotes))
}

#[derive(Debug, Deserialize)]
struct RemoteUpsertRequest {
    endpoint: String,
    public_key: String,
}

async fn upsert_remote(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<RemoteUpsertRequest>,
) -> Result<(StatusCode, Json<RemoteConfig>)> {
    let normalized_name = name.trim();
    if normalized_name.is_empty() {
        return Err(EventError::Config("remote name cannot be empty".into()));
    }

    let (status, remote) = mutate_remotes(&state, |config| {
        let was_new = !config.remotes.contains_key(normalized_name);
        let remote = RemoteConfig {
            endpoint: request.endpoint.trim().to_string(),
            public_key: request.public_key.trim().to_string(),
        };
        if remote.endpoint.is_empty() {
            return Err(EventError::Config("remote endpoint cannot be empty".into()));
        }
        if remote.public_key.is_empty() {
            return Err(EventError::Config(
                "remote public key cannot be empty".into(),
            ));
        }
        config
            .remotes
            .insert(normalized_name.to_string(), remote.clone());
        let status = if was_new {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        };
        Ok((status, remote))
    })?;

    Ok((status, Json(remote)))
}

async fn delete_remote(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    mutate_remotes(&state, |config| {
        let removed = config.remotes.remove(name.trim());
        match removed {
            Some(_) => Ok(()),
            None => Err(EventError::Config(format!(
                "remote '{}' is not configured",
                name
            ))),
        }
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_plugins(State(state): State<AppState>) -> Result<Json<Vec<PluginDefinition>>> {
    let definitions = snapshot_plugins(&state)?;
    Ok(Json(definitions))
}

#[derive(Debug, Deserialize)]
struct PluginUpsertRequest {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    name: Option<String>,
    config: PluginConfig,
}

async fn put_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<PluginUpsertRequest>,
) -> Result<(StatusCode, Json<PluginDefinition>)> {
    let normalized_name = name.trim();
    if normalized_name.is_empty() {
        return Err(EventError::Config("plugin name cannot be empty".into()));
    }

    if let Some(ref provided_name) = request.name {
        let provided = provided_name.trim();
        if !provided.is_empty() && provided != normalized_name {
            return Err(EventError::Config(format!(
                "plugin name '{}' does not match path parameter '{}'",
                provided, normalized_name
            )));
        }
    }

    let (status, definition) = update_plugins(&state, |definitions| {
        let enabled = request.enabled.unwrap_or(true);
        let definition = PluginDefinition {
            enabled,
            name: Some(normalized_name.to_string()),
            config: request.config.clone(),
        };

        if let Some(existing) = definitions
            .iter_mut()
            .find(|item| item.name.as_deref() == Some(normalized_name))
        {
            *existing = definition.clone();
            Ok((StatusCode::OK, definition))
        } else {
            definitions.push(definition.clone());
            Ok((StatusCode::CREATED, definition))
        }
    })?;

    Ok((status, Json(definition)))
}

async fn delete_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    update_plugins(&state, |definitions| {
        let initial_len = definitions.len();
        definitions.retain(|item| item.name.as_deref() != Some(name.trim()));
        if definitions.len() == initial_len {
            return Err(EventError::Config(format!(
                "plugin '{}' is not configured",
                name
            )));
        }
        Ok(())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn enable_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PluginDefinition>> {
    let definition = update_plugins(&state, |definitions| {
        let Some(existing) = definitions
            .iter_mut()
            .find(|item| item.name.as_deref() == Some(name.trim()))
        else {
            return Err(EventError::Config(format!(
                "plugin '{}' is not configured",
                name
            )));
        };
        existing.enabled = true;
        Ok(existing.clone())
    })?;
    Ok(Json(definition))
}

async fn disable_plugin(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PluginDefinition>> {
    let definition = update_plugins(&state, |definitions| {
        let Some(existing) = definitions
            .iter_mut()
            .find(|item| item.name.as_deref() == Some(name.trim()))
        else {
            return Err(EventError::Config(format!(
                "plugin '{}' is not configured",
                name
            )));
        };
        existing.enabled = false;
        Ok(existing.clone())
    })?;
    Ok(Json(definition))
}

fn snapshot_plugins(state: &AppState) -> Result<Vec<PluginDefinition>> {
    let config = state.config.read().expect("config lock poisoned");
    load_plugin_definitions(&config)
}

fn load_plugin_definitions(config: &Config) -> Result<Vec<PluginDefinition>> {
    let mut definitions = config.load_plugins()?;
    if definitions.is_empty() && !config.plugins.is_empty() {
        definitions = config.plugins.clone();
    }
    Ok(definitions)
}

fn update_plugins<F, R>(state: &AppState, mutator: F) -> Result<R>
where
    F: FnOnce(&mut Vec<PluginDefinition>) -> Result<R>,
{
    let (result, manager) = {
        let mut config = state.config.write().expect("config lock poisoned");
        let mut definitions = load_plugin_definitions(&config)?;
        let result = mutator(&mut definitions)?;
        config.plugins = definitions.clone();
        config.updated_at = Utc::now();
        config.save_plugins(&definitions)?;
        let manager = PluginManager::from_config(&config)?;
        (result, manager)
    };

    state.replace_plugins(manager);
    Ok(result)
}

fn mutate_remotes<F, R>(state: &AppState, mutator: F) -> Result<R>
where
    F: FnOnce(&mut Config) -> Result<R>,
{
    let (result, manager) = {
        let mut config = state.config.write().expect("config lock poisoned");
        let result = mutator(&mut config)?;
        config.updated_at = Utc::now();
        config.save(state.config_path.as_ref())?;
        let manager = if config.remotes.is_empty() {
            None
        } else {
            Some(ReplicationManager::from_config(&config))
        };
        (result, manager)
    };

    state.set_replication_manager(manager);
    Ok(result)
}
