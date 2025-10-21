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
use serde::Deserialize;

use crate::{
    config::{AdminApiConfig, PluginConfig, PluginDefinition, RemoteConfig},
    error::{EventError, Result},
    schema::{AggregateSchema, SchemaUpdate},
    server::{AppState, extract_bearer_token, run_cli_command, run_cli_json},
    token::TokenRecord,
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

    let config = state.config();
    let guard = config.read().expect("admin config lock poisoned");
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

async fn list_tokens(State(_state): State<AppState>) -> Result<Json<Vec<TokenRecord>>> {
    let tokens: Vec<TokenRecord> = run_cli_json(vec![
        "token".to_string(),
        "list".to_string(),
        "--json".to_string(),
    ])
    .await?;
    Ok(Json(tokens))
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
    State(_state): State<AppState>,
    Json(request): Json<IssueTokenRequest>,
) -> Result<(StatusCode, Json<TokenRecord>)> {
    let mut args = vec![
        "token".to_string(),
        "generate".to_string(),
        "--group".to_string(),
        request.group.clone(),
        "--user".to_string(),
        request.user.clone(),
    ];
    if let Some(expiration) = request.expiration_secs {
        args.push("--expiration".to_string());
        args.push(expiration.to_string());
    }
    if let Some(limit) = request.limit {
        args.push("--limit".to_string());
        args.push(limit.to_string());
    }
    if request.keep_alive {
        args.push("--keep-alive".to_string());
    }
    args.push("--json".to_string());

    let record: TokenRecord = run_cli_json(args).await?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn revoke_token(
    State(_state): State<AppState>,
    Path(token): Path<String>,
) -> Result<StatusCode> {
    run_cli_command(vec!["token".to_string(), "revoke".to_string(), token]).await?;
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
    State(_state): State<AppState>,
    Path(token): Path<String>,
    Json(request): Json<RefreshTokenRequest>,
) -> Result<Json<TokenRecord>> {
    let mut args = vec![
        "token".to_string(),
        "refresh".to_string(),
        "--token".to_string(),
        token,
    ];
    if let Some(expiration) = request.expiration_secs {
        args.push("--expiration".to_string());
        args.push(expiration.to_string());
    }
    if let Some(limit) = request.limit {
        args.push("--limit".to_string());
        args.push(limit.to_string());
    }
    args.push("--json".to_string());
    let record: TokenRecord = run_cli_json(args).await?;
    Ok(Json(record))
}

async fn list_schemas(State(_state): State<AppState>) -> Result<Json<Vec<AggregateSchema>>> {
    let schemas: Vec<AggregateSchema> = run_cli_json(vec![
        "schema".to_string(),
        "list".to_string(),
        "--json".to_string(),
    ])
    .await?;
    Ok(Json(schemas))
}

async fn get_schema(
    State(_state): State<AppState>,
    Path(aggregate): Path<String>,
) -> Result<Json<AggregateSchema>> {
    let schema: AggregateSchema = run_cli_json(vec!["schema".to_string(), aggregate]).await?;
    Ok(Json(schema))
}

#[derive(Debug, Deserialize)]
struct CreateSchemaRequest {
    aggregate: String,
    events: Vec<String>,
    #[serde(default)]
    snapshot_threshold: Option<u64>,
}

async fn create_schema(
    State(_state): State<AppState>,
    Json(request): Json<CreateSchemaRequest>,
) -> Result<(StatusCode, Json<AggregateSchema>)> {
    let mut args = vec![
        "schema".to_string(),
        "create".to_string(),
        "--aggregate".to_string(),
        request.aggregate.clone(),
        "--events".to_string(),
        request.events.join(","),
    ];
    if let Some(threshold) = request.snapshot_threshold {
        args.push("--snapshot-threshold".to_string());
        args.push(threshold.to_string());
    }
    args.push("--json".to_string());

    let schema: AggregateSchema = run_cli_json(args).await?;
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
    let schema = state.schemas().update(&aggregate, update)?;
    Ok(Json(schema))
}

async fn remove_schema_event(
    State(state): State<AppState>,
    Path((aggregate, event)): Path<(String, String)>,
) -> Result<Json<AggregateSchema>> {
    let schema = state.schemas().remove_event(&aggregate, &event)?;
    Ok(Json(schema))
}

async fn list_remotes(
    State(_state): State<AppState>,
) -> Result<Json<BTreeMap<String, RemoteConfig>>> {
    let remotes: BTreeMap<String, RemoteConfig> = run_cli_json(vec![
        "remote".to_string(),
        "ls".to_string(),
        "--json".to_string(),
    ])
    .await?;
    Ok(Json(remotes))
}

#[derive(Debug, Deserialize)]
struct RemoteUpsertRequest {
    endpoint: String,
    public_key: String,
}

async fn upsert_remote(
    State(_state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<RemoteUpsertRequest>,
) -> Result<(StatusCode, Json<RemoteConfig>)> {
    let normalized_name = name.trim();
    if normalized_name.is_empty() {
        return Err(EventError::Config("remote name cannot be empty".into()));
    }
    let endpoint = request.endpoint.trim();
    if endpoint.is_empty() {
        return Err(EventError::Config("remote endpoint cannot be empty".into()));
    }
    let public_key = request.public_key.trim();
    if public_key.is_empty() {
        return Err(EventError::Config(
            "remote public key cannot be empty".into(),
        ));
    }

    let existing: BTreeMap<String, RemoteConfig> = run_cli_json(vec![
        "remote".to_string(),
        "ls".to_string(),
        "--json".to_string(),
    ])
    .await?;
    let existed = existing.contains_key(normalized_name);

    let args = vec![
        "remote".to_string(),
        "add".to_string(),
        normalized_name.to_string(),
        endpoint.to_string(),
        "--public-key".to_string(),
        public_key.to_string(),
        "--replace".to_string(),
    ];
    run_cli_command(args).await?;

    let remotes: BTreeMap<String, RemoteConfig> = run_cli_json(vec![
        "remote".to_string(),
        "ls".to_string(),
        "--json".to_string(),
    ])
    .await?;
    let remote = remotes.get(normalized_name).cloned().ok_or_else(|| {
        EventError::Config(format!("remote '{}' is not configured", normalized_name))
    })?;

    let status = if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };

    Ok((status, Json(remote)))
}

async fn delete_remote(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    run_cli_command(vec![
        "remote".to_string(),
        "rm".to_string(),
        name.trim().to_string(),
    ])
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_plugins(State(_state): State<AppState>) -> Result<Json<Vec<PluginDefinition>>> {
    let definitions: Vec<PluginDefinition> = run_cli_json(vec![
        "plugin".to_string(),
        "list".to_string(),
        "--json".to_string(),
    ])
    .await?;
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
    State(_state): State<AppState>,
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

    let existing_plugins = fetch_plugins().await?;
    let existed = existing_plugins
        .iter()
        .any(|plugin| plugin.name.as_deref() == Some(normalized_name));

    let args = build_plugin_config_args(normalized_name, &request)?;
    run_cli_command(args).await?;

    if let Some(enabled) = request.enabled {
        let command = if enabled { "enable" } else { "disable" };
        run_cli_command(vec![
            "plugin".to_string(),
            command.to_string(),
            normalized_name.to_string(),
        ])
        .await?;
    }

    let plugins = fetch_plugins().await?;
    let definition = plugins
        .into_iter()
        .find(|plugin| plugin.name.as_deref() == Some(normalized_name))
        .ok_or_else(|| {
            EventError::Config(format!("plugin '{}' is not configured", normalized_name))
        })?;

    let status = if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };

    Ok((status, Json(definition)))
}

async fn delete_plugin(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    run_cli_command(vec![
        "plugin".to_string(),
        "remove".to_string(),
        name.trim().to_string(),
    ])
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn enable_plugin(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PluginDefinition>> {
    run_cli_command(vec![
        "plugin".to_string(),
        "enable".to_string(),
        name.trim().to_string(),
    ])
    .await?;
    let plugins = fetch_plugins().await?;
    let definition = plugins
        .into_iter()
        .find(|plugin| plugin.name.as_deref() == Some(name.trim()))
        .ok_or_else(|| EventError::Config(format!("plugin '{}' is not configured", name)))?;
    Ok(Json(definition))
}

async fn disable_plugin(
    State(_state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PluginDefinition>> {
    run_cli_command(vec![
        "plugin".to_string(),
        "disable".to_string(),
        name.trim().to_string(),
    ])
    .await?;
    let plugins = fetch_plugins().await?;
    let definition = plugins
        .into_iter()
        .find(|plugin| plugin.name.as_deref() == Some(name.trim()))
        .ok_or_else(|| EventError::Config(format!("plugin '{}' is not configured", name)))?;
    Ok(Json(definition))
}

async fn fetch_plugins() -> Result<Vec<PluginDefinition>> {
    run_cli_json(vec![
        "plugin".to_string(),
        "list".to_string(),
        "--json".to_string(),
    ])
    .await
}

fn build_plugin_config_args(name: &str, request: &PluginUpsertRequest) -> Result<Vec<String>> {
    let mut args = vec!["plugin".to_string(), "config".to_string()];
    match &request.config {
        PluginConfig::Log(cfg) => {
            args.push("log".to_string());
            args.push("--name".to_string());
            args.push(name.to_string());
            args.push("--level".to_string());
            args.push(cfg.level.clone());
            if let Some(template) = &cfg.template {
                args.push("--template".to_string());
                args.push(template.clone());
            }
        }
        PluginConfig::Http(cfg) => {
            args.push("http".to_string());
            args.push("--name".to_string());
            args.push(name.to_string());
            args.push("--endpoint".to_string());
            args.push(cfg.endpoint.clone());
            if cfg.https {
                args.push("--https".to_string());
            }
            for (key, value) in &cfg.headers {
                args.push("--header".to_string());
                args.push(format!("{}={}", key, value));
            }
        }
        PluginConfig::Grpc(cfg) => {
            args.push("grpc".to_string());
            args.push("--name".to_string());
            args.push(name.to_string());
            args.push("--endpoint".to_string());
            args.push(cfg.endpoint.clone());
        }
        PluginConfig::Tcp(cfg) => {
            args.push("tcp".to_string());
            args.push("--name".to_string());
            args.push(name.to_string());
            args.push("--host".to_string());
            args.push(cfg.host.clone());
            args.push("--port".to_string());
            args.push(cfg.port.to_string());
        }
    }

    if matches!(request.enabled, Some(false)) {
        args.push("--disable".to_string());
    }

    Ok(args)
}
