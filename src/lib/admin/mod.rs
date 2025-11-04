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
    config::{AdminApiConfig, PluginConfig, PluginDefinition},
    error::{EventError, Result},
    schema::{AggregateSchema, SchemaUpdate},
    server::{AppState, extract_bearer_token, run_cli_command, run_cli_json},
    token::{
        IssueTokenInput, JwtLimits, ROOT_ACTION, ROOT_RESOURCE, RevokeTokenInput, TokenRecord,
    },
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
    let Some(token) = extract_admin_token(headers) else {
        return Ok(false);
    };

    let manager = state.tokens();
    match manager.authorize_action(&token, ROOT_ACTION, Some(ROOT_RESOURCE)) {
        Ok(_) => Ok(true),
        Err(EventError::Unauthorized)
        | Err(EventError::InvalidToken)
        | Err(EventError::TokenExpired) => Ok(false),
        Err(err) => Err(err),
    }
}

fn extract_admin_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(ADMIN_KEY_HEADER) {
        if let Ok(token) = value.to_str() {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    extract_bearer_token(headers)
}

async fn list_tokens(State(state): State<AppState>) -> Result<Json<Vec<TokenRecord>>> {
    let manager = state.tokens();
    let records = manager.list()?;
    Ok(Json(records))
}

#[derive(Debug, Deserialize)]
struct IssueTokenRequest {
    group: String,
    user: String,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    actions: Vec<String>,
    #[serde(default)]
    resources: Vec<String>,
    #[serde(default)]
    ttl_secs: Option<u64>,
    #[serde(default)]
    issued_by: Option<String>,
    #[serde(default)]
    write_limit: Option<u64>,
    #[serde(default)]
    keep_alive: bool,
}

async fn issue_token(
    State(state): State<AppState>,
    Json(request): Json<IssueTokenRequest>,
) -> Result<(StatusCode, Json<TokenRecord>)> {
    let manager = state.tokens();
    let IssueTokenRequest {
        group,
        user,
        subject,
        actions,
        mut resources,
        ttl_secs,
        issued_by,
        write_limit,
        keep_alive,
    } = request;

    if actions.is_empty() {
        return Err(EventError::Config(
            "actions must be provided when issuing a token".to_string(),
        ));
    }

    if resources.is_empty() {
        resources.push("*".to_string());
    }

    let record = manager.issue(IssueTokenInput {
        subject: subject.unwrap_or_else(|| format!("{}:{}", group, user)),
        group,
        user,
        actions,
        resources,
        ttl_secs,
        not_before: None,
        issued_by: issued_by.unwrap_or_else(|| "admin".to_string()),
        limits: JwtLimits {
            write_events: write_limit,
            keep_alive,
        },
    })?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn revoke_token(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<StatusCode> {
    let manager = state.tokens();
    manager.revoke(RevokeTokenInput {
        token_or_id: token,
        reason: None,
    })?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct RefreshTokenRequest {
    #[serde(default)]
    ttl_secs: Option<u64>,
}

async fn refresh_token(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Json(request): Json<RefreshTokenRequest>,
) -> Result<Json<TokenRecord>> {
    let manager = state.tokens();
    let record = manager.refresh(&token, request.ttl_secs)?;
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

async fn list_plugins(State(_state): State<AppState>) -> Result<Json<Vec<PluginDefinition>>> {
    let definitions = fetch_plugins().await?;
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

#[derive(Deserialize)]
struct ConfiguredPluginInfo {
    definition: PluginDefinition,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PluginListPayload {
    Simple(Vec<PluginDefinition>),
    Envelope {
        configured: Vec<ConfiguredPluginInfo>,
    },
}

async fn fetch_plugins() -> Result<Vec<PluginDefinition>> {
    let payload: PluginListPayload = run_cli_json(vec![
        "plugin".to_string(),
        "list".to_string(),
        "--json".to_string(),
    ])
    .await?;
    let configured = match payload {
        PluginListPayload::Simple(definitions) => definitions,
        PluginListPayload::Envelope { configured } => {
            configured.into_iter().map(|info| info.definition).collect()
        }
    };
    Ok(configured)
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
        PluginConfig::Tcp(cfg) => {
            args.push("tcp".to_string());
            args.push("--name".to_string());
            args.push(name.to_string());
            args.push("--host".to_string());
            args.push(cfg.host.clone());
            args.push("--port".to_string());
            args.push(cfg.port.to_string());
        }
        PluginConfig::Capnp(cfg) => {
            args.push("capnp".to_string());
            args.push("--name".to_string());
            args.push(name.to_string());
            args.push("--host".to_string());
            args.push(cfg.host.clone());
            args.push("--port".to_string());
            args.push(cfg.port.to_string());
        }
        PluginConfig::Process(_) => {
            return Err(EventError::Config(
                "process plugins must be managed via the CLI".into(),
            ));
        }
    }

    if matches!(request.enabled, Some(false)) {
        args.push("--disable".to_string());
    }

    Ok(args)
}
