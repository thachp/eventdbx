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
    root: bool,
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
        root,
        actions,
        mut resources,
        ttl_secs,
        issued_by,
        write_limit,
        keep_alive,
    } = request;

    if !root && actions.is_empty() {
        return Err(EventError::Config(
            "actions must be provided for non-root tokens".to_string(),
        ));
    }

    if resources.is_empty() {
        resources.push("*".to_string());
    }

    let record = manager.issue(IssueTokenInput {
        subject: subject.unwrap_or_else(|| format!("{}:{}", group, user)),
        group,
        user,
        root,
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

    let (ip, port) = parse_remote_endpoint(endpoint)?;

    let existing: BTreeMap<String, RemoteConfig> = run_cli_json(vec![
        "remote".to_string(),
        "ls".to_string(),
        "--json".to_string(),
    ])
    .await?;
    let existed = existing.contains_key(normalized_name);

    let mut args = vec![
        "remote".to_string(),
        "add".to_string(),
        normalized_name.to_string(),
        ip,
        "--public-key".to_string(),
        public_key.to_string(),
    ];
    if let Some(port) = port {
        args.push("--port".to_string());
        args.push(port.to_string());
    }
    args.push("--replace".to_string());
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

fn parse_remote_endpoint(endpoint: &str) -> Result<(String, Option<u16>)> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err(EventError::Config(
            "remote endpoint cannot be empty".to_string(),
        ));
    }

    let without_scheme = if let Some((scheme, rest)) = trimmed.split_once("://") {
        if !scheme.eq_ignore_ascii_case("tcp") {
            return Err(EventError::Config(format!(
                "remote endpoint scheme must be 'tcp', found '{}'",
                scheme
            )));
        }
        rest
    } else {
        trimmed
    };

    let without_scheme = without_scheme.trim();
    if without_scheme.is_empty() {
        return Err(EventError::Config(
            "remote endpoint host cannot be empty".to_string(),
        ));
    }

    if without_scheme.starts_with('[') {
        let end = without_scheme
            .find(']')
            .ok_or_else(|| EventError::Config("remote endpoint has invalid IPv6 host".into()))?;
        let host = &without_scheme[1..end];
        if host.trim().is_empty() {
            return Err(EventError::Config(
                "remote endpoint host cannot be empty".to_string(),
            ));
        }

        let remainder = without_scheme[end + 1..].trim();
        if remainder.is_empty() {
            return Ok((host.to_string(), None));
        }
        if !remainder.starts_with(':') {
            return Err(EventError::Config(
                "remote endpoint IPv6 port must follow ']'".to_string(),
            ));
        }
        let port = parse_endpoint_port(remainder[1..].trim())?;
        return Ok((host.to_string(), Some(port)));
    }

    if let Some((host, port_str)) = without_scheme.rsplit_once(':') {
        if host.trim().is_empty() {
            return Err(EventError::Config(
                "remote endpoint host cannot be empty".to_string(),
            ));
        }
        if port_str.trim().is_empty() {
            return Err(EventError::Config(
                "remote endpoint port cannot be empty".to_string(),
            ));
        }
        let port = parse_endpoint_port(port_str.trim())?;
        return Ok((host.trim().to_string(), Some(port)));
    }

    Ok((without_scheme.to_string(), None))
}

fn parse_endpoint_port(port: &str) -> Result<u16> {
    let parsed: u16 = port
        .parse()
        .map_err(|err| EventError::Config(format!("remote endpoint port is invalid: {err}")))?;
    if parsed == 0 {
        return Err(EventError::Config(
            "remote endpoint port must be greater than zero".to_string(),
        ));
    }
    Ok(parsed)
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
struct PluginListEnvelope {
    configured: Vec<ConfiguredPluginInfo>,
}

#[derive(Deserialize)]
struct ConfiguredPluginInfo {
    definition: PluginDefinition,
}

async fn fetch_plugins() -> Result<Vec<PluginDefinition>> {
    let envelope: PluginListEnvelope = run_cli_json(vec![
        "plugin".to_string(),
        "list".to_string(),
        "--json".to_string(),
    ])
    .await?;
    Ok(envelope
        .configured
        .into_iter()
        .map(|info| info.definition)
        .collect())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remote_endpoint_supports_tcp_scheme() {
        let (host, port) = parse_remote_endpoint("tcp://0.0.0.0:6363").unwrap();
        assert_eq!(host, "0.0.0.0");
        assert_eq!(port, Some(6363));
    }

    #[test]
    fn parse_remote_endpoint_allows_no_port() {
        let (host, port) = parse_remote_endpoint("tcp://example.com").unwrap();
        assert_eq!(host, "example.com");
        assert!(port.is_none());
    }

    #[test]
    fn parse_remote_endpoint_handles_ipv6() {
        let (host, port) = parse_remote_endpoint("tcp://[2001:db8::1]:6363").unwrap();
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, Some(6363));
    }

    #[test]
    fn parse_remote_endpoint_rejects_non_tcp_scheme() {
        let err = parse_remote_endpoint("udp://10.0.0.1:6363").unwrap_err();
        if let EventError::Config(message) = err {
            assert!(
                message.contains("scheme"),
                "expected scheme error, got {message}"
            );
        } else {
            panic!("expected EventError::Config, got {err:?}");
        }
    }
}
