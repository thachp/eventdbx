use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use chrono::SecondsFormat;
use clap::Args;
use json_patch;
use serde_json::{self, Value};

use eventdbx::{
    config::{Config, load_or_default},
    schema_history::{PublishOptions, SchemaAuditAction, SchemaHistoryManager, SchemaManifest},
    tenant::normalize_tenant_id,
};

use crate::commands::tenant::prepare_remote_client;

#[derive(Args, Clone)]
pub struct SchemaPublishArgs {
    /// Tenant identifier the schema applies to (defaults to active domain)
    #[arg(value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Optional JSON source file to publish; defaults to the tenant's active schemas.json
    #[arg(long = "source", value_name = "PATH")]
    pub source: Option<PathBuf>,

    /// Reason for auditing why this version was published
    #[arg(long = "reason", value_name = "TEXT")]
    pub reason: Option<String>,

    /// Override the actor recorded in the audit log (defaults to current user)
    #[arg(long = "actor", value_name = "NAME")]
    pub actor: Option<String>,

    /// Optional labels to attach to the version (comma-delimited or repeated)
    #[arg(long = "label", value_name = "LABEL", value_delimiter = ',')]
    pub labels: Vec<String>,

    /// Immediately activate the published version
    #[arg(long, default_value_t = false)]
    pub activate: bool,

    /// Always create a version even if the payload hasn't changed
    #[arg(long, default_value_t = false)]
    pub force: bool,

    /// Reload the tenant schema cache even if the version stays inactive
    #[arg(long, default_value_t = false)]
    pub reload: bool,

    /// Skip daemon reload/eviction even when the active version changes
    #[arg(long = "no-reload", default_value_t = false)]
    pub no_reload: bool,
}

#[derive(Args, Clone, Default)]
pub struct SchemaHistoryArgs {
    /// Tenant identifier to inspect (defaults to active domain)
    #[arg(value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,

    /// Include audit log entries
    #[arg(long, default_value_t = false)]
    pub audit: bool,
}

#[derive(Args, Clone, Default)]
pub struct SchemaShowArgs {
    /// Tenant identifier to inspect (defaults to active domain)
    #[arg(value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Version identifier to display (use 'active' or 'latest' for shortcuts)
    #[arg(long = "version", value_name = "VERSION")]
    pub version: Option<String>,
}

#[derive(Args, Clone, Default)]
pub struct SchemaDiffArgs {
    /// Tenant identifier to inspect (defaults to active domain)
    #[arg(value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Base version for the diff (defaults to active)
    #[arg(long = "from", value_name = "VERSION")]
    pub from: Option<String>,

    /// Target version for the diff (defaults to latest)
    #[arg(long = "to", value_name = "VERSION")]
    pub to: Option<String>,

    /// Emit JSON patch output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Clone)]
pub struct SchemaActivateArgs {
    /// Tenant identifier whose schema should change (defaults to active domain)
    #[arg(value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Version identifier to activate (defaults to latest)
    #[arg(long = "version", value_name = "VERSION")]
    pub version: Option<String>,

    /// Reason recorded in the audit log
    #[arg(long = "reason", value_name = "TEXT")]
    pub reason: Option<String>,

    /// Override the audit actor (defaults to current user)
    #[arg(long = "actor", value_name = "NAME")]
    pub actor: Option<String>,

    /// Skip daemon reload/eviction after activation
    #[arg(long = "no-reload", default_value_t = false)]
    pub no_reload: bool,
}

#[derive(Args, Clone)]
pub struct SchemaRollbackArgs {
    /// Tenant identifier whose schema should roll back (defaults to active domain)
    #[arg(value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Version identifier to roll back to (defaults to previous active)
    #[arg(long = "version", value_name = "VERSION")]
    pub version: Option<String>,

    /// Reason recorded in the audit log
    #[arg(long = "reason", value_name = "TEXT")]
    pub reason: Option<String>,

    /// Override the audit actor (defaults to current user)
    #[arg(long = "actor", value_name = "NAME")]
    pub actor: Option<String>,

    /// Skip daemon reload/eviction after rollback
    #[arg(long = "no-reload", default_value_t = false)]
    pub no_reload: bool,
}

#[derive(Args, Clone)]
pub struct SchemaReloadArgs {
    /// Tenant identifier whose schema cache should be refreshed (defaults to active domain)
    #[arg(value_name = "TENANT")]
    pub tenant: Option<String>,
}

pub(crate) fn schema_publish(config_path: Option<PathBuf>, args: SchemaPublishArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = resolve_schema_tenant(&config, args.tenant.as_deref())?;
    let source_path = args
        .source
        .clone()
        .unwrap_or_else(|| config.domain_data_dir_for(&tenant).join("schemas.json"));
    let payload = fs::read_to_string(&source_path)
        .with_context(|| format!("failed to read schema input from {}", source_path.display()))?;

    let actor = resolve_actor(args.actor.as_deref());
    let manager = SchemaHistoryManager::new(config.domain_data_dir_for(&tenant));
    let outcome = manager.publish(PublishOptions {
        schema_json: &payload,
        actor: Some(actor.as_str()),
        reason: args.reason.as_deref(),
        labels: &args.labels,
        activate: args.activate,
        skip_if_identical: !args.force,
    })?;

    if outcome.skipped {
        if outcome.activated {
            println!(
                "tenant={} version={} already existed; re-activated",
                tenant, outcome.version_id
            );
        } else {
            println!(
                "tenant={} no schema changes detected; version={} retained",
                tenant, outcome.version_id
            );
        }
        return Ok(());
    }

    if outcome.activated {
        println!(
            "tenant={} published version={} (activated)",
            tenant, outcome.version_id
        );
    } else {
        println!(
            "tenant={} published version={} (inactive)",
            tenant, outcome.version_id
        );
    }

    let reload_requested = (args.activate || args.reload) && !args.no_reload;
    if reload_requested {
        let reloaded = schema_reload_online(&config, &tenant)?;
        report_schema_reload(&tenant, reloaded);
    } else if args.no_reload && (args.activate || args.reload) {
        println!("tenant={} daemon reload skipped (--no-reload)", tenant);
    }
    Ok(())
}

pub(crate) fn schema_history(config_path: Option<PathBuf>, args: SchemaHistoryArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = resolve_schema_tenant(&config, args.tenant.as_deref())?;
    let manager = SchemaHistoryManager::new(config.domain_data_dir_for(&tenant));
    let manifest = manager.manifest()?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&manifest)?);
        return Ok(());
    }

    if manifest.versions.is_empty() {
        println!("tenant={} has no published schema versions", tenant);
    } else {
        print_schema_history(&tenant, &manifest);
    }

    if args.audit {
        print_schema_audit(&manifest);
    }
    Ok(())
}

pub(crate) fn schema_show(config_path: Option<PathBuf>, args: SchemaShowArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = resolve_schema_tenant(&config, args.tenant.as_deref())?;
    let manager = SchemaHistoryManager::new(config.domain_data_dir_for(&tenant));
    let manifest = manager.manifest()?;
    let version_id =
        resolve_version_spec(&manifest, args.version.as_deref(), VersionFallback::Active)?;
    let payload = manager.version_payload(&version_id)?;
    println!("{payload}");
    Ok(())
}

pub(crate) fn schema_diff(config_path: Option<PathBuf>, args: SchemaDiffArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = resolve_schema_tenant(&config, args.tenant.as_deref())?;
    let manager = SchemaHistoryManager::new(config.domain_data_dir_for(&tenant));
    let manifest = manager.manifest()?;
    if manifest.versions.is_empty() {
        bail!(
            "tenant '{}' has no schema versions available for diffing",
            tenant
        );
    }
    let from_id = resolve_version_spec(&manifest, args.from.as_deref(), VersionFallback::Active)?;
    let to_id = resolve_version_spec(&manifest, args.to.as_deref(), VersionFallback::Latest)?;

    let from_payload = manager.version_payload(&from_id)?;
    let to_payload = manager.version_payload(&to_id)?;

    let from_json: Value = serde_json::from_str(&from_payload)?;
    let to_json: Value = serde_json::from_str(&to_payload)?;
    let patch = json_patch::diff(&from_json, &to_json);
    let patch_value = serde_json::to_value(&patch)?;
    let operations = patch_value.as_array().map(|items| items.len()).unwrap_or(0);

    if args.json {
        println!("{}", serde_json::to_string_pretty(&patch_value)?);
        return Ok(());
    }

    if operations == 0 {
        println!(
            "tenant={} diff from {} to {} has no changes",
            tenant, from_id, to_id
        );
    } else {
        println!(
            "tenant={} diff from {} to {} includes {} operation(s):",
            tenant, from_id, to_id, operations
        );
        println!("{}", serde_json::to_string_pretty(&patch_value)?);
    }
    Ok(())
}

pub(crate) fn schema_reload(config_path: Option<PathBuf>, args: SchemaReloadArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = resolve_schema_tenant(&config, args.tenant.as_deref())?;
    let reloaded = schema_reload_online(&config, &tenant)?;
    report_schema_reload(&tenant, reloaded);
    Ok(())
}

pub(crate) fn schema_activate(
    config_path: Option<PathBuf>,
    args: SchemaActivateArgs,
    action: SchemaAuditAction,
    verb: &str,
) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = resolve_schema_tenant(&config, args.tenant.as_deref())?;
    let manager = SchemaHistoryManager::new(config.domain_data_dir_for(&tenant));
    let manifest = manager.manifest()?;
    if manifest.versions.is_empty() {
        bail!("tenant '{}' has no schema versions to activate", tenant);
    }

    let fallback = match action {
        SchemaAuditAction::Rollback => VersionFallback::PreviousActive,
        _ => VersionFallback::Latest,
    };
    let version_id = resolve_version_spec(&manifest, args.version.as_deref(), fallback)?;
    let actor = resolve_actor(args.actor.as_deref());
    let result = manager.activate_version(
        &version_id,
        Some(actor.as_str()),
        args.reason.as_deref(),
        action,
    )?;

    if result.changed {
        println!("tenant={} version={} {}", tenant, result.version_id, verb);
    } else {
        println!(
            "tenant={} version={} already active; audit entry recorded",
            tenant, result.version_id
        );
    }

    if args.no_reload {
        println!("tenant={} daemon reload skipped (--no-reload)", tenant);
    } else {
        let reloaded = schema_reload_online(&config, &tenant)?;
        report_schema_reload(&tenant, reloaded);
    }
    Ok(())
}

pub(crate) fn schema_rollback(
    config_path: Option<PathBuf>,
    args: SchemaRollbackArgs,
) -> Result<()> {
    schema_activate(
        config_path,
        SchemaActivateArgs {
            tenant: args.tenant,
            version: args.version,
            reason: args.reason,
            actor: args.actor,
            no_reload: args.no_reload,
        },
        SchemaAuditAction::Rollback,
        "rolled back",
    )
}

fn schema_reload_online(config: &Config, tenant: &str) -> Result<bool> {
    if let Ok((client, token)) = prepare_remote_client(config, tenant) {
        if let Ok(reloaded) = client.reload_tenant(&token, tenant) {
            return Ok(reloaded);
        }
    }
    Ok(false)
}

fn report_schema_reload(tenant: &str, reloaded: bool) {
    if reloaded {
        println!("tenant={} schema cache reloaded", tenant);
    } else {
        println!("tenant={} schema cache reload skipped", tenant);
    }
}

pub(crate) fn resolve_schema_tenant(config: &Config, tenant_arg: Option<&str>) -> Result<String> {
    match tenant_arg {
        Some(value) => Ok(normalize_tenant_id(value)?),
        None => Ok(normalize_tenant_id(config.active_domain())?),
    }
}

fn resolve_actor(explicit: Option<&str>) -> String {
    if let Some(value) = explicit {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    env::var("USER")
        .or_else(|_| env::var("USERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[derive(Clone, Copy)]
enum VersionFallback {
    Active,
    Latest,
    PreviousActive,
}

fn resolve_version_spec(
    manifest: &SchemaManifest,
    input: Option<&str>,
    fallback: VersionFallback,
) -> Result<String> {
    let trimmed = input.and_then(|value| {
        let candidate = value.trim();
        if candidate.is_empty() {
            None
        } else {
            Some(candidate)
        }
    });

    if let Some(raw) = trimmed {
        if raw.eq_ignore_ascii_case("active") {
            return manifest
                .active_version
                .clone()
                .ok_or_else(|| anyhow!("no active schema version available"));
        }
        if raw.eq_ignore_ascii_case("latest") {
            return SchemaHistoryManager::latest_version_id(manifest)
                .ok_or_else(|| anyhow!("no schema versions recorded"));
        }
        if manifest.versions.iter().any(|entry| entry.id == raw) {
            return Ok(raw.to_string());
        }
        bail!(
            "schema version '{}' was not found; run `dbx tenant schema history <tenant>` to list versions",
            raw
        );
    }

    match fallback {
        VersionFallback::Active => manifest
            .active_version
            .clone()
            .ok_or_else(|| anyhow!("no active schema version available")),
        VersionFallback::Latest => SchemaHistoryManager::latest_version_id(manifest)
            .ok_or_else(|| anyhow!("no schema versions recorded")),
        VersionFallback::PreviousActive => previous_version_before_active(manifest)
            .ok_or_else(|| anyhow!("no earlier schema version to roll back to")),
    }
}

fn previous_version_before_active(manifest: &SchemaManifest) -> Option<String> {
    let active = manifest.active_version.as_ref()?;
    let mut previous: Option<String> = None;
    for entry in &manifest.versions {
        if &entry.id == active {
            break;
        }
        previous = Some(entry.id.clone());
    }
    previous
}

fn print_schema_history(tenant: &str, manifest: &SchemaManifest) {
    let mut version_width = "version".len();
    let mut created_width = "created_at".len();
    let mut author_width = "author".len();
    let mut labels_width = "labels".len();
    let mut reason_width = "reason".len();

    for entry in &manifest.versions {
        version_width = version_width.max(entry.id.len());
        created_width = created_width.max(
            entry
                .created_at
                .to_rfc3339_opts(SecondsFormat::Secs, true)
                .len(),
        );
        author_width = author_width.max(entry.author.as_deref().unwrap_or("-").len());
        labels_width = labels_width.max(entry.labels.join(",").chars().count().max("-".len()));
        reason_width = reason_width.max(entry.reason.as_deref().unwrap_or("-").len());
    }

    println!(
        "tenant={} versions ( '*' indicates the active version )",
        tenant
    );
    println!(
        " {}  {:version_width$}  {:created_width$}  {:author_width$}  {:labels_width$}  {:reason_width$}",
        " ",
        "version",
        "created_at",
        "author",
        "labels",
        "reason",
        version_width = version_width,
        created_width = created_width,
        author_width = author_width,
        labels_width = labels_width,
        reason_width = reason_width,
    );

    for entry in &manifest.versions {
        let marker = if manifest.active_version.as_deref() == Some(entry.id.as_str()) {
            "*"
        } else {
            " "
        };
        let created_at = entry.created_at.to_rfc3339_opts(SecondsFormat::Secs, true);
        let author = entry.author.as_deref().unwrap_or("-");
        let labels = if entry.labels.is_empty() {
            "-".to_string()
        } else {
            entry.labels.join(",")
        };
        let reason = entry.reason.as_deref().unwrap_or("-");
        println!(
            " {}  {:version_width$}  {:created_width$}  {:author_width$}  {:labels_width$}  {:reason_width$}",
            marker,
            entry.id,
            created_at,
            author,
            labels,
            reason,
            version_width = version_width,
            created_width = created_width,
            author_width = author_width,
            labels_width = labels_width,
            reason_width = reason_width,
        );
    }
}

fn print_schema_audit(manifest: &SchemaManifest) {
    if manifest.audit_log.is_empty() {
        println!("no schema audit entries recorded");
        return;
    }

    let mut time_width = "timestamp".len();
    let mut action_width = "action".len();
    let mut version_width = "version".len();
    let mut actor_width = "actor".len();
    let mut reason_width = "reason".len();
    let mut details_width = "details".len();

    for entry in &manifest.audit_log {
        time_width = time_width.max(
            entry
                .timestamp
                .to_rfc3339_opts(SecondsFormat::Secs, true)
                .len(),
        );
        action_width = action_width.max(entry.action.len());
        version_width = version_width.max(entry.version.len());
        actor_width = actor_width.max(entry.actor.as_deref().unwrap_or("-").len());
        reason_width = reason_width.max(entry.reason.as_deref().unwrap_or("-").len());
        details_width = details_width.max(entry.details.as_deref().unwrap_or("-").len());
    }

    println!("audit log:");
    println!(
        " {:time_width$}  {:action_width$}  {:version_width$}  {:actor_width$}  {:reason_width$}  {:details_width$}",
        "timestamp",
        "action",
        "version",
        "actor",
        "reason",
        "details",
        time_width = time_width,
        action_width = action_width,
        version_width = version_width,
        actor_width = actor_width,
        reason_width = reason_width,
        details_width = details_width,
    );

    for entry in &manifest.audit_log {
        let timestamp = entry.timestamp.to_rfc3339_opts(SecondsFormat::Secs, true);
        let actor = entry.actor.as_deref().unwrap_or("-");
        let reason = entry.reason.as_deref().unwrap_or("-");
        let details = entry.details.as_deref().unwrap_or("-");
        println!(
            " {:time_width$}  {:action_width$}  {:version_width$}  {:actor_width$}  {:reason_width$}  {:details_width$}",
            timestamp,
            entry.action,
            entry.version,
            actor,
            reason,
            details,
            time_width = time_width,
            action_width = action_width,
            version_width = version_width,
            actor_width = actor_width,
            reason_width = reason_width,
            details_width = details_width,
        );
    }
}
