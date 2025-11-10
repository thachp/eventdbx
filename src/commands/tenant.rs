use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json::{self, Value};

use chrono::SecondsFormat;

use crate::commands::{cli_token, client::ServerClient};
use eventdbx::{
    config::{Config, load_or_default},
    error::EventError,
    schema_history::{PublishOptions, SchemaAuditAction, SchemaHistoryManager, SchemaManifest},
    store::EventStore,
    tenant::{normalize_shard_id, normalize_tenant_id},
    tenant_store::TenantAssignmentStore,
};

#[derive(Subcommand)]
pub enum TenantCommands {
    /// Assign a tenant to a specific shard
    Assign(TenantAssignArgs),
    /// Remove a tenant-specific shard assignment
    Unassign(TenantUnassignArgs),
    /// List manual tenant assignments
    List(TenantListArgs),
    /// Show shard allocation statistics
    Stats(TenantStatsArgs),
    /// Manage tenant aggregate quotas
    Quota {
        #[command(subcommand)]
        command: TenantQuotaCommands,
    },
    /// Manage tenant schema history and activation
    Schema {
        #[command(subcommand)]
        command: TenantSchemaCommands,
    },
}

#[derive(Args)]
pub struct TenantAssignArgs {
    /// Tenant identifier to assign
    #[arg(value_name = "TENANT")]
    pub tenant: String,

    /// Shard to assign the tenant to (format: shard-0001 or numeric index)
    #[arg(long = "shard", value_name = "SHARD")]
    pub shard: String,
}

#[derive(Args)]
pub struct TenantUnassignArgs {
    /// Tenant identifier to unassign
    #[arg(value_name = "TENANT")]
    pub tenant: String,
}

#[derive(Args, Default)]
pub struct TenantListArgs {
    /// Filter the list to a specific shard
    #[arg(long = "shard", value_name = "SHARD")]
    pub shard: Option<String>,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Default)]
pub struct TenantStatsArgs {
    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Subcommand)]
pub enum TenantQuotaCommands {
    /// Set an aggregate quota for a tenant
    Set(TenantQuotaSetArgs),
    /// Clear an aggregate quota for a tenant
    Clear(TenantQuotaClearArgs),
    /// Recalculate aggregate counts for a tenant
    Recalc(TenantQuotaRecalcArgs),
}

#[derive(Args)]
pub struct TenantQuotaSetArgs {
    /// Tenant identifier whose quota should be updated
    #[arg(value_name = "TENANT")]
    pub tenant: String,

    /// Maximum number of aggregates the tenant may create
    #[arg(long = "max-aggregates", value_name = "COUNT")]
    pub max_aggregates: u64,
}

#[derive(Args)]
pub struct TenantQuotaClearArgs {
    /// Tenant identifier whose quota should be cleared
    #[arg(value_name = "TENANT")]
    pub tenant: String,
}

#[derive(Args)]
pub struct TenantQuotaRecalcArgs {
    /// Tenant identifier whose aggregate count should be recomputed
    #[arg(value_name = "TENANT")]
    pub tenant: String,
}

#[derive(Subcommand)]
pub enum TenantSchemaCommands {
    /// Publish a new schema version (optionally activating it)
    Publish(TenantSchemaPublishArgs),
    /// Show schema version history for a tenant
    History(TenantSchemaHistoryArgs),
    /// Show the JSON contents for a schema version
    Show(TenantSchemaShowArgs),
    /// Diff two schema versions
    Diff(TenantSchemaDiffArgs),
    /// Activate a specific schema version
    Activate(TenantSchemaActivateArgs),
    /// Roll back to an earlier schema version
    Rollback(TenantSchemaRollbackArgs),
    /// Reload the tenant schema cache in the running daemon
    Reload(TenantSchemaReloadArgs),
}

#[derive(Args)]
pub struct TenantSchemaPublishArgs {
    /// Tenant identifier the schema applies to
    #[arg(value_name = "TENANT")]
    pub tenant: String,

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

#[derive(Args)]
pub struct TenantSchemaHistoryArgs {
    /// Tenant identifier to inspect
    #[arg(value_name = "TENANT")]
    pub tenant: String,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,

    /// Include audit log entries
    #[arg(long, default_value_t = false)]
    pub audit: bool,
}

#[derive(Args)]
pub struct TenantSchemaShowArgs {
    /// Tenant identifier to inspect
    #[arg(value_name = "TENANT")]
    pub tenant: String,

    /// Version identifier to display (use 'active' or 'latest' for shortcuts)
    #[arg(long = "version", value_name = "VERSION")]
    pub version: Option<String>,
}

#[derive(Args)]
pub struct TenantSchemaDiffArgs {
    /// Tenant identifier to inspect
    #[arg(value_name = "TENANT")]
    pub tenant: String,

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

#[derive(Args)]
pub struct TenantSchemaActivateArgs {
    /// Tenant identifier whose schema should change
    #[arg(value_name = "TENANT")]
    pub tenant: String,

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

#[derive(Args)]
pub struct TenantSchemaRollbackArgs {
    /// Tenant identifier whose schema should roll back
    #[arg(value_name = "TENANT")]
    pub tenant: String,

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

#[derive(Args)]
pub struct TenantSchemaReloadArgs {
    /// Tenant identifier whose schema cache should be refreshed
    #[arg(value_name = "TENANT")]
    pub tenant: String,
}

pub fn execute(config_path: Option<PathBuf>, command: TenantCommands) -> Result<()> {
    match command {
        TenantCommands::Assign(args) => assign(config_path, args),
        TenantCommands::Unassign(args) => unassign(config_path, args),
        TenantCommands::List(args) => list(config_path, args),
        TenantCommands::Stats(args) => stats(config_path, args),
        TenantCommands::Quota { command } => match command {
            TenantQuotaCommands::Set(args) => quota_set(config_path, args),
            TenantQuotaCommands::Clear(args) => quota_clear(config_path, args),
            TenantQuotaCommands::Recalc(args) => quota_recalc(config_path, args),
        },
        TenantCommands::Schema { command } => match command {
            TenantSchemaCommands::Publish(args) => schema_publish(config_path, args),
            TenantSchemaCommands::History(args) => schema_history(config_path, args),
            TenantSchemaCommands::Show(args) => schema_show(config_path, args),
            TenantSchemaCommands::Diff(args) => schema_diff(config_path, args),
            TenantSchemaCommands::Activate(args) => {
                schema_activate(config_path, args, SchemaAuditAction::Activate, "activated")
            }
            TenantSchemaCommands::Rollback(args) => schema_activate(
                config_path,
                TenantSchemaActivateArgs {
                    tenant: args.tenant,
                    version: args.version,
                    reason: args.reason,
                    actor: args.actor,
                    no_reload: args.no_reload,
                },
                SchemaAuditAction::Rollback,
                "rolled back",
            ),
            TenantSchemaCommands::Reload(args) => schema_reload(config_path, args),
        },
    }
}

fn assign(config_path: Option<PathBuf>, args: TenantAssignArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    let shard = normalize_shard_id(&args.shard, config.shard_count())?;
    match try_assign_offline(&config, &tenant, &shard) {
        Ok(changed) => report_assign(&tenant, &shard, changed),
        Err(err) if is_lock_error(&err) => {
            let changed = assign_online(&config, &tenant, &shard)?;
            report_assign(&tenant, &shard, changed);
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn unassign(config_path: Option<PathBuf>, args: TenantUnassignArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    match try_unassign_offline(&config, &tenant) {
        Ok(changed) => report_unassign(&tenant, changed),
        Err(err) if is_lock_error(&err) => {
            let changed = unassign_online(&config, &tenant)?;
            report_unassign(&tenant, changed);
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn list(config_path: Option<PathBuf>, args: TenantListArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = TenantAssignmentStore::open_read_only(config.tenant_meta_path())?;
    let shard_filter = args
        .shard
        .as_deref()
        .map(|value| normalize_shard_id(value, config.shard_count()))
        .transpose()?;

    let mut entries: Vec<_> = store
        .list()?
        .into_iter()
        .filter_map(|(tenant, record)| {
            if let Some(filter) = shard_filter.as_ref() {
                if record
                    .shard
                    .as_ref()
                    .map(|value| !value.eq_ignore_ascii_case(filter))
                    .unwrap_or(true)
                {
                    return None;
                }
            }
            if record.shard.is_none() && record.aggregate_quota.is_none() {
                return None;
            }
            Some(TenantSummary {
                tenant,
                shard: record.shard,
                quota: record.aggregate_quota,
                count: record.aggregate_count,
            })
        })
        .collect();
    entries.sort_by(|a, b| a.tenant.cmp(&b.tenant));

    if args.json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
        return Ok(());
    }

    if entries.is_empty() {
        println!("no tenant assignments or quotas found");
        println!(
            "tenants will hash across {} shards by default",
            config.shard_count()
        );
        return Ok(());
    }

    print_table(entries);
    Ok(())
}

fn stats(config_path: Option<PathBuf>, args: TenantStatsArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = TenantAssignmentStore::open_read_only(config.tenant_meta_path())?;
    let mut counts = std::collections::BTreeMap::new();
    for (_, record) in store.list()? {
        if let Some(shard) = record.shard {
            *counts.entry(shard).or_insert(0usize) += 1;
        }
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&counts)?);
        return Ok(());
    }

    if counts.is_empty() {
        println!("no explicit tenant assignments; shards fall back to hash-based placement");
        return Ok(());
    }

    let mut shard_width = "shard".len();
    let mut count_width = "count".len();
    for (shard, count) in &counts {
        shard_width = shard_width.max(shard.len());
        count_width = count_width.max(count.to_string().len());
    }
    println!(
        "{:shard_width$}  {:>count_width$}",
        "shard",
        "count",
        shard_width = shard_width,
        count_width = count_width
    );
    for (shard, count) in counts {
        println!(
            "{:shard_width$}  {:>count_width$}",
            shard,
            count,
            shard_width = shard_width,
            count_width = count_width
        );
    }
    Ok(())
}

#[derive(Serialize)]
struct TenantSummary {
    tenant: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    shard: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quota: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
}

fn print_table(entries: Vec<TenantSummary>) {
    let mut tenant_width = "tenant".len();
    let mut shard_width = "shard".len();
    let mut quota_width = "quota".len();
    let mut count_width = "count".len();
    for entry in &entries {
        tenant_width = tenant_width.max(entry.tenant.len());
        shard_width = shard_width.max(entry.shard.as_deref().unwrap_or("-").len());
        if let Some(quota) = entry.quota {
            quota_width = quota_width.max(quota.to_string().len());
        }
        if let Some(count) = entry.count {
            count_width = count_width.max(count.to_string().len());
        }
    }

    println!(
        "{:tenant_width$}  {:shard_width$}  {:>quota_width$}  {:>count_width$}",
        "tenant",
        "shard",
        "quota",
        "count",
        tenant_width = tenant_width,
        shard_width = shard_width,
        quota_width = quota_width,
        count_width = count_width,
    );
    for entry in entries {
        let shard_display = entry.shard.as_deref().unwrap_or("-");
        let quota_display = entry
            .quota
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let count_display = entry
            .count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:tenant_width$}  {:shard_width$}  {:>quota_width$}  {:>count_width$}",
            entry.tenant,
            shard_display,
            quota_display,
            count_display,
            tenant_width = tenant_width,
            shard_width = shard_width,
            quota_width = quota_width,
            count_width = count_width,
        );
    }
}

fn quota_set(config_path: Option<PathBuf>, args: TenantQuotaSetArgs) -> Result<()> {
    if args.max_aggregates == 0 {
        bail!("--max-aggregates must be greater than zero");
    }
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    match try_set_quota_offline(&config, &tenant, args.max_aggregates) {
        Ok(changed) => report_quota_set(&tenant, args.max_aggregates, changed),
        Err(err) if is_lock_error(&err) => {
            let changed = quota_set_online(&config, &tenant, args.max_aggregates)?;
            report_quota_set(&tenant, args.max_aggregates, changed);
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn quota_clear(config_path: Option<PathBuf>, args: TenantQuotaClearArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    match try_clear_quota_offline(&config, &tenant) {
        Ok(changed) => report_quota_clear(&tenant, changed),
        Err(err) if is_lock_error(&err) => {
            let changed = quota_clear_online(&config, &tenant)?;
            report_quota_clear(&tenant, changed);
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn quota_recalc(config_path: Option<PathBuf>, args: TenantQuotaRecalcArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    match try_quota_recalc_offline(&config, &tenant) {
        Ok(counts) => report_quota_recalc(&tenant, counts),
        Err(err) if is_lock_error(&err) => {
            let counts = quota_recalc_online(&config, &tenant)?;
            report_quota_recalc(&tenant, counts);
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn schema_publish(config_path: Option<PathBuf>, args: TenantSchemaPublishArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
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

fn schema_history(config_path: Option<PathBuf>, args: TenantSchemaHistoryArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
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

fn schema_show(config_path: Option<PathBuf>, args: TenantSchemaShowArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    let manager = SchemaHistoryManager::new(config.domain_data_dir_for(&tenant));
    let manifest = manager.manifest()?;
    let version_id =
        resolve_version_spec(&manifest, args.version.as_deref(), VersionFallback::Active)?;
    let payload = manager.version_payload(&version_id)?;
    println!("{payload}");
    Ok(())
}

fn schema_diff(config_path: Option<PathBuf>, args: TenantSchemaDiffArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
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

fn schema_reload(config_path: Option<PathBuf>, args: TenantSchemaReloadArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    let reloaded = schema_reload_online(&config, &tenant)?;
    report_schema_reload(&tenant, reloaded);
    Ok(())
}

fn schema_activate(
    config_path: Option<PathBuf>,
    args: TenantSchemaActivateArgs,
    action: SchemaAuditAction,
    verb: &str,
) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
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

fn schema_reload_online(config: &Config, tenant: &str) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.reload_tenant(&token, tenant)
}

fn report_schema_reload(tenant: &str, reloaded: bool) {
    if reloaded {
        println!("tenant={} schema cache reloaded", tenant);
    } else {
        println!("tenant={} schema cache reload skipped", tenant);
    }
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
                .ok_or_else(|| anyhow::anyhow!("no active schema version available"));
        }
        if raw.eq_ignore_ascii_case("latest") {
            return SchemaHistoryManager::latest_version_id(manifest)
                .ok_or_else(|| anyhow::anyhow!("no schema versions recorded"));
        }
        if manifest.versions.iter().any(|entry| entry.id == raw) {
            return Ok(raw.to_string());
        }
        bail!(
            "schema version '{}' was not found; run `tenant schema history <tenant>` to list versions",
            raw
        );
    }

    match fallback {
        VersionFallback::Active => manifest
            .active_version
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no active schema version available")),
        VersionFallback::Latest => SchemaHistoryManager::latest_version_id(manifest)
            .ok_or_else(|| anyhow::anyhow!("no schema versions recorded")),
        VersionFallback::PreviousActive => previous_version_before_active(manifest)
            .ok_or_else(|| anyhow::anyhow!("no earlier schema version to roll back to")),
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

fn try_assign_offline(
    config: &Config,
    tenant: &str,
    shard: &str,
) -> std::result::Result<bool, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    store.assign(tenant, shard)
}

fn try_unassign_offline(config: &Config, tenant: &str) -> std::result::Result<bool, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    store.unassign(tenant)
}

fn try_set_quota_offline(
    config: &Config,
    tenant: &str,
    quota: u64,
) -> std::result::Result<bool, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    store.set_quota(tenant, Some(quota))
}

fn try_clear_quota_offline(config: &Config, tenant: &str) -> std::result::Result<bool, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    store.set_quota(tenant, None)
}

fn try_quota_recalc_offline(config: &Config, tenant: &str) -> std::result::Result<u64, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    let tenant_event_store = EventStore::open(
        config.event_store_path_for(tenant),
        config.encryption_key()?,
        config.snowflake_worker_id,
    )?;
    let counts = tenant_event_store
        .counts()
        .map(|counts| counts.total_aggregates() as u64)?;
    store.ensure_aggregate_count(tenant, || Ok(counts))?;
    Ok(counts)
}

fn assign_online(config: &Config, tenant: &str, shard: &str) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.assign_tenant(&token, tenant, shard)
}

fn unassign_online(config: &Config, tenant: &str) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.unassign_tenant(&token, tenant)
}

fn quota_set_online(config: &Config, tenant: &str, quota: u64) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.set_tenant_quota(&token, tenant, quota)
}

fn quota_clear_online(config: &Config, tenant: &str) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.clear_tenant_quota(&token, tenant)
}

fn quota_recalc_online(config: &Config, tenant: &str) -> Result<u64> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.recalc_tenant_aggregates(&token, tenant)
}

fn prepare_remote_client(config: &Config, tenant: &str) -> Result<(ServerClient, String)> {
    let token = cli_token::ensure_bootstrap_token(config)?;
    let client = ServerClient::new(config)?.with_tenant(Some(tenant.to_string()));
    Ok((client, token))
}

fn is_lock_error(err: &EventError) -> bool {
    match err {
        EventError::Storage(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("lock file") || lower.contains("resource temporarily unavailable")
        }
        _ => false,
    }
}

fn report_assign(tenant: &str, shard: &str, changed: bool) {
    if changed {
        println!("Assigned tenant '{}' to shard '{}'.", tenant, shard);
    } else {
        println!(
            "Tenant '{}' is already assigned to shard '{}'.",
            tenant, shard
        );
    }
}

fn report_unassign(tenant: &str, changed: bool) {
    if changed {
        println!(
            "Removed explicit shard assignment for tenant '{}'; default hashing will apply.",
            tenant
        );
    } else {
        println!(
            "Tenant '{}' was not explicitly assigned to a shard.",
            tenant
        );
    }
}

fn report_quota_set(tenant: &str, quota: u64, changed: bool) {
    if changed {
        println!("Set aggregate quota for tenant '{}' to {}.", tenant, quota);
    } else {
        println!(
            "Aggregate quota for tenant '{}' already set to {}.",
            tenant, quota
        );
    }
}

fn report_quota_clear(tenant: &str, changed: bool) {
    if changed {
        println!("Cleared aggregate quota for tenant '{}'.", tenant);
    } else {
        println!("No aggregate quota configured for tenant '{}'.", tenant);
    }
}

fn report_quota_recalc(tenant: &str, count: u64) {
    println!(
        "Recalculated aggregate count for tenant '{}' ({} aggregate(s)).",
        tenant, count
    );
}
