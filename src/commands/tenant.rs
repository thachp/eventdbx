use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json;

use crate::commands::{
    cli_token,
    client::ServerClient,
    schema_version::{
        SchemaActivateArgs, SchemaDiffArgs, SchemaHistoryArgs, SchemaPublishArgs, SchemaReloadArgs,
        SchemaRollbackArgs, SchemaShowArgs, schema_activate, schema_diff, schema_history,
        schema_publish, schema_reload, schema_rollback, schema_show,
    },
};
use eventdbx::{
    config::{Config, load_or_default},
    error::EventError,
    schema_history::SchemaAuditAction,
    store::EventStore,
    tenant::{normalize_shard_id, normalize_tenant_id},
    tenant_store::{BYTES_PER_MEGABYTE, TenantAssignmentStore, compute_default_shard},
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
    /// Manage tenant storage quotas
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

    /// Shard to assign the tenant to (format: shard-0001 or numeric index). Defaults to hash-based placement.
    #[arg(long = "shard", value_name = "SHARD")]
    pub shard: Option<String>,
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
    /// Set a storage quota for a tenant
    Set(TenantQuotaSetArgs),
    /// Clear a storage quota for a tenant
    Clear(TenantQuotaClearArgs),
    /// Recalculate aggregate counts for a tenant
    Recalc(TenantQuotaRecalcArgs),
}

#[derive(Args)]
pub struct TenantQuotaSetArgs {
    /// Tenant identifier whose quota should be updated
    #[arg(value_name = "TENANT")]
    pub tenant: String,

    /// Maximum storage allocation in megabytes
    #[arg(
        short = 'm',
        long = "max-storage-mb",
        alias = "max-aggregates",
        value_name = "MB"
    )]
    pub max_storage_mb: u64,
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
    Publish(SchemaPublishArgs),
    /// Show schema version history for a tenant
    History(SchemaHistoryArgs),
    /// Show the JSON contents for a schema version
    Show(SchemaShowArgs),
    /// Diff two schema versions
    Diff(SchemaDiffArgs),
    /// Activate a specific schema version
    Activate(SchemaActivateArgs),
    /// Roll back to an earlier schema version
    Rollback(SchemaRollbackArgs),
    /// Reload the tenant schema cache in the running daemon
    Reload(SchemaReloadArgs),
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
            TenantSchemaCommands::Rollback(args) => schema_rollback(config_path, args),
            TenantSchemaCommands::Reload(args) => schema_reload(config_path, args),
        },
    }
}

fn assign(config_path: Option<PathBuf>, args: TenantAssignArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let TenantAssignArgs { tenant, shard } = args;
    let tenant = normalize_tenant_id(&tenant)?;
    let shard = match shard {
        Some(value) => normalize_shard_id(&value, config.shard_count())?,
        None => compute_default_shard(&tenant, config.shard_count()),
    };
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
            if record.shard.is_none()
                && record.storage_quota_mb.is_none()
                && record.storage_usage_bytes.is_none()
            {
                return None;
            }
            Some(TenantSummary {
                tenant,
                shard: record.shard,
                quota_mb: record.storage_quota_mb,
                usage_mb: record.storage_usage_bytes.map(bytes_to_megabytes),
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
    quota_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
}

struct TenantUsageStats {
    aggregate_count: u64,
    storage_bytes: u64,
}

fn print_table(entries: Vec<TenantSummary>) {
    let mut tenant_width = "tenant".len();
    let mut shard_width = "shard".len();
    let mut quota_width = "quota_mb".len();
    let mut usage_width = "usage_mb".len();
    let mut count_width = "count".len();
    for entry in &entries {
        tenant_width = tenant_width.max(entry.tenant.len());
        shard_width = shard_width.max(entry.shard.as_deref().unwrap_or("-").len());
        if let Some(quota) = entry.quota_mb {
            quota_width = quota_width.max(quota.to_string().len());
        }
        if let Some(usage) = entry.usage_mb {
            usage_width = usage_width.max(usage.to_string().len());
        }
        if let Some(count) = entry.count {
            count_width = count_width.max(count.to_string().len());
        }
    }

    println!(
        "{:tenant_width$}  {:shard_width$}  {:>quota_width$}  {:>usage_width$}  {:>count_width$}",
        "tenant",
        "shard",
        "quota_mb",
        "usage_mb",
        "count",
        tenant_width = tenant_width,
        shard_width = shard_width,
        quota_width = quota_width,
        usage_width = usage_width,
        count_width = count_width,
    );
    for entry in entries {
        let shard_display = entry.shard.as_deref().unwrap_or("-");
        let quota_display = entry
            .quota_mb
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let usage_display = entry
            .usage_mb
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let count_display = entry
            .count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:tenant_width$}  {:shard_width$}  {:>quota_width$}  {:>usage_width$}  {:>count_width$}",
            entry.tenant,
            shard_display,
            quota_display,
            usage_display,
            count_display,
            tenant_width = tenant_width,
            shard_width = shard_width,
            quota_width = quota_width,
            usage_width = usage_width,
            count_width = count_width,
        );
    }
}

fn quota_set(config_path: Option<PathBuf>, args: TenantQuotaSetArgs) -> Result<()> {
    if args.max_storage_mb == 0 {
        bail!("--max-storage-mb must be greater than zero");
    }
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    match try_set_quota_offline(&config, &tenant, args.max_storage_mb) {
        Ok(changed) => report_quota_set(&tenant, args.max_storage_mb, changed),
        Err(err) if is_lock_error(&err) => {
            let changed = quota_set_online(&config, &tenant, args.max_storage_mb)?;
            report_quota_set(&tenant, args.max_storage_mb, changed);
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
        Ok(stats) => report_quota_recalc(&tenant, stats),
        Err(err) if is_lock_error(&err) => {
            let stats = quota_recalc_online(&config, &tenant)?;
            report_quota_recalc(&tenant, stats);
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
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
    quota_mb: u64,
) -> std::result::Result<bool, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    store.set_quota(tenant, Some(quota_mb))
}

fn try_clear_quota_offline(config: &Config, tenant: &str) -> std::result::Result<bool, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    store.set_quota(tenant, None)
}

fn try_quota_recalc_offline(
    config: &Config,
    tenant: &str,
) -> std::result::Result<TenantUsageStats, EventError> {
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    let tenant_event_store = EventStore::open(
        config.event_store_path_for(tenant),
        config.encryption_key()?,
        config.snowflake_worker_id,
    )?;
    let counts = tenant_event_store
        .counts()
        .map(|counts| counts.total_aggregates() as u64)?;
    let usage = tenant_event_store.approximate_storage_bytes()?;
    store.ensure_aggregate_count(tenant, || Ok(counts))?;
    store.set_storage_usage_bytes(tenant, Some(usage))?;
    Ok(TenantUsageStats {
        aggregate_count: counts,
        storage_bytes: usage,
    })
}

fn assign_online(config: &Config, tenant: &str, shard: &str) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.assign_tenant(&token, tenant, shard)
}

fn unassign_online(config: &Config, tenant: &str) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.unassign_tenant(&token, tenant)
}

fn quota_set_online(config: &Config, tenant: &str, quota_mb: u64) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.set_tenant_quota(&token, tenant, quota_mb)
}

fn quota_clear_online(config: &Config, tenant: &str) -> Result<bool> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    client.clear_tenant_quota(&token, tenant)
}

fn quota_recalc_online(config: &Config, tenant: &str) -> Result<TenantUsageStats> {
    let (client, token) = prepare_remote_client(config, tenant)?;
    let (aggregate_count, storage_bytes) = client.recalc_tenant_aggregates(&token, tenant)?;
    Ok(TenantUsageStats {
        aggregate_count,
        storage_bytes,
    })
}

pub(crate) fn prepare_remote_client(
    config: &Config,
    tenant: &str,
) -> Result<(ServerClient, String)> {
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

fn bytes_to_megabytes(value: u64) -> u64 {
    if value == 0 {
        0
    } else {
        (value + BYTES_PER_MEGABYTE - 1) / BYTES_PER_MEGABYTE
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

fn report_quota_set(tenant: &str, quota_mb: u64, changed: bool) {
    if changed {
        println!(
            "Set storage quota for tenant '{}' to {} MB.",
            tenant, quota_mb
        );
    } else {
        println!(
            "Storage quota for tenant '{}' already set to {} MB.",
            tenant, quota_mb
        );
    }
}

fn report_quota_clear(tenant: &str, changed: bool) {
    if changed {
        println!("Cleared storage quota for tenant '{}'.", tenant);
    } else {
        println!("No storage quota configured for tenant '{}'.", tenant);
    }
}

fn report_quota_recalc(tenant: &str, stats: TenantUsageStats) {
    println!(
        "Recalculated usage for tenant '{}' ({} aggregate(s), {} MB used).",
        tenant,
        stats.aggregate_count,
        bytes_to_megabytes(stats.storage_bytes)
    );
}
