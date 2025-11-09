use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json;

use eventdbx::{
    config::load_or_default,
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
    }
}

fn assign(config_path: Option<PathBuf>, args: TenantAssignArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    let shard = normalize_shard_id(&args.shard, config.shard_count())?;
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    let changed = store.assign(&tenant, &shard)?;
    if changed {
        println!("Assigned tenant '{}' to shard '{}'.", tenant, shard);
    } else {
        println!(
            "Tenant '{}' is already assigned to shard '{}'.",
            tenant, shard
        );
    }
    Ok(())
}

fn unassign(config_path: Option<PathBuf>, args: TenantUnassignArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    if store.unassign(&tenant)? {
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
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    let changed = store.set_quota(&tenant, Some(args.max_aggregates))?;
    if changed {
        println!(
            "Set aggregate quota for tenant '{}' to {}.",
            tenant, args.max_aggregates
        );
    } else {
        println!(
            "Aggregate quota for tenant '{}' already set to {}.",
            tenant, args.max_aggregates
        );
    }
    Ok(())
}

fn quota_clear(config_path: Option<PathBuf>, args: TenantQuotaClearArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    if store.set_quota(&tenant, None)? {
        println!("Cleared aggregate quota for tenant '{}'.", tenant);
    } else {
        println!("No aggregate quota configured for tenant '{}'.", tenant);
    }
    Ok(())
}

fn quota_recalc(config_path: Option<PathBuf>, args: TenantQuotaRecalcArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = normalize_tenant_id(&args.tenant)?;
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    let tenant_event_store = EventStore::open(
        config.event_store_path_for(&tenant),
        config.encryption_key()?,
        config.snowflake_worker_id,
    )?;
    let counts = tenant_event_store
        .counts()
        .map(|counts| counts.total_aggregates() as u64)?;
    store.set_quota(&tenant, store.quota_for(&tenant)?)?;
    store.ensure_aggregate_count(&tenant, || Ok(counts))?;
    println!(
        "Recalculated aggregate count for tenant '{}' ({} aggregate(s)).",
        tenant, counts
    );
    Ok(())
}
