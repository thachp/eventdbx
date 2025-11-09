use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json;

use eventdbx::{
    config::load_or_default,
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

pub fn execute(config_path: Option<PathBuf>, command: TenantCommands) -> Result<()> {
    match command {
        TenantCommands::Assign(args) => assign(config_path, args),
        TenantCommands::Unassign(args) => unassign(config_path, args),
        TenantCommands::List(args) => list(config_path, args),
        TenantCommands::Stats(args) => stats(config_path, args),
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
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    let shard_filter = args
        .shard
        .as_deref()
        .map(|value| normalize_shard_id(value, config.shard_count()))
        .transpose()?;

    let mut entries: Vec<_> = store
        .list()?
        .into_iter()
        .filter(|(_, shard)| {
            shard_filter
                .as_ref()
                .map(|filter| shard.eq_ignore_ascii_case(filter))
                .unwrap_or(true)
        })
        .map(|(tenant, shard)| TenantSummary { tenant, shard })
        .collect();
    entries.sort_by(|a, b| a.tenant.cmp(&b.tenant));

    if args.json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
        return Ok(());
    }

    if entries.is_empty() {
        println!("no tenant shard assignments found");
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
    let store = TenantAssignmentStore::open(config.tenant_meta_path())?;
    let mut counts = std::collections::BTreeMap::new();
    for (_, shard) in store.list()? {
        *counts.entry(shard).or_insert(0usize) += 1;
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
    shard: String,
}

fn print_table(entries: Vec<TenantSummary>) {
    let mut tenant_width = "tenant".len();
    let mut shard_width = "shard".len();
    for entry in &entries {
        tenant_width = tenant_width.max(entry.tenant.len());
        shard_width = shard_width.max(entry.shard.len());
    }
    println!(
        "{:tenant_width$}  {:shard_width$}",
        "tenant",
        "shard",
        tenant_width = tenant_width,
        shard_width = shard_width
    );
    for entry in entries {
        println!(
            "{:tenant_width$}  {:shard_width$}",
            entry.tenant,
            entry.shard,
            tenant_width = tenant_width,
            shard_width = shard_width
        );
    }
}
