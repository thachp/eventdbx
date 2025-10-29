use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::Args;
#[cfg(unix)]
use libc;
use serde::Deserialize;
use serde_json::Value;

use eventdbx::{
    config::{Config, DEFAULT_DOMAIN_NAME, load_or_default},
    schema::{AggregateSchema, SchemaManager},
    store::{AggregateQueryScope, EventStore},
};

#[derive(Args)]
pub struct DomainCheckoutArgs {
    /// Domain to activate
    #[arg(short = 'd', long = "domain", value_name = "NAME")]
    pub flag_domain: Option<String>,

    /// Domain to activate (positional alias for -d/--domain)
    #[arg(value_name = "NAME")]
    pub positional_domain: Option<String>,
}

#[derive(Args)]
pub struct DomainMergeArgs {
    /// Source domain to import from
    #[arg(long = "from", value_name = "DOMAIN")]
    pub from: String,

    /// Target domain to import into (defaults to the currently active domain)
    #[arg(long = "into", value_name = "DOMAIN")]
    pub into: Option<String>,

    /// Replace conflicting schema definitions in the target domain
    #[arg(long, default_value_t = false)]
    pub overwrite_schemas: bool,
}

pub fn checkout(config_path: Option<PathBuf>, args: DomainCheckoutArgs) -> Result<()> {
    let target = resolve_checkout_domain(&args)?;
    let (mut config, path) = load_or_default(config_path)?;

    if config.active_domain() == target {
        println!(
            "Domain '{}' is already active (data directory: {}).",
            target,
            config.domain_data_dir().display()
        );
        return Ok(());
    }

    config.domain = target.clone();
    config.ensure_data_dir()?;
    let domain_root = config.domain_data_dir();
    config.updated_at = Utc::now();
    config.save(&path)?;

    println!(
        "Switched to domain '{}' (data directory: {}).",
        target,
        domain_root.display()
    );

    Ok(())
}

pub fn merge(config_path: Option<PathBuf>, args: DomainMergeArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let source_domain = normalize_domain_name(&args.from)?;
    let target_domain = match args.into {
        Some(ref value) => normalize_domain_name(value)?,
        None => config.active_domain().to_string(),
    };

    if source_domain == target_domain {
        bail!("source and target domains must be different");
    }

    let mut source_config = config.clone();
    source_config.domain = source_domain.clone();
    source_config.ensure_data_dir()?;

    let mut target_config = config.clone();
    target_config.domain = target_domain.clone();
    target_config.ensure_data_dir()?;

    ensure_domain_stopped(&source_config)?;
    ensure_domain_stopped(&target_config)?;

    let schema_stats = merge_schemas(&source_config, &target_config, args.overwrite_schemas)?;
    let event_stats = merge_events(&source_config, &target_config)?;

    if schema_stats.added == 0 && schema_stats.replaced == 0 && event_stats.events == 0 {
        println!(
            "No data to merge from '{}' into '{}'.",
            source_domain, target_domain
        );
        return Ok(());
    }

    println!(
        "Merged {} aggregate(s) and {} event(s) from '{}' into '{}'.",
        event_stats.aggregates, event_stats.events, source_domain, target_domain
    );
    if event_stats.archived > 0 {
        println!(
            "  • {} aggregate(s) marked archived after merge",
            event_stats.archived
        );
    }
    if schema_stats.added > 0 || schema_stats.replaced > 0 {
        println!(
            "Merged schemas: {} added{}",
            schema_stats.added,
            if schema_stats.replaced > 0 {
                format!("; {} replaced", schema_stats.replaced)
            } else {
                String::new()
            }
        );
    }

    Ok(())
}

pub(crate) fn normalize_domain_name(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("domain name cannot be empty");
    }

    if trimmed.eq_ignore_ascii_case(DEFAULT_DOMAIN_NAME) {
        return Ok(DEFAULT_DOMAIN_NAME.to_string());
    }

    let lower = trimmed.to_ascii_lowercase();
    if !matches!(lower.chars().next(), Some(ch) if ch.is_ascii_alphanumeric()) {
        bail!("domain name must begin with an ASCII letter or digit");
    }
    if !lower
        .chars()
        .skip(1)
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        bail!("domain name may only contain letters, numbers, '-' or '_'");
    }
    Ok(lower)
}

fn resolve_checkout_domain(args: &DomainCheckoutArgs) -> Result<String> {
    match (&args.flag_domain, &args.positional_domain) {
        (Some(flag), None) => normalize_domain_name(flag),
        (None, Some(positional)) => normalize_domain_name(positional),
        (Some(flag), Some(positional)) => {
            let normalized_flag = normalize_domain_name(flag)?;
            let normalized_positional = normalize_domain_name(positional)?;
            if normalized_flag != normalized_positional {
                bail!("conflicting domain inputs provided via -d/--domain and positional argument");
            }
            Ok(normalized_flag)
        }
        (None, None) => {
            bail!("domain name must be provided via -d/--domain or positional argument")
        }
    }
}

struct SchemaMergeStats {
    added: usize,
    replaced: usize,
}

impl Default for SchemaMergeStats {
    fn default() -> Self {
        Self {
            added: 0,
            replaced: 0,
        }
    }
}

struct EventMergeStats {
    aggregates: usize,
    events: usize,
    archived: usize,
}

impl Default for EventMergeStats {
    fn default() -> Self {
        Self {
            aggregates: 0,
            events: 0,
            archived: 0,
        }
    }
}

fn merge_schemas(source: &Config, target: &Config, overwrite: bool) -> Result<SchemaMergeStats> {
    let source_manager = SchemaManager::load(source.schema_store_path())?;
    let target_manager = SchemaManager::load(target.schema_store_path())?;
    let source_items = source_manager.snapshot();

    if source_items.is_empty() {
        return Ok(SchemaMergeStats::default());
    }

    let mut target_items = target_manager.snapshot();
    let mut stats = SchemaMergeStats::default();

    for (name, schema) in source_items {
        match target_items.get(&name) {
            Some(existing) => {
                if schemas_equivalent(existing, &schema) {
                    continue;
                }
                if !overwrite {
                    bail!(
                        "schema '{}' already exists in target domain with different definition (use --overwrite-schemas to replace)",
                        name
                    );
                }
                target_items.insert(name.clone(), schema.clone());
                stats.replaced += 1;
            }
            None => {
                target_items.insert(name.clone(), schema.clone());
                stats.added += 1;
            }
        }
    }

    if stats.added > 0 || stats.replaced > 0 {
        target_manager.replace_all(target_items)?;
    }

    Ok(stats)
}

fn schemas_equivalent(lhs: &AggregateSchema, rhs: &AggregateSchema) -> bool {
    schema_signature(lhs) == schema_signature(rhs)
}

fn schema_signature(schema: &AggregateSchema) -> Value {
    let mut value = serde_json::to_value(schema).expect("schema should serialize successfully");
    if let Value::Object(ref mut map) = value {
        map.remove("created_at");
        map.remove("updated_at");
    }
    value
}

fn merge_events(source: &Config, target: &Config) -> Result<EventMergeStats> {
    let source_path = source.event_store_path();
    if !source_path.exists() {
        return Ok(EventMergeStats::default());
    }

    let source_store = EventStore::open_read_only(source_path, source.encryption_key()?)?;
    let target_store = EventStore::open(
        target.event_store_path(),
        target.encryption_key()?,
        target.snowflake_worker_id,
    )?;

    let mut stats = EventMergeStats::default();
    let mut aggregates = source_store.aggregates_paginated_with_transform(
        0,
        None,
        None,
        AggregateQueryScope::IncludeArchived,
        |aggregate| Some(aggregate),
    );
    aggregates.sort_by(|a, b| {
        a.aggregate_type
            .cmp(&b.aggregate_type)
            .then_with(|| a.aggregate_id.cmp(&b.aggregate_id))
    });

    for aggregate in aggregates {
        if aggregate.version == 0 {
            continue;
        }
        if target_store
            .aggregate_version(&aggregate.aggregate_type, &aggregate.aggregate_id)?
            .is_some()
        {
            bail!(
                "aggregate '{}::{}' already exists in target domain; aborting merge",
                aggregate.aggregate_type,
                aggregate.aggregate_id
            );
        }

        let events =
            source_store.list_events(&aggregate.aggregate_type, &aggregate.aggregate_id)?;
        for event in events {
            target_store.append_replica(event)?;
            stats.events += 1;
        }

        if aggregate.archived {
            target_store.set_archive(
                &aggregate.aggregate_type,
                &aggregate.aggregate_id,
                true,
                None,
            )?;
            stats.archived += 1;
        }

        stats.aggregates += 1;
    }

    Ok(stats)
}

fn ensure_domain_stopped(config: &Config) -> Result<()> {
    let pid_path = config.pid_file_path();
    if !pid_path.exists() {
        return Ok(());
    }
    match read_pid(&pid_path)? {
        Some(pid) if process_is_running(pid) => bail!(
            "EventDBX server appears to be running for domain '{}' (pid {}); stop it before merging",
            config.active_domain(),
            pid
        ),
        _ => Ok(()),
    }
}

fn read_pid(path: &Path) -> Result<Option<u32>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read pid file at {}", path.display()))?;
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let Ok(record) = serde_json::from_str::<PidRecord>(trimmed) {
        return Ok(Some(record.pid));
    }
    trimmed
        .parse::<u32>()
        .map(Some)
        .map_err(|err| anyhow::anyhow!("invalid pid contents at {}: {}", path.display(), err))
}

fn process_is_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unsafe {
            if libc::kill(pid as libc::pid_t, 0) == 0 {
                true
            } else {
                let err = io::Error::last_os_error();
                !matches!(err.raw_os_error(), Some(libc::ESRCH))
            }
        }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::{
            Foundation::CloseHandle,
            System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION},
        };
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle == 0 {
                false
            } else {
                CloseHandle(handle);
                true
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

#[derive(Deserialize)]
struct PidRecord {
    pid: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn normalizes_default_domain_case_insensitively() {
        let result = normalize_domain_name("DEFAULT").expect("domain should normalize");
        assert_eq!(result, DEFAULT_DOMAIN_NAME);
    }

    #[test]
    fn normalizes_custom_domain() {
        let result = normalize_domain_name("Herds-01").expect("domain should normalize");
        assert_eq!(result, "herds-01");
    }

    #[test]
    fn resolve_domain_from_flag() {
        let args = DomainCheckoutArgs {
            flag_domain: Some("Herds".to_string()),
            positional_domain: None,
        };
        let result = resolve_checkout_domain(&args).expect("flag domain should resolve");
        assert_eq!(result, "herds");
    }

    #[test]
    fn resolve_domain_from_positional() {
        let args = DomainCheckoutArgs {
            flag_domain: None,
            positional_domain: Some("Herds_01".to_string()),
        };
        let result = resolve_checkout_domain(&args).expect("positional domain should resolve");
        assert_eq!(result, "herds_01");
    }

    #[test]
    fn resolve_conflicting_inputs_errors() {
        let args = DomainCheckoutArgs {
            flag_domain: Some("alpha".to_string()),
            positional_domain: Some("beta".to_string()),
        };
        assert!(resolve_checkout_domain(&args).is_err());
    }

    #[test]
    fn rejects_invalid_characters() {
        assert!(normalize_domain_name("bad/name").is_err());
        assert!(normalize_domain_name("  ").is_err());
        assert!(normalize_domain_name("-leading").is_err());
    }

    #[test]
    fn schema_signature_ignores_timestamps() {
        let now = Utc::now();
        let schema_a = sample_schema("alpha", now);
        let mut schema_b = sample_schema("alpha", now + chrono::Duration::seconds(5));
        assert!(schemas_equivalent(&schema_a, &schema_b));

        schema_b.events.insert(
            "alpha_updated".to_string(),
            eventdbx::schema::EventSchema::default(),
        );
        assert!(!schemas_equivalent(&schema_a, &schema_b));
    }

    fn sample_schema(name: &str, timestamp: chrono::DateTime<chrono::Utc>) -> AggregateSchema {
        let mut events = BTreeMap::new();
        events.insert(
            format!("{}_created", name),
            eventdbx::schema::EventSchema::default(),
        );
        AggregateSchema {
            aggregate: name.to_string(),
            snapshot_threshold: None,
            locked: false,
            field_locks: Vec::new(),
            hidden: false,
            hidden_fields: Vec::new(),
            column_types: BTreeMap::new(),
            events,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }
}
