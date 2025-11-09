use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use chrono::Utc;
use clap::{Args, Parser, Subcommand};
#[cfg(unix)]
use libc;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};

use eventdbx::{
    config::{Config, DEFAULT_DOMAIN_NAME, DEFAULT_SOCKET_PORT, load_or_default},
    encryption::{self, Encryptor},
    error::EventError,
    merkle::{compute_merkle_root, empty_root},
    schema::{AggregateSchema, SchemaManager},
    store::{AggregateQueryScope, AggregateState, EventStore},
    tenant::normalize_tenant_id,
    validation::{ensure_aggregate_id, ensure_snake_case},
};
use std::io::{self, Write};

use crate::commands::client::ServerClient;

#[derive(Args)]
#[command(arg_required_else_help = true)]
pub struct DomainCheckoutArgs {
    /// Domain to activate
    #[arg(short = 'd', long = "domain", value_name = "NAME")]
    pub flag_domain: Option<String>,

    /// Domain to activate (positional alias for -d/--domain)
    #[arg(value_name = "NAME")]
    pub positional_domain: Option<String>,

    /// Create the domain before switching if it does not exist
    #[arg(
        short = 'c',
        long = "create",
        default_value_t = false,
        conflicts_with = "delete"
    )]
    pub create: bool,

    /// Delete the specified domain instead of switching
    #[arg(long, default_value_t = false)]
    pub delete: bool,

    /// Skip the interactive confirmation when deleting a domain
    #[arg(long, default_value_t = false, requires = "delete")]
    pub force: bool,

    /// Remote control socket (host or host:port) to associate with the domain
    #[arg(long = "remote", value_name = "ADDR")]
    pub remote_addr: Option<String>,

    /// Port for the remote control socket (defaults to 6363)
    #[arg(long = "port", value_name = "PORT", requires = "remote_addr")]
    pub remote_port: Option<u16>,

    /// Authorization token used when syncing with the remote domain
    #[arg(long = "token", value_name = "TOKEN")]
    pub remote_token: Option<String>,

    /// Tenant identifier to associate with the remote endpoint
    #[arg(long = "remote-tenant", value_name = "TENANT")]
    pub remote_tenant: Option<String>,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DomainRemoteConfig {
    #[serde(default)]
    connect_addr: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
}

#[derive(Debug, Clone)]
struct DomainRemoteEndpoint {
    connect_addr: String,
    token: String,
    tenant: String,
}

#[derive(Args)]
pub struct SchemaSyncArgs {
    /// Domain to synchronise
    #[arg(value_name = "DOMAIN")]
    pub domain: String,
}

#[derive(Subcommand)]
pub enum PushCommand {
    /// Push schemas to the remote endpoint
    #[command(name = "schema")]
    Schema(SchemaSyncArgs),
    /// Push domain data (domain name as positional argument)
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[derive(Subcommand)]
pub enum PullCommand {
    /// Pull schemas from the remote endpoint
    #[command(name = "schema")]
    Schema(SchemaSyncArgs),
    /// Pull domain data (domain name as positional argument)
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[derive(Debug, Parser)]
#[command(
    name = "domain-sync",
    disable_help_flag = true,
    disable_version_flag = true
)]
pub struct DomainSyncArgs {
    /// Domain to synchronise
    #[arg(value_name = "DOMAIN")]
    pub domain: String,

    /// Aggregate type to limit the operation to
    #[arg(long = "aggregate", value_name = "AGGREGATE")]
    pub aggregate: Option<String>,

    /// Aggregate identifier to limit the operation to (requires --aggregate)
    #[arg(long = "id", value_name = "AGGREGATE_ID", requires = "aggregate")]
    pub aggregate_id: Option<String>,

    /// Number of aggregates to process concurrently (defaults to CPU count)
    #[arg(long = "concurrency", value_name = "THREADS", value_parser = clap::value_parser!(usize))]
    pub concurrency: Option<usize>,
}

pub fn push(config_path: Option<PathBuf>, command: PushCommand) -> Result<()> {
    match command {
        PushCommand::Schema(args) => push_schema(config_path, args),
        PushCommand::External(argv) => {
            let args = parse_domain_sync_args(argv)?;
            push_domain(config_path, args)
        }
    }
}

pub fn pull(config_path: Option<PathBuf>, command: PullCommand) -> Result<()> {
    match command {
        PullCommand::Schema(args) => pull_schema(config_path, args),
        PullCommand::External(argv) => {
            let args = parse_domain_sync_args(argv)?;
            pull_domain(config_path, args)
        }
    }
}

pub fn checkout(config_path: Option<PathBuf>, args: DomainCheckoutArgs) -> Result<()> {
    if args.delete {
        return delete_domain(config_path, &args);
    }

    let target = resolve_checkout_domain(&args)?;
    let (mut config, path) = load_or_default(config_path)?;
    let mut candidate = config.clone();
    candidate.domain = target.clone();
    let domain_dir = candidate.domain_data_dir();

    if args.create {
        create_domain(&config, &target)?;
    }

    if config.active_domain() == target {
        println!(
            "Domain '{}' is already active (data directory: {}).",
            target,
            config.domain_data_dir().display()
        );
        return Ok(());
    }

    if !args.create && !candidate.is_default_domain() && !domain_dir.exists() {
        bail!(
            "domain '{}' does not exist; re-run with --create to create it",
            target
        );
    }

    config.domain = target.clone();
    config.ensure_data_dir()?;
    let domain_root = config.domain_data_dir();
    config.updated_at = Utc::now();
    config.save(&path)?;

    let remote_port = if args.remote_addr.is_some() {
        args.remote_port
    } else {
        None
    };

    if args.remote_addr.is_some() || args.remote_token.is_some() || args.remote_tenant.is_some() {
        update_domain_remote_config(
            &config,
            args.remote_addr.as_deref(),
            remote_port,
            args.remote_token.as_deref(),
            args.remote_tenant.as_deref(),
        )?;
    }

    println!(
        "Switched to domain '{}' (data directory: {}).",
        target,
        domain_root.display()
    );

    Ok(())
}

fn create_domain(config: &Config, domain: &str) -> Result<()> {
    if domain.eq_ignore_ascii_case(DEFAULT_DOMAIN_NAME) {
        println!("Domain '{}' already exists.", DEFAULT_DOMAIN_NAME);
        return Ok(());
    }

    let mut candidate = config.clone();
    candidate.domain = domain.to_string();
    let domain_dir = candidate.domain_data_dir();

    if domain_dir.exists() {
        println!(
            "Domain '{}' already exists (data directory: {}).",
            domain,
            domain_dir.display()
        );
        return Ok(());
    }

    candidate
        .ensure_data_dir()
        .with_context(|| format!("failed to create domain '{domain}'"))?;

    println!(
        "Created domain '{}' (data directory: {}).",
        domain,
        domain_dir.display()
    );
    Ok(())
}

fn delete_domain(config_path: Option<PathBuf>, args: &DomainCheckoutArgs) -> Result<()> {
    let target = resolve_checkout_domain(args)?;
    let (config, _) = load_or_default(config_path)?;

    if target.eq_ignore_ascii_case(DEFAULT_DOMAIN_NAME) {
        bail!("cannot delete the default domain");
    }
    if config.active_domain().eq_ignore_ascii_case(&target) {
        bail!("cannot delete the currently active domain; switch to a different domain first");
    }

    let mut candidate = config.clone();
    candidate.domain = target.clone();
    let domain_dir = candidate.domain_data_dir();

    if !domain_dir.exists() {
        println!("Domain '{}' does not exist.", target);
        return Ok(());
    }

    if !args.force && !confirm_delete(&target)? {
        println!("Domain deletion cancelled.");
        return Ok(());
    }

    ensure_domain_stopped(&candidate)?;

    fs::remove_dir_all(&domain_dir).with_context(|| {
        format!(
            "failed to delete domain '{}' at {}",
            target,
            domain_dir.display()
        )
    })?;

    println!(
        "Deleted domain '{}' (data directory: {}).",
        target,
        domain_dir.display()
    );
    Ok(())
}

fn confirm_delete(domain: &str) -> Result<bool> {
    print!("Type the domain name '{}' to confirm deletion: ", domain);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim() == domain)
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

fn push_domain(config_path: Option<PathBuf>, args: DomainSyncArgs) -> Result<()> {
    let domain = normalize_domain_name(&args.domain)?;
    let (config, _) = load_or_default(config_path)?;
    ensure_existing_domain(&config, &domain)?;

    let mut domain_config = config.clone();
    domain_config.domain = domain.clone();

    ensure_domain_stopped(&domain_config)?;

    let event_store_path = domain_config.event_store_path();
    if !event_store_path.is_dir() || !event_store_path.join("CURRENT").is_file() {
        println!(
            "Local domain '{}' has no event data to push (event store not initialised at {}).",
            domain,
            event_store_path.display()
        );
        println!(
            "Run `dbx start` once with domain '{}' active to initialise the store before pushing.",
            domain
        );
        return Ok(());
    }

    let store = Arc::new(EventStore::open_read_only(
        event_store_path,
        domain_config.encryption_key()?,
    )?);

    let aggregate_list = collect_local_aggregates(
        store.as_ref(),
        args.aggregate.as_deref(),
        args.aggregate_id.as_deref(),
    )?;
    if aggregate_list.is_empty() {
        println!("No aggregates matched the specified criteria.");
        return Ok(());
    }

    let total_aggregates = aggregate_list.len();
    let aggregates = Arc::new(aggregate_list);

    let remote = load_remote_endpoint_for_domain(&config, &domain)?;
    let concurrency = resolve_concurrency(args.concurrency, total_aggregates);

    let next_index = Arc::new(AtomicUsize::new(0));
    let failure_flag = Arc::new(AtomicBool::new(false));
    let output_lock = Arc::new(Mutex::new(()));
    let events_counter = Arc::new(AtomicUsize::new(0));
    let aggregates_counter = Arc::new(AtomicUsize::new(0));
    let archive_counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let store = Arc::clone(&store);
        let aggregates = Arc::clone(&aggregates);
        let token = remote.token.clone();
        let connect_addr = remote.connect_addr.clone();
        let remote_tenant = remote.tenant.clone();
        let next_index = Arc::clone(&next_index);
        let failure_flag = Arc::clone(&failure_flag);
        let output_lock = Arc::clone(&output_lock);
        let events_counter = Arc::clone(&events_counter);
        let aggregates_counter = Arc::clone(&aggregates_counter);
        let archive_counter = Arc::clone(&archive_counter);
        let total = total_aggregates;

        handles.push(thread::spawn(move || -> Result<()> {
            let client =
                ServerClient::with_addr_and_tenant(connect_addr, Some(remote_tenant.clone()));
            loop {
                if failure_flag.load(Ordering::Relaxed) {
                    break;
                }
                let idx = next_index.fetch_add(1, Ordering::Relaxed);
                if idx >= aggregates.len() {
                    break;
                }
                let aggregate = &aggregates[idx];
                let mut logs = Vec::new();
                match process_push_aggregate(
                    &mut logs,
                    store.as_ref(),
                    &client,
                    token.as_str(),
                    aggregate,
                    idx,
                    total,
                ) {
                    Ok(stats) => {
                        {
                            let _guard = output_lock.lock().unwrap();
                            for line in logs {
                                println!("{}", line);
                            }
                        }
                        if stats.events_pushed > 0 {
                            events_counter.fetch_add(stats.events_pushed, Ordering::Relaxed);
                        }
                        if stats.aggregate_updated {
                            aggregates_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        if stats.archive_updated {
                            archive_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(err) => {
                        {
                            let _guard = output_lock.lock().unwrap();
                            for line in logs {
                                println!("{}", line);
                            }
                        }
                        failure_flag.store(true, Ordering::Relaxed);
                        return Err(err);
                    }
                }
            }
            Ok(())
        }));
    }

    let mut first_err: Option<anyhow::Error> = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(panic) => {
                let message = if let Some(msg) = panic.downcast_ref::<&str>() {
                    msg.to_string()
                } else if let Some(msg) = panic.downcast_ref::<String>() {
                    msg.clone()
                } else {
                    "unknown panic".to_string()
                };
                if first_err.is_none() {
                    first_err = Some(anyhow!("push worker panicked: {}", message));
                }
            }
        }
    }
    if let Some(err) = first_err {
        return Err(err);
    }

    let events_pushed = events_counter.load(Ordering::Relaxed);
    let aggregates_updated = aggregates_counter.load(Ordering::Relaxed);
    let archive_updates = archive_counter.load(Ordering::Relaxed);

    if aggregates_updated == 0 && archive_updates == 0 && events_pushed == 0 {
        println!(
            "Remote domain '{}' at {} is already up to date.",
            domain, remote.connect_addr
        );
    } else {
        if events_pushed > 0 {
            println!(
                "Pushed {} event(s) across {} aggregate(s) to remote {}.",
                events_pushed, aggregates_updated, remote.connect_addr
            );
        }
        if archive_updates > 0 {
            println!(
                "Updated archive status for {} aggregate(s) on remote {}.",
                archive_updates, remote.connect_addr
            );
        }
        if events_pushed == 0 && archive_updates > 0 {
            println!("Remote domain '{}' archive flags synchronised.", domain);
        }
    }
    Ok(())
}

struct PushAggregateStats {
    events_pushed: usize,
    aggregate_updated: bool,
    archive_updated: bool,
}

fn process_push_aggregate(
    logs: &mut Vec<String>,
    store: &EventStore,
    client: &ServerClient,
    token: &str,
    aggregate: &AggregateState,
    index: usize,
    total: usize,
) -> Result<PushAggregateStats> {
    let aggregate_type = &aggregate.aggregate_type;
    let aggregate_id = &aggregate.aggregate_id;

    logs.push(format!(
        "Pushing aggregate {}/{}: {}::{}",
        index + 1,
        total,
        aggregate_type,
        aggregate_id
    ));

    let remote_state = client
        .get_aggregate(token, aggregate_type, aggregate_id)
        .with_context(|| {
            format!(
                "failed to fetch remote aggregate '{}::{}'",
                aggregate_type, aggregate_id
            )
        })?;

    let local_version = aggregate.version;
    if local_version == 0 {
        logs.push("  no local events; skipping".to_string());
        return Ok(PushAggregateStats {
            events_pushed: 0,
            aggregate_updated: false,
            archive_updated: false,
        });
    }

    let remote_version = remote_state
        .as_ref()
        .map(|state| state.version)
        .unwrap_or(0);
    if remote_version > local_version {
        bail!(
            "remote aggregate '{}::{}' has {} event(s) but local copy has {}; refusing to overwrite",
            aggregate_type,
            aggregate_id,
            remote_version,
            local_version
        );
    }

    if remote_version > 0 {
        let local_root = store
            .merkle_root_at(aggregate_type, aggregate_id, remote_version)
            .with_context(|| {
                format!(
                    "failed to compute local merkle root at version {} for '{}::{}'",
                    remote_version, aggregate_type, aggregate_id
                )
            })?
            .ok_or_else(|| {
                anyhow!(
                    "local aggregate metadata missing for '{}::{}' when computing merkle root",
                    aggregate_type,
                    aggregate_id
                )
            })?;
        let remote_root = remote_state
            .as_ref()
            .map(|state| state.merkle_root.clone())
            .unwrap_or_else(|| empty_root());
        if local_root != remote_root {
            bail!(
                "remote aggregate '{}::{}' diverged at version {}",
                aggregate_type,
                aggregate_id,
                remote_version
            );
        }
    }

    let new_events_total = (local_version - remote_version) as usize;
    let mut new_events = Vec::with_capacity(new_events_total);
    for version in (remote_version + 1)..=local_version {
        let event = store
            .event_by_version(aggregate_type, aggregate_id, version)?
            .ok_or_else(|| {
                anyhow!(
                    "local event version {} missing for '{}::{}'",
                    version,
                    aggregate_type,
                    aggregate_id
                )
            })?;
        new_events.push(event);
    }

    let mut remote_has_aggregate = remote_state.is_some();
    let mut wrote_events = 0usize;

    if !new_events.is_empty() && !remote_has_aggregate {
        logs.push(format!(
            "  created aggregate; pushing {} event(s)",
            new_events.len()
        ));
    }

    for (offset, event) in new_events.iter().enumerate() {
        if !remote_has_aggregate {
            client
                .create_aggregate(
                    token,
                    aggregate_type,
                    aggregate_id,
                    &event.event_type,
                    &event.payload,
                    event.extensions.as_ref(),
                    event.metadata.note.as_deref(),
                )
                .with_context(|| {
                    format!(
                        "failed to create remote aggregate '{}::{}'",
                        aggregate_type, aggregate_id
                    )
                })?;
            remote_has_aggregate = true;
        } else {
            client
                .append_event(
                    token,
                    aggregate_type,
                    aggregate_id,
                    &event.event_type,
                    Some(&event.payload),
                    event.extensions.as_ref(),
                    event.metadata.note.as_deref(),
                )
                .with_context(|| {
                    format!(
                        "failed to append event version {} for '{}::{}' on remote endpoint",
                        event.version, aggregate_type, aggregate_id
                    )
                })?;
        }
        wrote_events += 1;
        logs.push(format!(
            "    pushed event {}/{} (version {})",
            offset + 1,
            new_events_total,
            event.version
        ));
    }

    let remote_archived = remote_state
        .as_ref()
        .map(|state| state.archived)
        .unwrap_or(false);
    let mut archive_updated = false;
    if aggregate.archived != remote_archived {
        client
            .set_aggregate_archive(token, aggregate_type, aggregate_id, aggregate.archived)
            .with_context(|| {
                format!(
                    "failed to update archive status for '{}::{}' on remote endpoint",
                    aggregate_type, aggregate_id
                )
            })?;
        archive_updated = true;
        logs.push(format!(
            "  updated archive status to {}",
            if aggregate.archived {
                "archived"
            } else {
                "active"
            }
        ));
    }

    if wrote_events == 0 && !archive_updated {
        logs.push("  aggregate already synchronised".to_string());
    }

    Ok(PushAggregateStats {
        events_pushed: wrote_events,
        aggregate_updated: wrote_events > 0 || archive_updated,
        archive_updated,
    })
}

fn pull_domain(config_path: Option<PathBuf>, args: DomainSyncArgs) -> Result<()> {
    let domain = normalize_domain_name(&args.domain)?;
    let (config, _) = load_or_default(config_path)?;
    ensure_existing_domain(&config, &domain)?;

    let remote = load_remote_endpoint_for_domain(&config, &domain)?;
    let client = ServerClient::with_addr_and_tenant(
        remote.connect_addr.clone(),
        Some(remote.tenant.clone()),
    );

    let remote_aggregates_list = collect_remote_aggregates(
        &client,
        &remote.token,
        args.aggregate.as_deref(),
        args.aggregate_id.as_deref(),
    )?;
    if remote_aggregates_list.is_empty() {
        println!("Remote domain has no aggregates matching the specified criteria.");
        return Ok(());
    }

    let mut domain_config = config.clone();
    domain_config.domain = domain.clone();

    ensure_domain_stopped(&domain_config)?;

    let store = Arc::new(EventStore::open(
        domain_config.event_store_path(),
        domain_config.encryption_key()?,
        domain_config.snowflake_worker_id,
    )?);

    let total_remote = remote_aggregates_list.len();
    let remote_aggregates = Arc::new(remote_aggregates_list);
    let concurrency = resolve_concurrency(args.concurrency, total_remote);

    let next_index = Arc::new(AtomicUsize::new(0));
    let failure_flag = Arc::new(AtomicBool::new(false));
    let output_lock = Arc::new(Mutex::new(()));
    let events_counter = Arc::new(AtomicUsize::new(0));
    let aggregates_counter = Arc::new(AtomicUsize::new(0));
    let archive_counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let store = Arc::clone(&store);
        let remote_aggregates = Arc::clone(&remote_aggregates);
        let token = remote.token.clone();
        let connect_addr = remote.connect_addr.clone();
        let remote_tenant = remote.tenant.clone();
        let next_index = Arc::clone(&next_index);
        let failure_flag = Arc::clone(&failure_flag);
        let output_lock = Arc::clone(&output_lock);
        let events_counter = Arc::clone(&events_counter);
        let aggregates_counter = Arc::clone(&aggregates_counter);
        let archive_counter = Arc::clone(&archive_counter);
        let total = total_remote;

        handles.push(thread::spawn(move || -> Result<()> {
            let client =
                ServerClient::with_addr_and_tenant(connect_addr, Some(remote_tenant.clone()));
            loop {
                if failure_flag.load(Ordering::Relaxed) {
                    break;
                }
                let idx = next_index.fetch_add(1, Ordering::Relaxed);
                if idx >= remote_aggregates.len() {
                    break;
                }
                let remote_state = &remote_aggregates[idx];
                let mut logs = Vec::new();
                match process_pull_aggregate(
                    &mut logs,
                    store.as_ref(),
                    &client,
                    token.as_str(),
                    remote_state,
                    idx,
                    total,
                ) {
                    Ok(stats) => {
                        {
                            let _guard = output_lock.lock().unwrap();
                            for line in logs {
                                println!("{}", line);
                            }
                        }
                        if stats.events_imported > 0 {
                            events_counter.fetch_add(stats.events_imported, Ordering::Relaxed);
                        }
                        if stats.aggregate_modified {
                            aggregates_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        if stats.archive_updated {
                            archive_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(err) => {
                        {
                            let _guard = output_lock.lock().unwrap();
                            for line in logs {
                                println!("{}", line);
                            }
                        }
                        failure_flag.store(true, Ordering::Relaxed);
                        return Err(err);
                    }
                }
            }
            Ok(())
        }));
    }

    let mut first_err: Option<anyhow::Error> = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(panic) => {
                let message = if let Some(msg) = panic.downcast_ref::<&str>() {
                    msg.to_string()
                } else if let Some(msg) = panic.downcast_ref::<String>() {
                    msg.clone()
                } else {
                    "unknown panic".to_string()
                };
                if first_err.is_none() {
                    first_err = Some(anyhow!("pull worker panicked: {}", message));
                }
            }
        }
    }
    if let Some(err) = first_err {
        return Err(err);
    }

    let events_imported = events_counter.load(Ordering::Relaxed);
    let aggregates_modified = aggregates_counter.load(Ordering::Relaxed);
    let archive_updates = archive_counter.load(Ordering::Relaxed);

    if aggregates_modified == 0 && archive_updates == 0 {
        println!(
            "Local domain '{}' is already synchronised with remote {}.",
            domain, remote.connect_addr
        );
    } else {
        if events_imported > 0 {
            println!(
                "Imported {} event(s) across {} aggregate(s) from remote {}.",
                events_imported, aggregates_modified, remote.connect_addr
            );
        }
        if archive_updates > 0 {
            println!(
                "Updated archive status for {} aggregate(s) from remote {}.",
                archive_updates, remote.connect_addr
            );
        }
        if events_imported == 0 && archive_updates > 0 {
            println!("Archive status synchronised for domain '{}'.", domain);
        }
    }
    Ok(())
}

struct PullAggregateStats {
    events_imported: usize,
    aggregate_modified: bool,
    archive_updated: bool,
}

fn process_pull_aggregate(
    logs: &mut Vec<String>,
    store: &EventStore,
    client: &ServerClient,
    token: &str,
    remote_state: &AggregateState,
    index: usize,
    total: usize,
) -> Result<PullAggregateStats> {
    let aggregate_type = remote_state.aggregate_type.as_str();
    let aggregate_id = remote_state.aggregate_id.as_str();

    logs.push(format!(
        "Pulling aggregate {}/{}: {}::{}",
        index + 1,
        total,
        aggregate_type,
        aggregate_id
    ));

    let remote_version = remote_state.version;
    let remote_root = remote_state.merkle_root.clone();

    let local_version = match store.aggregate_version(aggregate_type, aggregate_id) {
        Ok(Some(version)) => version,
        Ok(None) => 0,
        Err(err) => return Err(err.into()),
    };

    if local_version > remote_version {
        bail!(
            "local aggregate '{}::{}' is ahead of remote ({} events vs {}); refusing to overwrite",
            aggregate_type,
            aggregate_id,
            local_version,
            remote_version
        );
    }

    let mut combined_hashes = if local_version > 0 {
        store
            .event_hashes(aggregate_type, aggregate_id)?
            .ok_or_else(|| {
                anyhow!(
                    "local aggregate metadata missing for '{}::{}'",
                    aggregate_type,
                    aggregate_id
                )
            })?
    } else {
        Vec::new()
    };

    if combined_hashes.len() != local_version as usize {
        bail!(
            "local aggregate '{}::{}' has inconsistent metadata (expected {} hashes, found {})",
            aggregate_type,
            aggregate_id,
            local_version,
            combined_hashes.len()
        );
    }

    let new_events_total = (remote_version - local_version) as usize;
    let new_events = if new_events_total > 0 {
        client
            .list_events_since(
                token,
                aggregate_type,
                aggregate_id,
                local_version,
                remote_state.archived,
            )
            .with_context(|| {
                format!(
                    "failed to fetch remote events for '{}::{}' starting at version {}",
                    aggregate_type,
                    aggregate_id,
                    local_version + 1
                )
            })?
    } else {
        Vec::new()
    };

    if new_events.len() != new_events_total {
        bail!(
            "remote aggregate '{}::{}' returned {} new event(s) but expected {} to reach version {}",
            aggregate_type,
            aggregate_id,
            new_events.len(),
            new_events_total,
            remote_version
        );
    }

    combined_hashes.extend(new_events.iter().map(|event| event.hash.clone()));
    let expected_root = compute_merkle_root(&combined_hashes);
    if expected_root != remote_root {
        bail!(
            "remote aggregate '{}::{}' diverged (expected merkle root {}, found {})",
            aggregate_type,
            aggregate_id,
            expected_root,
            remote_root
        );
    }

    let mut imported = 0usize;
    for (offset, event) in new_events.iter().enumerate() {
        store
            .append_imported_event(event.clone())
            .with_context(|| {
                format!(
                    "failed to import remote event version {} for '{}::{}'",
                    event.version, aggregate_type, aggregate_id
                )
            })?;
        imported += 1;
        logs.push(format!(
            "    imported event {}/{} (version {})",
            offset + 1,
            new_events_total,
            event.version
        ));
    }

    let mut archive_updated = false;
    if remote_state.archived {
        let local_archived = match store.get_aggregate_state(aggregate_type, aggregate_id) {
            Ok(state) => state.archived,
            Err(EventError::AggregateNotFound) => false,
            Err(err) => return Err(err.into()),
        };
        if !local_archived {
            store
                .set_archive(aggregate_type, aggregate_id, true, None)
                .with_context(|| {
                    format!(
                        "failed to archive aggregate '{}::{}' locally",
                        aggregate_type, aggregate_id
                    )
                })?;
            archive_updated = true;
            logs.push("  updated archive status to archived".to_string());
        }
    } else if let Ok(state) = store.get_aggregate_state(aggregate_type, aggregate_id) {
        if state.archived {
            store
                .set_archive(aggregate_type, aggregate_id, false, None)
                .with_context(|| {
                    format!(
                        "failed to unarchive aggregate '{}::{}' locally",
                        aggregate_type, aggregate_id
                    )
                })?;
            archive_updated = true;
            logs.push("  updated archive status to active".to_string());
        }
    }

    if imported == 0 && !remote_state.archived {
        if let Ok(state) = store.get_aggregate_state(aggregate_type, aggregate_id) {
            if !state.archived {
                logs.push("  aggregate already synchronised".to_string());
            }
        }
    }

    Ok(PullAggregateStats {
        events_imported: imported,
        aggregate_modified: imported > 0,
        archive_updated,
    })
}

fn push_schema(config_path: Option<PathBuf>, args: SchemaSyncArgs) -> Result<()> {
    let domain = normalize_domain_name(&args.domain)?;
    let (config, _) = load_or_default(config_path)?;
    ensure_existing_domain(&config, &domain)?;

    let remote = load_remote_endpoint_for_domain(&config, &domain)?;
    let client = ServerClient::with_addr_and_tenant(
        remote.connect_addr.clone(),
        Some(remote.tenant.clone()),
    );

    let mut domain_config = config.clone();
    domain_config.domain = domain.clone();
    ensure_domain_stopped(&domain_config)?;

    let manager = SchemaManager::load(domain_config.schema_store_path())?;
    let snapshot = manager.snapshot();

    client
        .replace_schemas(&remote.token, &snapshot)
        .with_context(|| {
            format!(
                "failed to push schemas for domain '{}' to remote {}",
                domain, remote.connect_addr
            )
        })?;

    println!(
        "Pushed {} schema(s) for domain '{}' to remote {}.",
        snapshot.len(),
        domain,
        remote.connect_addr
    );
    Ok(())
}

fn pull_schema(config_path: Option<PathBuf>, args: SchemaSyncArgs) -> Result<()> {
    let domain = normalize_domain_name(&args.domain)?;
    let (config, _) = load_or_default(config_path)?;
    ensure_existing_domain(&config, &domain)?;

    let remote = load_remote_endpoint_for_domain(&config, &domain)?;
    let client = ServerClient::with_addr_and_tenant(
        remote.connect_addr.clone(),
        Some(remote.tenant.clone()),
    );

    let remote_snapshot = client.list_schemas(&remote.token).with_context(|| {
        format!(
            "failed to fetch remote schemas for domain '{}' from {}",
            domain, remote.connect_addr
        )
    })?;

    let mut domain_config = config.clone();
    domain_config.domain = domain.clone();
    ensure_domain_stopped(&domain_config)?;

    let manager = SchemaManager::load(domain_config.schema_store_path())?;
    let count = remote_snapshot.len();
    manager.replace_all(remote_snapshot)?;

    println!(
        "Pulled {} schema(s) for domain '{}' from remote {}.",
        count, domain, remote.connect_addr
    );
    Ok(())
}

fn parse_domain_sync_args(argv: Vec<String>) -> Result<DomainSyncArgs> {
    if argv.is_empty() {
        bail!("domain name must be provided");
    }
    let mut args = Vec::with_capacity(argv.len() + 1);
    args.push("dbx-domain-sync".to_string());
    args.extend(argv);
    DomainSyncArgs::try_parse_from(args).map_err(|err| err.into())
}

fn ensure_existing_domain(config: &Config, domain: &str) -> Result<PathBuf> {
    let root = domain_data_dir_for(config, domain);
    if !root.exists() {
        bail!(
            "domain '{}' does not exist; run `dbx checkout {} --create` first",
            domain,
            domain
        );
    }
    Ok(root)
}

fn domain_data_dir_for(config: &Config, domain: &str) -> PathBuf {
    config.domain_data_dir_for(domain)
}

fn load_remote_endpoint_for_domain(config: &Config, domain: &str) -> Result<DomainRemoteEndpoint> {
    let path = remote_config_path(domain_data_dir_for(config, domain).as_path());
    let encryptor = config.encryption_key()?;
    let remote = load_remote_config(&path, encryptor.as_ref())?;
    let connect_addr = remote.connect_addr.ok_or_else(|| {
        anyhow::anyhow!(
            "remote endpoint not configured for domain '{}'; run `dbx checkout {} --remote <addr> --token <token>`",
            domain,
            domain
        )
    })?;
    let token = remote.token.ok_or_else(|| {
        anyhow::anyhow!(
            "remote token not configured for domain '{}'; run `dbx checkout {} --token <token>`",
            domain,
            domain
        )
    })?;
    let tenant = remote
        .tenant
        .clone()
        .unwrap_or(normalize_tenant_id(domain)?);
    Ok(DomainRemoteEndpoint {
        connect_addr,
        token,
        tenant,
    })
}

fn collect_local_aggregates(
    store: &EventStore,
    aggregate: Option<&str>,
    aggregate_id: Option<&str>,
) -> Result<Vec<AggregateState>> {
    match (aggregate, aggregate_id) {
        (Some(agg), Some(id)) => {
            ensure_snake_case("aggregate_type", agg)?;
            ensure_aggregate_id(id)?;
            match store.get_aggregate_state(agg, id) {
                Ok(state) => Ok(vec![state]),
                Err(EventError::AggregateNotFound) => bail!(
                    "aggregate '{}::{}' does not exist in the local store",
                    agg,
                    id
                ),
                Err(err) => Err(err.into()),
            }
        }
        (Some(agg), None) => {
            ensure_snake_case("aggregate_type", agg)?;
            let aggregates = store.aggregates_paginated_with_transform(
                0,
                None,
                None,
                AggregateQueryScope::IncludeArchived,
                |state| {
                    if state.aggregate_type == agg {
                        Some(state)
                    } else {
                        None
                    }
                },
            );
            Ok(aggregates)
        }
        (None, Some(_)) => bail!("--id requires --aggregate"),
        (None, None) => Ok(store.aggregates_paginated_with_transform(
            0,
            None,
            None,
            AggregateQueryScope::IncludeArchived,
            |state| Some(state),
        )),
    }
}

fn collect_remote_aggregates(
    client: &ServerClient,
    token: &str,
    aggregate: Option<&str>,
    aggregate_id: Option<&str>,
) -> Result<Vec<AggregateState>> {
    match (aggregate, aggregate_id) {
        (Some(agg), Some(id)) => {
            ensure_snake_case("aggregate_type", agg)?;
            ensure_aggregate_id(id)?;
            let state = client
                .get_aggregate(token, agg, id)
                .with_context(|| format!("failed to fetch remote aggregate '{}::{}'", agg, id))?;
            match state {
                Some(state) => Ok(vec![state]),
                None => bail!("remote aggregate '{}::{}' not found", agg, id),
            }
        }
        (Some(agg), None) => {
            ensure_snake_case("aggregate_type", agg)?;
            let filter = Some(build_aggregate_filter_expr(agg)?);
            client.list_aggregates(token, filter.as_deref(), true, false)
        }
        (None, Some(_)) => bail!("--id requires --aggregate"),
        (None, None) => client.list_aggregates(token, None, true, false),
    }
}

fn resolve_concurrency(requested: Option<usize>, total_tasks: usize) -> usize {
    let default = thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    let desired = requested.unwrap_or(default).max(1);
    let limit = total_tasks.max(1);
    desired.min(limit)
}

fn build_aggregate_filter_expr(aggregate: &str) -> Result<String> {
    let value = serde_json::to_string(aggregate)?;
    Ok(format!("aggregate_type = {}", value))
}

fn update_domain_remote_config(
    config: &Config,
    remote: Option<&str>,
    remote_port: Option<u16>,
    token: Option<&str>,
    tenant: Option<&str>,
) -> Result<()> {
    let domain_dir = config.domain_data_dir();
    if !domain_dir.exists() {
        fs::create_dir_all(&domain_dir)?;
    }
    let path = remote_config_path(domain_dir.as_path());
    let encryptor = config.encryption_key()?;
    let mut current = load_remote_config(&path, encryptor.as_ref())?;
    let mut changed = false;

    if let Some(raw_remote) = remote {
        let trimmed = raw_remote.trim();
        let normalized = if trimmed.is_empty() {
            None
        } else {
            Some(normalize_remote_addr(trimmed, remote_port)?)
        };
        if current.connect_addr != normalized {
            current.connect_addr = normalized;
            changed = true;
        }
    } else if let Some(port) = remote_port {
        if let Some(existing) = current.connect_addr.clone() {
            let normalized = normalize_remote_addr(&existing, Some(port))?;
            if Some(normalized.clone()) != current.connect_addr {
                current.connect_addr = Some(normalized);
                changed = true;
            }
        } else {
            bail!("remote address must be provided before updating the port (use --remote)");
        }
    }

    if let Some(raw_token) = token {
        let trimmed = raw_token.trim();
        let normalized = if trimmed.is_empty() {
            None
        } else {
            ensure_jwt_token_format(trimmed)?;
            Some(trimmed.to_string())
        };
        if current.token != normalized {
            current.token = normalized;
            changed = true;
        }
    }

    if let Some(raw_tenant) = tenant {
        let trimmed = raw_tenant.trim();
        let normalized = if trimmed.is_empty() {
            None
        } else {
            Some(normalize_tenant_id(trimmed)?)
        };
        if current.tenant != normalized {
            current.tenant = normalized;
            changed = true;
        }
    }

    if changed {
        save_remote_config(&path, &current, encryptor.as_ref())?;
        println!(
            "Updated remote configuration for domain '{}'.",
            config.active_domain()
        );
    }
    Ok(())
}

fn normalize_remote_addr(raw: &str, override_port: Option<u16>) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("remote address cannot be empty");
    }

    if let Some(port) = override_port {
        return Ok(normalize_with_port(trimmed, port)?);
    }

    if trimmed.parse::<SocketAddr>().is_ok() {
        return Ok(trimmed.to_string());
    }
    if trimmed.starts_with('[') {
        bail!(
            "remote address '{}' must include a port (e.g. [::1]:{})",
            trimmed,
            DEFAULT_SOCKET_PORT
        );
    }
    if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
        return Ok(match ip {
            std::net::IpAddr::V4(_) => format!("{}:{}", trimmed, DEFAULT_SOCKET_PORT),
            std::net::IpAddr::V6(_) => format!("[{}]:{}", trimmed, DEFAULT_SOCKET_PORT),
        });
    }
    if let Some((host, port)) = trimmed.rsplit_once(':') {
        if !host.is_empty() && port.parse::<u16>().is_ok() {
            return Ok(trimmed.to_string());
        }
    }
    Ok(format!("{}:{}", trimmed, DEFAULT_SOCKET_PORT))
}

fn normalize_with_port(host: &str, port: u16) -> Result<String> {
    if let Ok(addr) = host.parse::<SocketAddr>() {
        return Ok(match addr.ip() {
            std::net::IpAddr::V4(ip) => format!("{}:{}", ip, port),
            std::net::IpAddr::V6(ip) => format!("[{}]:{}", ip, port),
        });
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(match ip {
            std::net::IpAddr::V4(ip) => format!("{}:{}", ip, port),
            std::net::IpAddr::V6(ip) => format!("[{}]:{}", ip, port),
        });
    }
    if host.starts_with('[') {
        if let Some(end) = host.find(']') {
            let inner = &host[1..end];
            return Ok(format!("[{}]:{}", inner, port));
        } else {
            bail!(
                "remote address '{}' must include a closing bracket (e.g. [::1])",
                host
            );
        }
    }
    if let Some((base, _)) = host.rsplit_once(':') {
        if base.find(':').is_none() {
            return Ok(format!("{}:{}", base, port));
        }
    }
    Ok(format!("{}:{}", host, port))
}

fn ensure_jwt_token_format(token: &str) -> Result<()> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        bail!("remote token must be a JWT (expected three segments separated by '.')");
    }
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            bail!("remote token segments cannot be empty");
        }
        if part.chars().any(|ch| ch.is_whitespace()) {
            bail!("remote token must not contain whitespace");
        }
        if index < 2 {
            decode_jwt_segment(part)?;
        }
    }
    Ok(())
}

fn decode_jwt_segment(segment: &str) -> Result<()> {
    if URL_SAFE_NO_PAD.decode(segment).is_ok() {
        return Ok(());
    }
    let mut padded = segment.to_string();
    while padded.len() % 4 != 0 {
        padded.push('=');
    }
    URL_SAFE
        .decode(padded.as_bytes())
        .map(|_| ())
        .map_err(|err| anyhow!("remote token segment is not valid base64url: {}", err))
}

fn remote_config_path(domain_dir: &Path) -> PathBuf {
    domain_dir.join("remote.json")
}

fn load_remote_config(path: &Path, encryptor: Option<&Encryptor>) -> Result<DomainRemoteConfig> {
    if !path.exists() {
        return Ok(DomainRemoteConfig::default());
    }
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(DomainRemoteConfig::default());
    }

    if let Some(enc) = encryptor {
        if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
            if let Some(ciphertext) = encryption::extract_encrypted_value(&value) {
                let decrypted = enc.decrypt_from_str(ciphertext).with_context(|| {
                    format!(
                        "failed to decrypt {} using data encryption key",
                        path.display()
                    )
                })?;
                let config = serde_json::from_slice(&decrypted).with_context(|| {
                    format!(
                        "failed to parse decrypted remote configuration at {}",
                        path.display()
                    )
                })?;
                return Ok(config);
            }
        }
    }

    let config = serde_json::from_str(trimmed)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(config)
}

fn save_remote_config(
    path: &Path,
    remote: &DomainRemoteConfig,
    encryptor: Option<&Encryptor>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let contents = if let Some(enc) = encryptor {
        let payload = serde_json::to_vec(remote)
            .with_context(|| "failed to serialize remote configuration")?;
        let ciphertext = enc
            .encrypt_to_string(&payload)
            .with_context(|| "failed to encrypt remote configuration")?;
        let wrapped = encryption::wrap_encrypted_value(ciphertext);
        serde_json::to_string_pretty(&wrapped)?
    } else {
        serde_json::to_string_pretty(remote)?
    };
    fs::write(path, contents)?;
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
            target_store.append_imported_event(event)?;
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
    use tempfile::tempdir;

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
            create: false,
            delete: false,
            force: false,
            remote_addr: None,
            remote_port: None,
            remote_token: None,
            remote_tenant: None,
        };
        let result = resolve_checkout_domain(&args).expect("flag domain should resolve");
        assert_eq!(result, "herds");
    }

    #[test]
    fn resolve_domain_from_positional() {
        let args = DomainCheckoutArgs {
            flag_domain: None,
            positional_domain: Some("Herds_01".to_string()),
            create: false,
            delete: false,
            force: false,
            remote_addr: None,
            remote_port: None,
            remote_token: None,
            remote_tenant: None,
        };
        let result = resolve_checkout_domain(&args).expect("positional domain should resolve");
        assert_eq!(result, "herds_01");
    }

    #[test]
    fn resolve_conflicting_inputs_errors() {
        let args = DomainCheckoutArgs {
            flag_domain: Some("alpha".to_string()),
            positional_domain: Some("beta".to_string()),
            create: false,
            delete: false,
            force: false,
            remote_addr: None,
            remote_port: None,
            remote_token: None,
            remote_tenant: None,
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
    fn checkout_requires_create_for_new_domain() {
        let temp = tempdir().expect("temp dir");
        let config_path = temp.path().join("config.toml");

        let mut cfg = Config::default();
        cfg.data_dir = temp.path().join("data");
        cfg.ensure_data_dir().expect("create default data dir");
        cfg.save(&config_path).expect("write config");

        let args = DomainCheckoutArgs {
            flag_domain: Some("herds".to_string()),
            positional_domain: None,
            create: false,
            delete: false,
            force: false,
            remote_addr: None,
            remote_port: None,
            remote_token: None,
            remote_tenant: None,
        };

        let err = checkout(Some(config_path.clone()), args)
            .expect_err("checkout should fail when domain is missing");
        let message = err.to_string();
        assert!(
            message.contains("does not exist"),
            "unexpected error: {message}"
        );

        let (reloaded, _) =
            load_or_default(Some(config_path.clone())).expect("reload configuration");
        assert_eq!(reloaded.active_domain(), DEFAULT_DOMAIN_NAME);

        let mut candidate = reloaded.clone();
        candidate.domain = "herds".to_string();
        assert!(
            !candidate.domain_data_dir().exists(),
            "missing domain directory should not be created automatically"
        );
    }

    #[test]
    fn checkout_switches_to_existing_domain_without_create() {
        let temp = tempdir().expect("temp dir");
        let config_path = temp.path().join("config.toml");

        let mut cfg = Config::default();
        cfg.data_dir = temp.path().join("data");
        cfg.ensure_data_dir().expect("create default data dir");
        cfg.save(&config_path).expect("write config");

        let create_args = DomainCheckoutArgs {
            flag_domain: Some("Herds".to_string()),
            positional_domain: None,
            create: true,
            delete: false,
            force: false,
            remote_addr: None,
            remote_port: None,
            remote_token: None,
            remote_tenant: None,
        };
        checkout(Some(config_path.clone()), create_args).expect("domain creation should succeed");

        let default_args = DomainCheckoutArgs {
            flag_domain: Some(DEFAULT_DOMAIN_NAME.to_string()),
            positional_domain: None,
            create: false,
            delete: false,
            force: false,
            remote_addr: None,
            remote_port: None,
            remote_token: None,
            remote_tenant: None,
        };
        checkout(Some(config_path.clone()), default_args)
            .expect("switching to default domain should succeed");

        let switch_args = DomainCheckoutArgs {
            flag_domain: Some("herds".to_string()),
            positional_domain: None,
            create: false,
            delete: false,
            force: false,
            remote_addr: None,
            remote_port: None,
            remote_token: None,
            remote_tenant: None,
        };
        checkout(Some(config_path.clone()), switch_args)
            .expect("switching to an existing domain should succeed");

        let (reloaded, _) =
            load_or_default(Some(config_path.clone())).expect("reload configuration");
        assert_eq!(reloaded.active_domain(), "herds");

        let mut candidate = reloaded.clone();
        candidate.domain = "herds".to_string();
        assert!(
            candidate.domain_data_dir().exists(),
            "existing domain directory should be preserved"
        );
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
