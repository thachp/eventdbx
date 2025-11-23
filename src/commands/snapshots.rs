use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use eventdbx::{
    config::{Config, load_or_default},
    error::EventError,
    snowflake::SnowflakeId,
    store::{EventStore, SnapshotRecord},
    validation::{ensure_aggregate_id, ensure_snake_case},
};
use serde_json;

use crate::commands::{aggregate::ensure_proxy_token, client::ServerClient, is_lock_error_message};

#[derive(Subcommand)]
pub enum SnapshotsCommands {
    /// List snapshots with optional aggregate filters
    List(SnapshotsListArgs),
    /// Create a snapshot of an aggregate state
    Create(SnapshotsCreateArgs),
    /// Fetch a snapshot by id
    Get(SnapshotsGetArgs),
}

#[derive(Args)]
pub struct SnapshotsListArgs {
    /// Aggregate type
    pub aggregate: Option<String>,

    /// Aggregate identifier
    #[arg(requires = "aggregate")]
    pub aggregate_id: Option<String>,

    /// Filter snapshots matching a specific aggregate version
    #[arg(long)]
    pub version: Option<u64>,

    /// Emit results as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args)]
pub struct SnapshotsCreateArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Optional comment to record with the snapshot
    #[arg(long)]
    pub comment: Option<String>,
}

#[derive(Args)]
pub struct SnapshotsGetArgs {
    /// Snapshot identifier (snowflake)
    pub snapshot_id: String,

    /// Emit result as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

pub fn execute(config_path: Option<PathBuf>, command: SnapshotsCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    match command {
        SnapshotsCommands::List(args) => list_snapshots(&config, args),
        SnapshotsCommands::Create(args) => create_snapshot(&config, args),
        SnapshotsCommands::Get(args) => get_snapshot(&config, args),
    }
}

fn list_snapshots(config: &Config, args: SnapshotsListArgs) -> Result<()> {
    if let Some(ref aggregate) = args.aggregate {
        ensure_snake_case("aggregate_type", aggregate)?;
    }
    if let Some(ref aggregate_id) = args.aggregate_id {
        ensure_aggregate_id(aggregate_id)?;
    }
    let version = args.version;
    let aggregate = args.aggregate.as_deref();
    let aggregate_id = args.aggregate_id.as_deref();

    match EventStore::open_read_only(config.event_store_path(), config.encryption_key()?) {
        Ok(store) => {
            let snapshots = store.list_snapshots(aggregate, aggregate_id, version)?;
            print_snapshots(&snapshots, args.json)?;
        }
        Err(EventError::Storage(message)) if is_lock_error_message(&message) => {
            let snapshots =
                proxy_list_snapshots_via_socket(config, aggregate, aggregate_id, version)?;
            print_snapshots(&snapshots, args.json)?;
        }
        Err(err) => return Err(err.into()),
    }

    Ok(())
}

fn create_snapshot(config: &Config, args: SnapshotsCreateArgs) -> Result<()> {
    ensure_snake_case("aggregate_type", &args.aggregate)?;
    ensure_aggregate_id(&args.aggregate_id)?;

    match EventStore::open(
        config.event_store_path(),
        config.encryption_key()?,
        config.snowflake_worker_id,
    ) {
        Ok(store) => {
            let snapshot =
                store.create_snapshot(&args.aggregate, &args.aggregate_id, args.comment.clone())?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Err(EventError::Storage(message)) if is_lock_error_message(&message) => {
            let snapshot = proxy_create_snapshot_via_socket(
                config,
                &args.aggregate,
                &args.aggregate_id,
                args.comment.as_deref(),
            )?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Err(err) => return Err(err.into()),
    }

    Ok(())
}

fn get_snapshot(config: &Config, args: SnapshotsGetArgs) -> Result<()> {
    let snapshot_id: SnowflakeId = args
        .snapshot_id
        .parse()
        .with_context(|| format!("snapshot_id {} is not a valid snowflake", args.snapshot_id))?;

    let snapshot =
        match EventStore::open_read_only(config.event_store_path(), config.encryption_key()?) {
            Ok(store) => store.find_snapshot_by_id(snapshot_id)?,
            Err(EventError::Storage(message)) if is_lock_error_message(&message) => {
                proxy_get_snapshot_via_socket(config, snapshot_id)?
            }
            Err(err) => return Err(err.into()),
        };

    let Some(snapshot) = snapshot else {
        bail!("snapshot {} not found", snapshot_id);
    };

    print_snapshot(&snapshot, args.json)
}

fn proxy_list_snapshots_via_socket(
    config: &Config,
    aggregate: Option<&str>,
    aggregate_id: Option<&str>,
    version: Option<u64>,
) -> Result<Vec<SnapshotRecord>> {
    let token = ensure_proxy_token(config, None)?;
    let client = ServerClient::new(config)?;
    client
        .list_snapshots(&token, aggregate, aggregate_id, version)
        .with_context(|| {
            format!(
                "failed to list snapshots via running server socket {}",
                config.socket.bind_addr
            )
        })
}

fn proxy_get_snapshot_via_socket(
    config: &Config,
    snapshot_id: SnowflakeId,
) -> Result<Option<SnapshotRecord>> {
    let token = ensure_proxy_token(config, None)?;
    let client = ServerClient::new(config)?;
    let snapshots = client
        .list_snapshots(&token, None, None, None)
        .with_context(|| {
            format!(
                "failed to list snapshots via running server socket {}",
                config.socket.bind_addr
            )
        })?;
    Ok(snapshots
        .into_iter()
        .find(|snapshot| snapshot.snapshot_id == Some(snapshot_id)))
}

fn proxy_create_snapshot_via_socket(
    config: &Config,
    aggregate: &str,
    aggregate_id: &str,
    comment: Option<&str>,
) -> Result<SnapshotRecord> {
    let token = ensure_proxy_token(config, None)?;
    let client = ServerClient::new(config)?;
    client
        .create_snapshot(&token, aggregate, aggregate_id, comment)
        .with_context(|| {
            format!(
                "failed to create snapshot via running server socket {}",
                config.socket.bind_addr
            )
        })
}

fn print_snapshots(snapshots: &[SnapshotRecord], json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(snapshots)?);
        return Ok(());
    }

    if snapshots.is_empty() {
        println!("no snapshots found");
        return Ok(());
    }

    for snapshot in snapshots {
        let comment = snapshot.comment.as_deref().unwrap_or_default();
        println!(
            "snapshot_id={} aggregate_type={} aggregate_id={} version={} created_at={} comment=\"{}\"",
            snapshot_id_display(snapshot),
            snapshot.aggregate_type,
            snapshot.aggregate_id,
            snapshot.version,
            snapshot.created_at.to_rfc3339(),
            comment
        );
    }
    Ok(())
}

fn print_snapshot(snapshot: &SnapshotRecord, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(snapshot)?);
        return Ok(());
    }
    let comment = snapshot.comment.as_deref().unwrap_or_default();
    println!(
        "snapshot_id={} aggregate_type={} aggregate_id={} version={} created_at={} comment=\"{}\"\nstate={}\nmerkle_root={}",
        snapshot_id_display(snapshot),
        snapshot.aggregate_type,
        snapshot.aggregate_id,
        snapshot.version,
        snapshot.created_at.to_rfc3339(),
        comment,
        serde_json::to_string_pretty(&snapshot.state)?,
        snapshot.merkle_root,
    );
    Ok(())
}

fn snapshot_id_display(snapshot: &SnapshotRecord) -> String {
    snapshot
        .snapshot_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
