use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use eventdbx::{
    config::{Config, load_or_default},
    error::EventError,
    store::{EventStore, SnapshotRecord},
    validation::{ensure_aggregate_id, ensure_snake_case},
};

use crate::commands::{aggregate::ensure_proxy_token, client::ServerClient};

#[derive(Subcommand)]
pub enum SnapshotsCommands {
    /// List snapshots with optional aggregate filters
    List(SnapshotsListArgs),
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

pub fn execute(config_path: Option<PathBuf>, command: SnapshotsCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    match command {
        SnapshotsCommands::List(args) => list_snapshots(&config, args),
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
        Err(EventError::Storage(message)) if is_lock_error(&message) => {
            let snapshots =
                proxy_list_snapshots_via_socket(config, aggregate, aggregate_id, version)?;
            print_snapshots(&snapshots, args.json)?;
        }
        Err(err) => return Err(err.into()),
    }

    Ok(())
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
            "aggregate_type={} aggregate_id={} version={} created_at={} comment=\"{}\"",
            snapshot.aggregate_type,
            snapshot.aggregate_id,
            snapshot.version,
            snapshot.created_at.to_rfc3339(),
            comment
        );
    }
    Ok(())
}

fn is_lock_error(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("lock file")
        || lower.contains("resource temporarily unavailable")
        || lower.contains("store lock")
        || lower.contains("store is in use")
}
