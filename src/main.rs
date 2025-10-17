mod config;
mod error;
mod schema;
mod server;
mod store;
mod token;

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand};
use serde_json::json;
use tracing::info;

use crate::{
    config::{ConfigUpdate, load_or_default},
    schema::{CreateSchemaInput, SchemaManager, SchemaUpdate},
    store::{AppendEvent, EventStore},
    token::{IssueTokenInput, TokenManager},
};

#[derive(Parser)]
#[command(author, version, about = "EventfulDB server CLI")]
struct Cli {
    /// Path to the configuration file. Defaults to ~/.config/eventful/config.toml
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the EventfulDB server
    Start(StartArgs),
    /// Update system configuration
    Config(ConfigArgs),
    /// Manage access tokens
    Token {
        #[command(subcommand)]
        command: TokenCommands,
    },
    /// Manage schemas
    Schema {
        #[command(subcommand)]
        command: SchemaCommands,
    },
    /// Manage aggregates
    Aggregate {
        #[command(subcommand)]
        command: AggregateCommands,
    },
}

#[derive(Args)]
struct StartArgs {
    /// Override the configured server port
    #[arg(long)]
    port: Option<u16>,

    /// Override the configured data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[derive(Args)]
struct ConfigArgs {
    #[arg(long)]
    port: Option<u16>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    master_key: Option<String>,

    #[arg(long)]
    memory_threshold: Option<usize>,

    #[arg(long, alias = "dek")]
    data_encryption_key: Option<String>,
}

#[derive(Subcommand)]
enum TokenCommands {
    /// Generate a new token
    Generate(TokenGenerateArgs),
    /// List configured tokens
    List,
    /// Revoke an active token
    Revoke {
        /// Token value to revoke
        token: String,
    },
    /// Refresh an existing token
    Refresh(TokenRefreshArgs),
}

#[derive(Args)]
struct TokenGenerateArgs {
    #[arg(long)]
    identifier_type: String,

    #[arg(long)]
    identifier_id: String,

    #[arg(long)]
    expiration: Option<u64>,

    #[arg(long)]
    limit: Option<u64>,

    #[arg(long, default_value_t = false)]
    keep_alive: bool,
}

#[derive(Args)]
struct TokenRefreshArgs {
    #[arg(long)]
    token: String,

    #[arg(long)]
    expiration: Option<u64>,

    #[arg(long)]
    limit: Option<u64>,
}

#[derive(Subcommand)]
enum SchemaCommands {
    /// Create a new schema definition
    Create(SchemaCreateArgs),
    /// Alter an existing schema definition
    Alter(SchemaAlterArgs),
    /// List available schemas
    List,
    /// Show a specific schema
    Show {
        /// Aggregate name
        aggregate: String,
    },
}

#[derive(Args)]
struct SchemaCreateArgs {
    #[arg(long)]
    aggregate: String,

    #[arg(long, value_delimiter = ',')]
    events: Vec<String>,

    #[arg(long)]
    snapshot_threshold: Option<u64>,
}

#[derive(Args)]
struct SchemaAlterArgs {
    /// Aggregate name
    aggregate: String,

    /// Optional event name to alter
    event: Option<String>,

    /// Set the snapshot threshold
    #[arg(long)]
    snapshot_threshold: Option<u64>,

    /// Lock or unlock the aggregate (or field when --field is specified)
    #[arg(long)]
    lock: Option<bool>,

    /// Field name to lock/unlock
    #[arg(long)]
    field: Option<String>,

    /// Add fields to an event definition
    #[arg(long, short = 'a', value_delimiter = ',')]
    add: Vec<String>,

    /// Remove fields from an event definition
    #[arg(long, short = 'r', value_delimiter = ',')]
    remove: Vec<String>,
}

#[derive(Subcommand)]
enum AggregateCommands {
    /// Apply an event to an aggregate instance
    Apply(AggregateApplyArgs),
    /// List aggregates in the store
    List,
    /// Retrieve the state of an aggregate
    Get(AggregateGetArgs),
    /// Replay events for an aggregate instance
    Replay(AggregateReplayArgs),
    /// Verify an aggregate's Merkle root
    Verify(AggregateVerifyArgs),
}

#[derive(Args)]
struct AggregateApplyArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Event type to append
    event: String,

    /// Event fields expressed as KEY=VALUE pairs
    #[arg(long = "field", value_parser = parse_key_value, value_name = "KEY=VALUE")]
    fields: Vec<KeyValue>,
}

#[derive(Args)]
struct AggregateGetArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Optional version to compute state at
    #[arg(long)]
    version: Option<u64>,

    /// Include event history in the output
    #[arg(long, default_value_t = false)]
    include_events: bool,
}

#[derive(Args)]
struct AggregateReplayArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Number of events to skip
    #[arg(long, default_value_t = 0)]
    skip: usize,

    /// Number of events to return
    #[arg(long)]
    take: Option<usize>,
}

#[derive(Args)]
struct AggregateVerifyArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) => start_command(cli.config, args).await?,
        Commands::Config(args) => config_command(cli.config, args)?,
        Commands::Token { command } => token_command(cli.config, command)?,
        Commands::Schema { command } => schema_command(cli.config, command)?,
        Commands::Aggregate { command } => aggregate_command(cli.config, command)?,
    }

    Ok(())
}

async fn start_command(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    if let Some(port) = args.port {
        config.apply_update(ConfigUpdate {
            port: Some(port),
            ..ConfigUpdate::default()
        });
    }

    if let Some(dir) = args.data_dir {
        config.apply_update(ConfigUpdate {
            data_dir: Some(dir),
            ..ConfigUpdate::default()
        });
    }

    config.ensure_data_dir()?;
    config.save(&path)?;

    server::run(config).await?;
    Ok(())
}

fn config_command(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    config.apply_update(ConfigUpdate {
        port: args.port,
        data_dir: args.data_dir,
        master_key: args.master_key,
        memory_threshold: args.memory_threshold,
        data_encryption_key: args.data_encryption_key,
    });

    config.ensure_data_dir()?;
    config.save(&path)?;

    info!("Configuration saved to {}", path.display());
    Ok(())
}

fn token_command(config_path: Option<PathBuf>, command: TokenCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = TokenManager::load(config.tokens_path())?;

    match command {
        TokenCommands::Generate(args) => {
            let record = manager.issue(IssueTokenInput {
                identifier_type: args.identifier_type,
                identifier_id: args.identifier_id,
                expiration_secs: args.expiration,
                limit: args.limit,
                keep_alive: args.keep_alive,
            })?;
            println!(
                "token={} identifier_type={} identifier_id={} expires_at={} remaining_writes={}",
                record.token,
                record.identifier_type,
                record.identifier_id,
                record
                    .expires_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "never".into()),
                record
                    .remaining_writes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unlimited".into())
            );
        }
        TokenCommands::List => {
            for record in manager.list() {
                println!(
                    "token={} status={:?} issued_at={} expires_at={} remaining_writes={}",
                    record.token,
                    record.status,
                    record.issued_at.to_rfc3339(),
                    record
                        .expires_at
                        .map(|ts| ts.to_rfc3339())
                        .unwrap_or_else(|| "never".into()),
                    record
                        .remaining_writes
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unlimited".into())
                );
            }
        }
        TokenCommands::Revoke { token } => {
            manager.revoke(&token)?;
            println!("token {} revoked", token);
        }
        TokenCommands::Refresh(args) => {
            let record = manager.refresh(&args.token, args.expiration, args.limit)?;
            println!(
                "token={} expires_at={} remaining_writes={}",
                record.token,
                record
                    .expires_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "never".into()),
                record
                    .remaining_writes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unlimited".into())
            );
        }
    }

    Ok(())
}

fn schema_command(config_path: Option<PathBuf>, command: SchemaCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = SchemaManager::load(config.schema_store_path())?;

    match command {
        SchemaCommands::Create(args) => {
            let schema = manager.create(CreateSchemaInput {
                aggregate: args.aggregate,
                events: args.events,
                snapshot_threshold: args.snapshot_threshold,
            })?;
            println!(
                "schema={} events={} snapshot_threshold={:?}",
                schema.aggregate,
                schema.events.len(),
                schema.snapshot_threshold
            );
        }
        SchemaCommands::Alter(args) => {
            let mut update = SchemaUpdate::default();

            if let Some(value) = args.snapshot_threshold {
                update.snapshot_threshold = Some(Some(value));
            }

            if args.event.is_none() && args.field.is_none() {
                if let Some(lock) = args.lock {
                    update.locked = Some(lock);
                }
            }

            if let Some(field) = args.field {
                if let Some(lock) = args.lock {
                    update.field_lock = Some((field, lock));
                } else {
                    return Err(anyhow::anyhow!(
                        "--lock must be provided when using --field"
                    ));
                }
            }

            if let Some(event) = args.event {
                if !args.add.is_empty() {
                    update
                        .event_add_fields
                        .insert(event.clone(), args.add.clone());
                }
                if !args.remove.is_empty() {
                    update
                        .event_remove_fields
                        .insert(event.clone(), args.remove.clone());
                }
                if args.add.is_empty() && args.remove.is_empty() {
                    return Err(anyhow::anyhow!(
                        "provide --add or --remove when specifying an event"
                    ));
                }
            } else if (!args.add.is_empty() || !args.remove.is_empty()) && args.event.is_none() {
                return Err(anyhow::anyhow!(
                    "--event must be provided when adding or removing fields"
                ));
            }

            let schema = manager.update(&args.aggregate, update)?;
            println!(
                "schema={} updated_at={} version_events={}",
                schema.aggregate,
                schema.updated_at.to_rfc3339(),
                schema.events.len()
            );
        }
        SchemaCommands::List => {
            for schema in manager.list() {
                println!(
                    "schema={} events={} locked={} snapshot_threshold={:?}",
                    schema.aggregate,
                    schema.events.len(),
                    schema.locked,
                    schema.snapshot_threshold
                );
            }
        }
        SchemaCommands::Show { aggregate } => {
            let schema = manager.get(&aggregate)?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
    }

    Ok(())
}

fn aggregate_command(config_path: Option<PathBuf>, command: AggregateCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = EventStore::open(config.event_store_path())?;

    match command {
        AggregateCommands::List => {
            for aggregate in store.aggregates() {
                println!(
                    "aggregate_type={} aggregate_id={} version={} merkle_root={}",
                    aggregate.aggregate_type,
                    aggregate.aggregate_id,
                    aggregate.version,
                    aggregate.merkle_root
                );
            }
        }
        AggregateCommands::Get(args) => {
            let mut state = store.get_aggregate_state(&args.aggregate, &args.aggregate_id)?;
            let mut events_cache = None;

            if args.version.is_some() || args.include_events {
                events_cache = Some(store.list_events(&args.aggregate, &args.aggregate_id)?);
            }

            if let Some(version) = args.version {
                let events = events_cache
                    .as_ref()
                    .expect("events cache should be populated");
                let (target_version, target_state, merkle_root) = state_at_version(events, version);
                state.version = target_version;
                state.state = target_state;
                state.merkle_root = merkle_root;
            }

            let mut output = json!({
                "aggregate_type": state.aggregate_type,
                "aggregate_id": state.aggregate_id,
                "version": state.version,
                "state": state.state,
                "merkle_root": state.merkle_root,
            });

            if args.include_events {
                let events = match events_cache.take() {
                    Some(events) => events,
                    None => store.list_events(&args.aggregate, &args.aggregate_id)?,
                };
                let filtered: Vec<_> = match args.version {
                    Some(version) => events
                        .into_iter()
                        .filter(|event| event.version <= version)
                        .collect(),
                    None => events,
                };
                output["events"] = serde_json::to_value(filtered)?;
            }

            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        AggregateCommands::Apply(args) => {
            let payload = collect_payload(args.fields);
            let schema_manager = SchemaManager::load(config.schema_store_path())?;
            let schema = schema_manager.get(&args.aggregate).ok();

            if let Some(schema) = schema {
                if schema.locked {
                    return Err(anyhow!(
                        "aggregate {} is locked for updates",
                        schema.aggregate
                    ));
                }

                for key in payload.keys() {
                    if schema.field_locks.contains(key) {
                        return Err(anyhow!(
                            "field {} is locked for aggregate {}",
                            key,
                            schema.aggregate
                        ));
                    }
                }

                let event_schema = schema.events.get(&args.event).ok_or_else(|| {
                    anyhow!(
                        "event {} is not defined for aggregate {}",
                        args.event,
                        schema.aggregate
                    )
                })?;

                if !event_schema.fields.is_empty() {
                    for key in payload.keys() {
                        if !event_schema.fields.contains(key) {
                            return Err(anyhow!(
                                "field {} is not permitted for event {}",
                                key,
                                args.event
                            ));
                        }
                    }
                }
            }

            let record = store.append(AppendEvent {
                aggregate_type: args.aggregate,
                aggregate_id: args.aggregate_id,
                event_type: args.event,
                payload,
                issued_by: None,
            })?;

            println!("{}", serde_json::to_string_pretty(&record)?);
        }
        AggregateCommands::Replay(args) => {
            let events = store.list_events(&args.aggregate, &args.aggregate_id)?;
            let iter = events.into_iter().skip(args.skip);
            let events: Vec<_> = if let Some(limit) = args.take {
                iter.take(limit).collect()
            } else {
                iter.collect()
            };

            for event in events {
                println!("{}", serde_json::to_string_pretty(&event)?);
            }
        }
        AggregateCommands::Verify(args) => {
            let merkle_root = store.verify(&args.aggregate, &args.aggregate_id)?;
            println!(
                "aggregate_type={} aggregate_id={} merkle_root={}",
                args.aggregate, args.aggregate_id, merkle_root
            );
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct KeyValue {
    key: String,
    value: String,
}

fn parse_key_value(raw: &str) -> Result<KeyValue, String> {
    let mut parts = raw.splitn(2, '=');
    let key = parts
        .next()
        .ok_or_else(|| "missing key".to_string())?
        .trim();
    let value = parts
        .next()
        .ok_or_else(|| "missing value".to_string())?
        .trim();

    if key.is_empty() {
        return Err("field key cannot be empty".to_string());
    }

    Ok(KeyValue {
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn collect_payload(fields: Vec<KeyValue>) -> BTreeMap<String, String> {
    let mut payload = BTreeMap::new();
    for kv in fields {
        payload.insert(kv.key, kv.value);
    }
    payload
}

fn state_at_version(
    events: &[store::EventRecord],
    version: u64,
) -> (u64, BTreeMap<String, String>, String) {
    let mut state = BTreeMap::new();
    let mut hashes = Vec::new();
    let mut last_version = 0;

    for event in events {
        if event.version > version {
            break;
        }
        last_version = event.version;
        hashes.push(event.hash.clone());
        for (key, value) in &event.payload {
            state.insert(key.clone(), value.clone());
        }
    }

    let merkle_root = store::compute_merkle_root(&hashes);
    (last_version, state, merkle_root)
}

#[cfg(test)]
mod tests {
    use super::parse_key_value;

    #[test]
    fn parses_key_value_pairs() {
        let pair = parse_key_value("name=Alice").unwrap();
        assert_eq!(pair.key, "name");
        assert_eq!(pair.value, "Alice");

        assert!(parse_key_value("invalid").is_err());
    }
}
