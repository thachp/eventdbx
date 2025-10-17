use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Result;
use clap::{Args, Subcommand};

use eventful::{
    config::load_or_default,
    merkle::compute_merkle_root,
    plugin::PluginManager,
    schema::SchemaManager,
    store::{self, AppendEvent, EventStore},
};

#[derive(Subcommand)]
pub enum AggregateCommands {
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
    /// Create a snapshot of the aggregate state
    Snapshot(AggregateSnapshotArgs),
    /// Archive an aggregate instance
    Archive(AggregateArchiveArgs),
    /// Restore an archived aggregate instance
    Restore(AggregateArchiveArgs),
    /// Commit staged events (no-op placeholder)
    Commit,
}

#[derive(Args)]
pub struct AggregateApplyArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Event type to append
    pub event: String,

    /// Event fields expressed as KEY=VALUE pairs
    #[arg(long = "field", value_parser = parse_key_value, value_name = "KEY=VALUE")]
    pub fields: Vec<KeyValue>,
}

#[derive(Args)]
pub struct AggregateGetArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Optional version to compute state at
    #[arg(long)]
    pub version: Option<u64>,

    /// Include event history in the output
    #[arg(long, default_value_t = false)]
    pub include_events: bool,
}

#[derive(Args)]
pub struct AggregateReplayArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Number of events to skip
    #[arg(long, default_value_t = 0)]
    pub skip: usize,

    /// Number of events to return
    #[arg(long)]
    pub take: Option<usize>,
}

#[derive(Args)]
pub struct AggregateVerifyArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,
}

#[derive(Args)]
pub struct AggregateSnapshotArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Optional comment to record with the snapshot
    #[arg(long)]
    pub comment: Option<String>,
}

#[derive(Args)]
pub struct AggregateArchiveArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Optional comment recorded with the action
    #[arg(long)]
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub fn execute(config_path: Option<PathBuf>, command: AggregateCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    match command {
        AggregateCommands::List => {
            let store = EventStore::open_read_only(config.event_store_path())?;
            for aggregate in store.aggregates() {
                println!(
                    "aggregate_type={} aggregate_id={} version={} merkle_root={} archived={}",
                    aggregate.aggregate_type,
                    aggregate.aggregate_id,
                    aggregate.version,
                    aggregate.merkle_root,
                    aggregate.archived
                );
            }
        }
        AggregateCommands::Get(args) => {
            let store = EventStore::open_read_only(config.event_store_path())?;
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

            let mut output = serde_json::json!({
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
            if config.restrict {
                schema_manager.validate_event(&args.aggregate, &args.event, &payload)?;
            }

            let store = EventStore::open(config.event_store_path())?;
            let plugins = PluginManager::from_config(&config)?;
            let record = store.append(AppendEvent {
                aggregate_type: args.aggregate,
                aggregate_id: args.aggregate_id,
                event_type: args.event,
                payload,
                issued_by: None,
            })?;

            println!("{}", serde_json::to_string_pretty(&record)?);

            if !plugins.is_empty() {
                let schema = schema_manager.get(&record.aggregate_type).ok();
                match store.get_aggregate_state(&record.aggregate_type, &record.aggregate_id) {
                    Ok(current_state) => {
                        if let Err(err) =
                            plugins.notify_event(&record, &current_state, schema.as_ref())
                        {
                            eprintln!("plugin notification failed: {}", err);
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "plugin notification skipped (failed to load state): {}",
                            err
                        );
                    }
                }
            }
        }
        AggregateCommands::Replay(args) => {
            let store = EventStore::open_read_only(config.event_store_path())?;
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
            let store = EventStore::open_read_only(config.event_store_path())?;
            let merkle_root = store.verify(&args.aggregate, &args.aggregate_id)?;
            println!(
                "aggregate_type={} aggregate_id={} merkle_root={}",
                args.aggregate, args.aggregate_id, merkle_root
            );
        }
        AggregateCommands::Snapshot(args) => {
            let store = EventStore::open(config.event_store_path())?;
            let snapshot =
                store.create_snapshot(&args.aggregate, &args.aggregate_id, args.comment.clone())?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        AggregateCommands::Archive(args) => {
            let store = EventStore::open(config.event_store_path())?;
            let meta = store.set_archive(
                &args.aggregate,
                &args.aggregate_id,
                true,
                args.comment.clone(),
            )?;
            println!(
                "aggregate_type={} aggregate_id={} archived={} comment={}",
                meta.aggregate_type,
                meta.aggregate_id,
                meta.archived,
                args.comment.unwrap_or_default()
            );
        }
        AggregateCommands::Restore(args) => {
            let store = EventStore::open(config.event_store_path())?;
            let meta = store.set_archive(
                &args.aggregate,
                &args.aggregate_id,
                false,
                args.comment.clone(),
            )?;
            println!(
                "aggregate_type={} aggregate_id={} archived={} comment={}",
                meta.aggregate_type,
                meta.aggregate_id,
                meta.archived,
                args.comment.unwrap_or_default()
            );
        }
        AggregateCommands::Commit => {
            println!("all events are already committed");
        }
    }

    Ok(())
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

    let merkle_root = compute_merkle_root(&hashes);
    (last_version, state, merkle_root)
}
