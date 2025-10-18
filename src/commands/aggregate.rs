use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value};

use eventdbx::{
    config::load_or_default,
    merkle::compute_merkle_root,
    plugin::PluginManager,
    schema::SchemaManager,
    store::{self, ActorClaims, AppendEvent, EventStore, payload_to_map},
};

#[derive(Subcommand)]
pub enum AggregateCommands {
    /// Apply an event to an aggregate instance
    Apply(AggregateApplyArgs),
    /// Create an empty aggregate (no events)
    Create(AggregateIdentityArgs),
    /// List aggregates in the store
    List(AggregateListArgs),
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
    /// Remove an aggregate that has no events
    Remove(AggregateIdentityArgs),
    /// Commit staged events read from stdin (JSON array or NDJSON)
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

    /// Stage the event for a later commit instead of writing immediately
    #[arg(long, default_value_t = false)]
    pub stage: bool,
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

#[derive(Args)]
pub struct AggregateListArgs {
    /// Number of aggregates to skip
    #[arg(long, default_value_t = 0)]
    pub skip: usize,

    /// Maximum number of aggregates to return
    #[arg(long)]
    pub take: Option<usize>,

    /// Show staged events instead of persisted aggregates
    #[arg(long, default_value_t = false)]
    pub stage: bool,
}

#[derive(Args)]
pub struct AggregateIdentityArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,
}

pub fn execute(config_path: Option<PathBuf>, command: AggregateCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    match command {
        AggregateCommands::Create(args) => {
            let store = EventStore::open(config.event_store_path())?;
            store.create_aggregate(&args.aggregate, &args.aggregate_id)?;
            println!(
                "aggregate_type={} aggregate_id={} created",
                args.aggregate, args.aggregate_id
            );
        }
        AggregateCommands::List(args) => {
            if args.stage {
                let staging_path = config.staging_path();
                let staged_events = load_staged_events(staging_path.as_path())?;
                if staged_events.is_empty() {
                    println!("no staged events");
                } else {
                    for event in staged_events {
                        println!("{}", serde_json::to_string_pretty(&event)?);
                    }
                }
                return Ok(());
            }

            let store = EventStore::open_read_only(config.event_store_path())?;
            let take = args.take.or(Some(config.list_page_size));
            for aggregate in store.aggregates_paginated(args.skip, take) {
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
        AggregateCommands::Remove(args) => {
            let store = EventStore::open(config.event_store_path())?;
            store.remove_aggregate(&args.aggregate, &args.aggregate_id)?;
            println!(
                "aggregate_type={} aggregate_id={} removed",
                args.aggregate, args.aggregate_id
            );
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
            let AggregateApplyArgs {
                aggregate,
                aggregate_id,
                event,
                fields,
                stage,
            } = args;
            let payload = collect_payload(fields);
            let schema_manager = SchemaManager::load(config.schema_store_path())?;
            if config.restrict {
                let map = payload_to_map(&payload);
                schema_manager.validate_event(&aggregate, &event, &map)?;
            }

            if stage {
                let store = EventStore::open(config.event_store_path())?;
                {
                    let mut tx = store.transaction()?;
                    tx.append(AppendEvent {
                        aggregate_type: aggregate.clone(),
                        aggregate_id: aggregate_id.clone(),
                        event_type: event.clone(),
                        payload: payload.clone(),
                        issued_by: None,
                    })?;
                }

                let staged_event = StagedEvent {
                    aggregate,
                    aggregate_id,
                    event,
                    payload,
                    issued_by: None,
                };
                let staging_path = config.staging_path();
                append_staged_event(staging_path.as_path(), staged_event)?;
                println!("event staged for later commit");
                return Ok(());
            }

            let store = EventStore::open(config.event_store_path())?;
            let plugins = PluginManager::from_config(&config)?;
            let record = store.append(AppendEvent {
                aggregate_type: aggregate.clone(),
                aggregate_id: aggregate_id.clone(),
                event_type: event.clone(),
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
            let staging_path = config.staging_path();
            let staged_events = load_staged_events(staging_path.as_path())?;
            if staged_events.is_empty() {
                println!("no staged events to commit");
                return Ok(());
            }

            let schema_manager = SchemaManager::load(config.schema_store_path())?;
            let store = EventStore::open(config.event_store_path())?;
            let plugins = PluginManager::from_config(&config)?;
            let mut tx = store.transaction()?;

            for staged_event in &staged_events {
                if config.restrict {
                    let payload_map = payload_to_map(&staged_event.payload);
                    schema_manager.validate_event(
                        &staged_event.aggregate,
                        &staged_event.event,
                        &payload_map,
                    )?;
                }

                tx.append(staged_event.to_append_event())?;
            }

            let records = tx.commit()?;
            for record in &records {
                println!("{}", serde_json::to_string_pretty(record)?);
            }

            if !plugins.is_empty() {
                for record in &records {
                    let schema = schema_manager.get(&record.aggregate_type).ok();
                    match store.get_aggregate_state(&record.aggregate_type, &record.aggregate_id) {
                        Ok(current_state) => {
                            if let Err(err) =
                                plugins.notify_event(record, &current_state, schema.as_ref())
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

            clear_staged_events(staging_path.as_path())?;
            println!("committed {} event(s)", records.len());
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

fn collect_payload(fields: Vec<KeyValue>) -> Value {
    let mut map = JsonMap::new();
    for kv in fields {
        map.insert(kv.key, Value::String(kv.value));
    }
    Value::Object(map)
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
        for (key, value) in payload_to_map(&event.payload) {
            state.insert(key, value);
        }
    }

    let merkle_root = compute_merkle_root(&hashes);
    (last_version, state, merkle_root)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedEvent {
    aggregate: String,
    aggregate_id: String,
    event: String,
    #[serde(default = "default_event_payload")]
    payload: Value,
    #[serde(default)]
    issued_by: Option<StagedIssuedBy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedIssuedBy {
    group: String,
    user: String,
}

impl StagedEvent {
    fn to_append_event(&self) -> AppendEvent {
        AppendEvent {
            aggregate_type: self.aggregate.clone(),
            aggregate_id: self.aggregate_id.clone(),
            event_type: self.event.clone(),
            payload: self.payload.clone(),
            issued_by: self.issued_by.clone().map(Into::into),
        }
    }
}

impl From<StagedIssuedBy> for ActorClaims {
    fn from(value: StagedIssuedBy) -> Self {
        ActorClaims {
            group: value.group,
            user: value.user,
        }
    }
}

fn default_event_payload() -> Value {
    Value::Null
}

fn load_staged_events(path: &Path) -> Result<Vec<StagedEvent>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path)?;
    if contents.trim().is_empty() {
        return Ok(Vec::new());
    }

    let events = serde_json::from_str::<Vec<StagedEvent>>(&contents)?;
    Ok(events)
}

fn append_staged_event(path: &Path, event: StagedEvent) -> Result<()> {
    let mut events = load_staged_events(path)?;
    events.push(event);
    save_staged_events(path, &events)
}

fn clear_staged_events(path: &Path) -> Result<()> {
    if path.exists() {
        fs::write(path, "[]")?;
    }
    Ok(())
}

fn save_staged_events(path: &Path, events: &[StagedEvent]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_string_pretty(events)?;
    fs::write(path, payload)?;
    Ok(())
}
