use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};

use eventdbx::{
    config::{Config, PluginDefinition, load_or_default},
    error::EventError,
    plugin::{Plugin, establish_connection, instantiate_plugin},
    schema::SchemaManager,
    snowflake::SnowflakeId,
    store::{AggregateState, EventRecord, EventStore, payload_to_map},
};

use crate::commands::plugin::{dedupe_plugins_by_name, normalize_plugin_names, plugin_kind_name};

#[derive(Args)]
pub struct QueueArgs {
    #[command(subcommand)]
    pub action: Option<QueueAction>,
}

#[derive(Subcommand, Clone, Copy)]
pub enum QueueAction {
    /// Remove all dead entries from the queue
    #[command(name = "clear")]
    Clear,
    /// Retry dead entries, optionally filtering by event id
    #[command(name = "retry")]
    Retry(QueueRetryArgs),
}

#[derive(Args, Clone, Copy)]
pub struct QueueRetryArgs {
    /// Retry only the dead entry with this event id
    #[arg(long)]
    pub event_id: Option<SnowflakeId>,
}

pub fn execute(config_path: Option<PathBuf>, args: QueueArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;

    let mut plugins = config.load_plugins()?;
    let mut plugins_dirty = normalize_plugin_names(&mut plugins);
    if dedupe_plugins_by_name(&mut plugins) {
        plugins_dirty = true;
    }

    if plugins_dirty {
        config.ensure_data_dir()?;
        config.save_plugins(&plugins)?;
    }

    let queue_path = config.plugin_queue_path();
    match args.action {
        None => {
            print_plugin_queue_status(queue_path)?;
        }
        Some(QueueAction::Clear) => {
            let status = load_plugin_queue_status(&queue_path)?;
            let dead_count = status
                .as_ref()
                .map(|status| status.dead_events.len())
                .unwrap_or(0);

            if dead_count == 0 {
                println!("no dead plugin events to clear");
                return Ok(());
            }

            if !confirm_clear_dead(dead_count)? {
                println!("clear cancelled");
                return Ok(());
            }

            let cleared = clear_dead_queue(queue_path)?;
            println!("cleared {} dead event(s) from the plugin queue", cleared);
        }
        Some(QueueAction::Retry(retry_args)) => {
            retry_dead_events(&config, &plugins, queue_path, retry_args.event_id)?;
        }
    }
    Ok(())
}

fn print_plugin_queue_status(path: PathBuf) -> Result<()> {
    match load_plugin_queue_status(&path)? {
        Some(status) => {
            println!("pending={} dead={}", status.pending, status.dead);

            if !status.pending_events.is_empty() {
                println!("\nPending events:");
                for event in &status.pending_events {
                    println!(
                        "  {} aggregate={}::{} event={} attempts={}",
                        event.event_id,
                        event.aggregate_type,
                        event.aggregate_id,
                        event.event_type,
                        event.attempts
                    );
                }
            }

            if !status.dead_events.is_empty() {
                println!("\nDead events:");
                for event in &status.dead_events {
                    println!(
                        "  {} aggregate={}::{} event={} attempts={} (no further retries)",
                        event.event_id,
                        event.aggregate_type,
                        event.aggregate_id,
                        event.event_type,
                        event.attempts
                    );
                }
            }
        }
        None => {
            println!("pending=0 dead=0");
        }
    }
    Ok(())
}

fn clear_dead_queue(path: PathBuf) -> Result<usize> {
    let Some(mut status) = load_plugin_queue_status(&path)? else {
        return Ok(0);
    };

    let cleared = status.dead_events.len();
    if cleared == 0 && status.dead == 0 {
        return Ok(0);
    }

    status.dead = 0;
    status.dead_events.clear();
    write_plugin_queue_status(&path, &status)?;
    Ok(cleared)
}

fn retry_dead_events(
    config: &Config,
    plugins: &[PluginDefinition],
    queue_path: PathBuf,
    filter_event: Option<SnowflakeId>,
) -> Result<()> {
    let mut status = match load_plugin_queue_status(&queue_path)? {
        Some(status) => status,
        None => {
            println!("no dead plugin events to retry");
            return Ok(());
        }
    };

    let target_events: Vec<PluginQueueEvent> = status
        .dead_events
        .iter()
        .cloned()
        .filter(|entry| match filter_event {
            Some(id) => id == entry.event_id,
            None => true,
        })
        .collect();

    if target_events.is_empty() {
        if filter_event.is_some() {
            println!("no dead plugin events match the provided event id");
        } else {
            println!("no dead plugin events to retry");
        }
        return Ok(());
    }

    let store = EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)
        .map_err(anyhow::Error::from)?;
    let schema_manager = SchemaManager::load(config.schema_store_path())?;

    let mut active_plugins: Vec<(String, Box<dyn Plugin>)> = Vec::new();

    for definition in plugins.iter().filter(|definition| definition.enabled) {
        if let Err(err) = establish_connection(definition) {
            println!(
                "skipping plugin '{}' - failed to prepare connection ({})",
                plugin_instance_label(definition),
                err
            );
            continue;
        }
        let label = plugin_instance_label(definition);
        let instance = instantiate_plugin(definition, config);
        active_plugins.push((label, instance));
    }

    if active_plugins.is_empty() {
        println!("no enabled plugins available to process retries");
        return Ok(());
    }

    let mut succeeded: HashSet<SnowflakeId> = HashSet::new();
    let mut failed: HashSet<SnowflakeId> = HashSet::new();

    for entry in target_events {
        match materialize_event_state(&store, &entry) {
            Ok(Some((record, state))) => {
                let schema = schema_manager.get(&record.aggregate_type).ok();
                let mut event_failed = false;

                for (label, plugin) in active_plugins.iter() {
                    if let Err(err) = plugin.notify_event(&record, &state, schema.as_ref()) {
                        println!(
                            "plugin '{}' failed to process event {} ({}::{}) - {}",
                            label,
                            record.metadata.event_id,
                            record.aggregate_type,
                            record.aggregate_id,
                            err
                        );
                        event_failed = true;
                    }
                }

                if event_failed {
                    failed.insert(record.metadata.event_id);
                } else {
                    println!(
                        "event {} ({}::{}) retried successfully",
                        record.metadata.event_id, record.aggregate_type, record.aggregate_id
                    );
                    succeeded.insert(record.metadata.event_id);
                }
            }
            Ok(None) => {
                println!(
                    "event {} ({}::{}) not found; removing from dead queue",
                    entry.event_id, entry.aggregate_type, entry.aggregate_id
                );
                succeeded.insert(entry.event_id);
            }
            Err(err) => {
                println!(
                    "failed to load event {} ({}::{}) - {}",
                    entry.event_id, entry.aggregate_type, entry.aggregate_id, err
                );
                failed.insert(entry.event_id);
            }
        }
    }

    if !succeeded.is_empty() {
        status
            .dead_events
            .retain(|item| !succeeded.contains(&item.event_id));
        status.dead = status.dead_events.len();
        write_plugin_queue_status(&queue_path, &status)?;
    }

    println!(
        "retry summary: {} succeeded, {} failed, {} remaining",
        succeeded.len(),
        failed.len(),
        status.dead_events.len()
    );

    if !failed.is_empty() {
        println!(
            "events still pending: {}",
            failed
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

fn confirm_clear_dead(dead_count: usize) -> Result<bool> {
    print!(
        "This will remove {} dead plugin event(s). Type 'clear' to confirm: ",
        dead_count
    );
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .with_context(|| "failed to read confirmation input")?;
    Ok(input.trim().eq_ignore_ascii_case("clear"))
}

fn load_plugin_queue_status(path: &Path) -> Result<Option<PluginQueueStatus>> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read queue file at {}", path.display()))?;
    if contents.trim().is_empty() {
        return Ok(Some(PluginQueueStatus::default()));
    }
    let status =
        serde_json::from_str(&contents).with_context(|| "failed to decode queue status JSON")?;
    Ok(Some(status))
}

fn write_plugin_queue_status(path: &Path, status: &PluginQueueStatus) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    let payload = serde_json::to_string_pretty(status)?;
    fs::write(path, payload)?;
    Ok(())
}

fn materialize_event_state(
    store: &EventStore,
    entry: &PluginQueueEvent,
) -> Result<Option<(EventRecord, AggregateState)>> {
    let events = match store.list_events(&entry.aggregate_type, &entry.aggregate_id) {
        Ok(events) => events,
        Err(EventError::AggregateNotFound) => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    if events.is_empty() {
        return Ok(None);
    }

    let archived_flag = store
        .get_aggregate_state(&entry.aggregate_type, &entry.aggregate_id)
        .map(|state| state.archived)
        .unwrap_or(false);

    let mut state_map = BTreeMap::new();

    for event in events.into_iter() {
        for (key, value) in payload_to_map(&event.payload) {
            state_map.insert(key, value);
        }

        if event.metadata.event_id == entry.event_id {
            let aggregate_state = AggregateState {
                aggregate_type: event.aggregate_type.clone(),
                aggregate_id: event.aggregate_id.clone(),
                version: event.version,
                state: state_map.clone(),
                merkle_root: event.merkle_root.clone(),
                archived: archived_flag,
            };
            return Ok(Some((event, aggregate_state)));
        }
    }

    Ok(None)
}

fn plugin_instance_label(definition: &PluginDefinition) -> String {
    match definition.name.as_deref() {
        Some(name) if !name.trim().is_empty() => {
            format!("{} ({})", plugin_kind_name(definition.config.kind()), name)
        }
        _ => plugin_kind_name(definition.config.kind()).to_string(),
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct PluginQueueStatus {
    pending: usize,
    dead: usize,
    #[serde(default)]
    pending_events: Vec<PluginQueueEvent>,
    #[serde(default)]
    dead_events: Vec<PluginQueueEvent>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PluginQueueEvent {
    event_id: SnowflakeId,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    attempts: u32,
}
