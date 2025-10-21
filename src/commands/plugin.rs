use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use eventdbx::config::{
    Config, GrpcPluginConfig, HttpPluginConfig, LogPluginConfig, PluginConfig, PluginDefinition,
    PluginKind, TcpPluginConfig, load_or_default,
};
use eventdbx::plugin::{Plugin, establish_connection, instantiate_plugin};
use eventdbx::store::{
    ActorClaims, AggregateState, EventMetadata, EventRecord, EventStore, payload_to_map,
};
use eventdbx::{
    error::EventError,
    schema::{AggregateSchema, SchemaManager},
};
use std::any::Any;
use std::thread;

use tokio::runtime::Handle;

#[derive(Subcommand)]
pub enum PluginCommands {
    /// Configure plugins
    #[command(subcommand)]
    Config(PluginConfigureCommands),
    /// Enable a configured plugin
    #[command(name = "enable")]
    Enable(PluginEnableArgs),
    /// Disable a configured plugin
    #[command(name = "disable")]
    Disable(PluginDisableArgs),
    /// Remove a configured plugin
    #[command(name = "remove")]
    Remove(PluginRemoveArgs),
    /// Replay stored events through a plugin
    #[command(name = "replay")]
    Replay(PluginReplayArgs),
    /// Send a sample event to all enabled plugins
    #[command(name = "test")]
    Test,
    /// Show or manage the plugin retry queue
    #[command(name = "queue")]
    Queue(PluginQueueArgs),
    /// List enabled plugins
    #[command(name = "list")]
    List(PluginListArgs),
}

#[derive(Args)]
pub struct PluginQueueArgs {
    #[command(subcommand)]
    pub action: Option<PluginQueueAction>,
}

#[derive(Subcommand, Clone, Copy)]
pub enum PluginQueueAction {
    /// Remove all dead entries from the queue
    #[command(name = "clear")]
    Clear,
    /// Retry dead entries, optionally filtering by event id
    #[command(name = "retry")]
    Retry(PluginQueueRetryArgs),
}

#[derive(Args, Clone, Copy)]
pub struct PluginQueueRetryArgs {
    /// Retry only the dead entry with this event id
    #[arg(long)]
    pub event_id: Option<Uuid>,
}

#[derive(Args, Default)]
pub struct PluginListArgs {
    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Subcommand)]
pub enum PluginConfigureCommands {
    /// Configure the TCP plugin
    #[command(name = "tcp")]
    Tcp(PluginTcpConfigureArgs),
    /// Configure the HTTP plugin
    #[command(name = "http")]
    Http(PluginHttpConfigureArgs),
    /// Configure the gRPC plugin
    #[command(name = "grpc")]
    Grpc(PluginGrpcConfigureArgs),
    /// Configure the logging plugin
    #[command(name = "log")]
    Log(PluginLogConfigureArgs),
}

#[derive(Args)]
pub struct PluginTcpConfigureArgs {
    /// Hostname or IP of the TCP service
    #[arg(long)]
    pub host: String,

    /// Port of the TCP service
    #[arg(long)]
    pub port: u16,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Name for this TCP plugin instance
    #[arg(long)]
    pub name: String,
}

#[derive(Args)]
pub struct PluginHttpConfigureArgs {
    /// HTTP endpoint to POST aggregate updates to
    #[arg(long)]
    pub endpoint: String,

    /// Additional headers to send (key=value)
    #[arg(long = "header", value_parser = parse_key_value, value_name = "KEY=VALUE")]
    pub headers: Vec<KeyValue>,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Use HTTPS when constructing the endpoint
    #[arg(long, default_value_t = false)]
    pub https: bool,

    /// Name for this HTTP plugin instance
    #[arg(long)]
    pub name: String,
}

#[derive(Args)]
pub struct PluginGrpcConfigureArgs {
    /// gRPC endpoint to send replication batches to
    #[arg(long)]
    pub endpoint: String,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Name for this gRPC plugin instance
    #[arg(long)]
    pub name: String,
}

#[derive(Args)]
pub struct PluginLogConfigureArgs {
    /// Log level to use (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub level: String,

    /// Optional template using {aggregate}, {id}, {event}
    #[arg(long)]
    pub template: Option<String>,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Name for this Log plugin instance
    #[arg(long)]
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Args)]
pub struct PluginEnableArgs {
    /// Name of the plugin instance to enable
    pub name: String,
}

#[derive(Args)]
pub struct PluginDisableArgs {
    /// Name of the plugin instance to disable
    pub name: String,
}

#[derive(Args)]
pub struct PluginRemoveArgs {
    /// Name of the plugin instance to remove
    pub name: String,
}

#[derive(Args)]
pub struct PluginReplayArgs {
    /// Plugin name or type to target
    pub plugin: String,

    /// Aggregate type to replay
    pub aggregate: String,

    /// Specific aggregate instance (omit to replay all instances)
    pub aggregate_id: Option<String>,
}

pub fn execute(config_path: Option<PathBuf>, command: PluginCommands) -> Result<()> {
    let (config, path) = load_or_default(config_path)?;

    let mut plugins = config.load_plugins()?;
    let mut plugins_dirty = normalize_plugin_names(&mut plugins);
    if dedupe_plugins_by_name(&mut plugins) {
        plugins_dirty = true;
    }

    if plugins_dirty {
        config.ensure_data_dir()?;
        config.save_plugins(&plugins)?;
    }

    match command {
        PluginCommands::List(args) => {
            list_plugins(&plugins, args.json)?;
        }
        PluginCommands::Queue(args) => {
            let queue_path = config.plugin_queue_path();
            match args.action {
                None => {
                    print_plugin_queue_status(queue_path)?;
                }
                Some(PluginQueueAction::Clear) => {
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
                Some(PluginQueueAction::Retry(retry_args)) => {
                    retry_dead_events(&config, &plugins, queue_path, retry_args.event_id)?;
                }
            }
        }
        PluginCommands::Config(config_command) => match config_command {
            PluginConfigureCommands::Tcp(args) => {
                let name = args.name.trim();
                if name.is_empty() {
                    bail!("plugin name cannot be empty");
                }
                let name_owned = name.to_string();
                let label = display_label(name);
                match find_plugin_mut(&mut plugins, PluginKind::Tcp, Some(name))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(name_owned.clone());
                        plugin.config = PluginConfig::Tcp(TcpPluginConfig {
                            host: args.host,
                            port: args.port,
                        });
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            config: PluginConfig::Tcp(TcpPluginConfig {
                                host: args.host,
                                port: args.port,
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "TCP plugin '{}' {}",
                    label,
                    if args.disable {
                        "disabled"
                    } else {
                        "configured"
                    }
                );
            }
            PluginConfigureCommands::Http(args) => {
                let mut headers = BTreeMap::new();
                for entry in args.headers {
                    headers.insert(entry.key, entry.value);
                }
                let name = args.name.trim();
                if name.is_empty() {
                    bail!("plugin name cannot be empty");
                }
                let name_owned = name.to_string();
                let label = display_label(name);
                match find_plugin_mut(&mut plugins, PluginKind::Http, Some(name))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(name_owned.clone());
                        plugin.config = PluginConfig::Http(HttpPluginConfig {
                            endpoint: args.endpoint,
                            headers,
                            https: args.https,
                        });
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            config: PluginConfig::Http(HttpPluginConfig {
                                endpoint: args.endpoint,
                                headers,
                                https: args.https,
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "HTTP plugin '{}' {}",
                    label,
                    if args.disable {
                        "disabled"
                    } else {
                        "configured"
                    }
                );
            }
            PluginConfigureCommands::Grpc(args) => {
                let name = args.name.trim();
                if name.is_empty() {
                    bail!("plugin name cannot be empty");
                }
                let name_owned = name.to_string();
                let label = display_label(name);
                match find_plugin_mut(&mut plugins, PluginKind::Grpc, Some(name))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(name_owned.clone());
                        plugin.config = PluginConfig::Grpc(GrpcPluginConfig {
                            endpoint: args.endpoint.clone(),
                        });
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            config: PluginConfig::Grpc(GrpcPluginConfig {
                                endpoint: args.endpoint.clone(),
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "gRPC plugin '{}' {}",
                    label,
                    if args.disable {
                        "disabled"
                    } else {
                        "configured"
                    }
                );
            }
            PluginConfigureCommands::Log(args) => {
                let name = args.name.trim();
                if name.is_empty() {
                    bail!("plugin name cannot be empty");
                }
                let name_owned = name.to_string();
                let label = display_label(name);
                match find_plugin_mut(&mut plugins, PluginKind::Log, Some(name))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(name_owned.clone());
                        plugin.config = PluginConfig::Log(LogPluginConfig {
                            level: args.level.clone(),
                            template: args.template.clone(),
                        });
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            config: PluginConfig::Log(LogPluginConfig {
                                level: args.level.clone(),
                                template: args.template.clone(),
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "Log plugin '{}' {}",
                    label,
                    if args.disable {
                        "disabled"
                    } else {
                        "configured"
                    }
                );
            }
        },
        PluginCommands::Enable(args) => {
            let name = args.name.trim();
            if name.is_empty() {
                bail!("plugin name cannot be empty");
            }

            let plugin = plugins
                .iter_mut()
                .find(|definition| definition.name.as_deref() == Some(name))
                .ok_or_else(|| anyhow!("no plugin named '{}' is configured", name))?;

            let connection_definition = plugin.clone();
            run_blocking(move || {
                establish_connection(&connection_definition).map_err(anyhow::Error::from)
            })
            .with_context(|| format!("failed to establish connection for '{}'", name))?;

            let mut changed = false;
            if !plugin.enabled {
                plugin.enabled = true;
                changed = true;
            }

            if changed {
                config.ensure_data_dir()?;
                config.save_plugins(&plugins)?;
                println!("Plugin '{}' enabled", name);
            } else {
                println!("Plugin '{}' is already enabled", name);
            }
        }
        PluginCommands::Disable(args) => {
            let name = args.name.trim();
            if name.is_empty() {
                bail!("plugin name cannot be empty");
            }

            let plugin = plugins
                .iter_mut()
                .find(|definition| definition.name.as_deref() == Some(name))
                .ok_or_else(|| anyhow!("no plugin named '{}' is configured", name))?;

            if plugin.enabled {
                plugin.enabled = false;
                config.ensure_data_dir()?;
                config.save_plugins(&plugins)?;
                println!("Plugin '{}' disabled", name);
            } else {
                println!("Plugin '{}' is already disabled", name);
            }
        }
        PluginCommands::Remove(args) => {
            let name = args.name.trim();
            if name.is_empty() {
                bail!("plugin name cannot be empty");
            }

            let index = plugins
                .iter()
                .position(|definition| definition.name.as_deref() == Some(name))
                .ok_or_else(|| anyhow!("no plugin named '{}' is configured", name))?;

            if plugins[index].enabled {
                bail!(
                    "disable plugin '{}' before removing it (use plugin disable {})",
                    name,
                    name
                );
            }

            plugins.remove(index);
            config.ensure_data_dir()?;
            config.save_plugins(&plugins)?;
            println!("Plugin '{}' removed", name);
        }
        PluginCommands::Replay(args) => {
            replay(
                Some(path.clone()),
                args.plugin,
                args.aggregate,
                args.aggregate_id,
            )?;
        }
        PluginCommands::Test => {
            let enabled: Vec<PluginDefinition> = plugins
                .iter()
                .filter(|definition| definition.enabled)
                .cloned()
                .collect();

            if enabled.is_empty() {
                println!("(no plugins enabled)");
                return Ok(());
            }

            let mut aggregate_state_map = BTreeMap::new();
            aggregate_state_map.insert("status".to_string(), "inactive".to_string());
            aggregate_state_map.insert("comment".to_string(), "Archived via API".to_string());

            let aggregate_state = AggregateState {
                aggregate_type: "patient".to_string(),
                aggregate_id: "p-001".to_string(),
                version: 5,
                state: aggregate_state_map.clone(),
                merkle_root: "deadbeef…".to_string(),
                archived: false,
            };

            let record = EventRecord {
                aggregate_type: "patient".to_string(),
                aggregate_id: "p-001".to_string(),
                event_type: "patient-updated".to_string(),
                payload: json!({
                    "status": "inactive",
                    "comment": "Archived via API"
                }),
                metadata: EventMetadata {
                    event_id: Uuid::parse_str("45c3013e-9b95-4ed0-9af9-1a465f81d3cf")
                        .unwrap_or_else(|_| Uuid::new_v4()),
                    created_at: DateTime::parse_from_rfc3339("2024-12-01T17:22:43.512345Z")
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    issued_by: Some(ActorClaims {
                        group: "admin".to_string(),
                        user: "jane".to_string(),
                    }),
                },
                version: 5,
                hash: "cafe…".to_string(),
                merkle_root: "deadbeef…".to_string(),
            };

            let mut successes = 0;
            let mut failures = 0;

            for definition in enabled {
                let label = match &definition.name {
                    Some(name) => {
                        format!("{} ({})", plugin_kind_name(definition.config.kind()), name)
                    }
                    None => plugin_kind_name(definition.config.kind()).to_string(),
                };

                let definition_clone = definition.clone();
                let record_clone = record.clone();
                let state_clone = aggregate_state.clone();

                let result = run_blocking(move || {
                    establish_connection(&definition_clone).map_err(anyhow::Error::from)?;
                    let plugin = instantiate_plugin(&definition_clone);
                    plugin
                        .notify_event(&record_clone, &state_clone, None)
                        .map_err(anyhow::Error::from)
                });

                match result {
                    Ok(()) => {
                        println!("{} - ok", label);
                        successes += 1;
                    }
                    Err(err) => {
                        println!("{} - FAILED ({})", label, err);
                        failures += 1;
                    }
                }
            }

            println!(
                "Tested {} plugin(s): {} succeeded, {} failed",
                successes + failures,
                successes,
                failures
            );

            if failures > 0 {
                return Err(anyhow!("plugin test failed for {} plugin(s)", failures));
            }
        }
    }

    Ok(())
}

pub fn replay(
    config_path: Option<PathBuf>,
    plugin_name: String,
    aggregate: String,
    aggregate_id: Option<String>,
) -> Result<()> {
    run_blocking(move || replay_blocking(config_path, plugin_name, aggregate, aggregate_id))
}

fn replay_blocking(
    config_path: Option<PathBuf>,
    plugin_name: String,
    aggregate: String,
    aggregate_id: Option<String>,
) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = EventStore::open(config.event_store_path(), config.encryption_key()?)?;
    let schema_manager = SchemaManager::load(config.schema_store_path())?;

    let plugin_defs = config.load_plugins()?;

    let target_plugin = plugin_defs
        .iter()
        .find(|definition| {
            definition.enabled
                && match &definition.name {
                    Some(name) => name == &plugin_name,
                    None => plugin_name == plugin_kind_name(definition.config.kind()),
                }
        })
        .cloned()
        .ok_or_else(|| anyhow!("no enabled plugin named '{}' is configured", plugin_name))?;

    let plugin_instance = instantiate_plugin(&target_plugin);
    let plugin = plugin_instance.as_ref();
    let schema = schema_manager.get(&aggregate).ok();

    if let Some(aggregate_id) = aggregate_id {
        replay_single(&store, plugin, &aggregate, &aggregate_id, schema.as_ref())?
    } else {
        replay_all(&store, plugin, &aggregate, schema.as_ref())?;
    }

    Ok(())
}

fn replay_single(
    store: &EventStore,
    plugin: &dyn Plugin,
    aggregate: &str,
    aggregate_id: &str,
    schema: Option<&AggregateSchema>,
) -> Result<()> {
    let events = match store.list_events(aggregate, aggregate_id) {
        Ok(events) => events,
        Err(EventError::AggregateNotFound) => {
            println!("no events found for {}::{}", aggregate, aggregate_id);
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    if events.is_empty() {
        println!("no events found for {}::{}", aggregate, aggregate_id);
        return Ok(());
    }

    let mut state_map = BTreeMap::new();
    for event in events {
        for (key, value) in payload_to_map(&event.payload) {
            state_map.insert(key, value);
        }
        let state = AggregateState {
            aggregate_type: aggregate.to_string(),
            aggregate_id: aggregate_id.to_string(),
            version: event.version,
            state: state_map.clone(),
            merkle_root: event.merkle_root.clone(),
            archived: false,
        };
        plugin.notify_event(&event, &state, schema)?;
    }

    println!("replayed {}::{}", aggregate, aggregate_id);
    Ok(())
}

fn replay_all(
    store: &EventStore,
    plugin: &dyn Plugin,
    aggregate: &str,
    schema: Option<&AggregateSchema>,
) -> Result<()> {
    let mut total = 0;
    let aggregates = store.list_aggregate_ids(aggregate)?;
    for aggregate_id in aggregates {
        replay_single(store, plugin, aggregate, &aggregate_id, schema)?;
        total += 1;
    }
    if total == 0 {
        println!("no aggregates found for '{}'", aggregate);
    } else {
        println!("replayed {} aggregate(s) for '{}'", total, aggregate);
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
    filter_event: Option<Uuid>,
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
        let instance = instantiate_plugin(definition);
        active_plugins.push((label, instance));
    }

    if active_plugins.is_empty() {
        println!("no enabled plugins available to process retries");
        return Ok(());
    }

    let mut succeeded: HashSet<Uuid> = HashSet::new();
    let mut failed: HashSet<Uuid> = HashSet::new();

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

fn list_plugins(plugins: &[PluginDefinition], json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(plugins)?);
        return Ok(());
    }

    if plugins.is_empty() {
        println!("(no plugins configured)");
        return Ok(());
    }

    for plugin in plugins {
        let kind = plugin_kind_name(plugin.config.kind());
        let status = if plugin.enabled {
            "enabled"
        } else {
            "disabled"
        };
        let suggestion = plugin.name.as_deref().map(|name| {
            if plugin.enabled {
                format!(" (disable with plugin disable {})", name)
            } else {
                format!(" (enable with plugin enable {})", name)
            }
        });

        if let Some(name) = &plugin.name {
            match suggestion {
                Some(hint) => println!("{} ({}) - {}{}", kind, name, status, hint),
                None => println!("{} ({}) - {}", kind, name, status),
            }
        } else {
            match suggestion {
                Some(hint) => println!("{} - {}{}", kind, status, hint),
                None => println!("{} - {}", kind, status),
            }
        }
    }

    Ok(())
}

fn run_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match Handle::try_current() {
        Ok(_) => {
            let result = thread::spawn(move || f())
                .join()
                .map_err(|err| panic_into_anyhow(err))?;
            result
        }
        Err(_) => f(),
    }
}

fn panic_into_anyhow(err: Box<dyn Any + Send + 'static>) -> anyhow::Error {
    match err.downcast::<String>() {
        Ok(message) => anyhow!("blocking task panicked: {}", *message),
        Err(err) => match err.downcast::<&'static str>() {
            Ok(message) => anyhow!("blocking task panicked: {}", *message),
            Err(_) => anyhow!("blocking task panicked"),
        },
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
    event_id: Uuid,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    attempts: u32,
}

fn display_label(name: &str) -> &str {
    if name.trim().is_empty() {
        "default"
    } else {
        name
    }
}

fn plugin_kind_name(kind: PluginKind) -> &'static str {
    match kind {
        PluginKind::Tcp => "tcp",
        PluginKind::Http => "http",
        PluginKind::Grpc => "grpc",
        PluginKind::Log => "log",
    }
}

fn find_plugin_mut<'a>(
    plugins: &'a mut [PluginDefinition],
    kind: PluginKind,
    name: Option<&str>,
) -> Result<Option<&'a mut PluginDefinition>> {
    if let Some(target) = name {
        let plugin = plugins
            .iter_mut()
            .find(|def| def.config.kind() == kind && def.name.as_deref() == Some(target));
        return Ok(plugin);
    }

    let mut iter = plugins.iter_mut().filter(|def| def.config.kind() == kind);
    let first = iter.next();
    if first.is_none() {
        return Ok(None);
    }
    if iter.next().is_some() {
        return Err(anyhow!(
            "multiple {} plugins configured; specify --name",
            plugin_kind_name(kind)
        ));
    }
    Ok(first)
}

fn normalize_plugin_names(plugins: &mut [PluginDefinition]) -> bool {
    let mut changed = false;
    for plugin in plugins.iter_mut() {
        if let Some(name) = &mut plugin.name {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                plugin.name = None;
                changed = true;
            } else if trimmed != name.as_str() {
                *name = trimmed.to_string();
                changed = true;
            }
        }
    }
    changed
}

fn dedupe_plugins_by_name(plugins: &mut Vec<PluginDefinition>) -> bool {
    let mut seen: HashSet<String> = HashSet::new();
    let mut changed = false;
    let mut index = plugins.len();
    while index > 0 {
        index -= 1;
        let remove = match plugins[index].name.as_deref() {
            Some(name) if !seen.insert(name.to_string()) => true,
            _ => false,
        };
        if remove {
            plugins.remove(index);
            changed = true;
        }
    }
    changed
}

fn ensure_unique_plugin_name(plugins: &[PluginDefinition], name: &str) -> Result<()> {
    if let Some(conflict) = plugins
        .iter()
        .find(|plugin| plugin.name.as_deref() == Some(name))
    {
        bail!(
            "plugin name '{}' is already used by {}",
            name,
            plugin_kind_name(conflict.config.kind())
        );
    }
    Ok(())
}

fn parse_key_value(raw: &str) -> Result<KeyValue, String> {
    let mut parts = raw.splitn(2, '=');
    let key = parts
        .next()
        .ok_or_else(|| "missing key".to_string())?
        .trim()
        .to_string();
    let value = parts
        .next()
        .ok_or_else(|| "missing value".to_string())?
        .trim()
        .to_string();

    if key.is_empty() {
        return Err("header key cannot be empty".to_string());
    }

    Ok(KeyValue { key, value })
}
