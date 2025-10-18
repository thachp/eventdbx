use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use eventdbx::config::{
    CsvPluginConfig, HttpPluginConfig, JsonPluginConfig, LogPluginConfig, PluginConfig,
    PluginDefinition, PluginKind, PostgresColumnConfig, PostgresPluginConfig, TcpPluginConfig,
    load_or_default,
};
use eventdbx::plugin::{ColumnTypes, Plugin, establish_connection, instantiate_plugin};
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
    /// Configure per-plugin field mappings
    #[command(name = "map")]
    Map(PluginMapArgs),
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
    List,
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
}

#[derive(Subcommand)]
pub enum PluginConfigureCommands {
    /// Configure the Postgres plugin
    #[command(name = "postgres")]
    Postgres(PluginPostgresConfigureArgs),
    /// Configure the CSV plugin
    #[command(name = "csv")]
    Csv(PluginCsvConfigureArgs),
    /// Configure the TCP plugin
    #[command(name = "tcp")]
    Tcp(PluginTcpConfigureArgs),
    /// Configure the HTTP plugin
    #[command(name = "http")]
    Http(PluginHttpConfigureArgs),
    /// Configure the JSON file plugin
    #[command(name = "json")]
    Json(PluginJsonConfigureArgs),
    /// Configure the logging plugin
    #[command(name = "log")]
    Log(PluginLogConfigureArgs),
}

#[derive(Args)]
pub struct PluginPostgresConfigureArgs {
    /// Connection string used to reach the Postgres database
    #[arg(long = "connection")]
    pub connection: String,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Optional label to distinguish between instances
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Args)]
pub struct PluginCsvConfigureArgs {
    /// Output directory for CSV files
    #[arg(long)]
    pub output_dir: PathBuf,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Name for this CSV plugin instance
    #[arg(long)]
    pub name: String,
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
pub struct PluginJsonConfigureArgs {
    /// File path to append JSON snapshots into
    #[arg(long)]
    pub path: PathBuf,

    /// Pretty-print JSON
    #[arg(long, default_value_t = false)]
    pub pretty: bool,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Name for this JSON plugin instance
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
pub struct PluginMapArgs {
    /// Plugin identifier
    #[arg(long, value_enum)]
    pub plugin: Option<PluginTarget>,

    /// Specific plugin instance name (when multiple instances exist)
    #[arg(long = "plugin-name")]
    pub plugin_name: Option<String>,

    /// Aggregate name to configure
    #[arg(long)]
    pub aggregate: String,

    /// Field name to configure
    #[arg(long)]
    pub field: String,

    /// Data type to use for the field (e.g., VARCHAR(255))
    #[arg(long = "datatype")]
    pub data_type: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum PluginTarget {
    Postgres,
    Csv,
    Tcp,
    Http,
    Json,
    Log,
}

impl From<PluginTarget> for PluginKind {
    fn from(value: PluginTarget) -> Self {
        match value {
            PluginTarget::Postgres => PluginKind::Postgres,
            PluginTarget::Csv => PluginKind::Csv,
            PluginTarget::Tcp => PluginKind::Tcp,
            PluginTarget::Http => PluginKind::Http,
            PluginTarget::Json => PluginKind::Json,
            PluginTarget::Log => PluginKind::Log,
        }
    }
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
    let (mut config, path) = load_or_default(config_path)?;

    let mut plugins = config.load_plugins()?;
    let mut plugins_dirty = normalize_plugin_names(&mut plugins);
    if dedupe_plugins_by_name(&mut plugins) {
        plugins_dirty = true;
    }

    let mut config_dirty = false;
    if plugins.is_empty() && !config.plugins.is_empty() {
        plugins = config.plugins.clone();
        config.plugins.clear();
        plugins_dirty = true;
        config_dirty = true;

        if normalize_plugin_names(&mut plugins) {
            plugins_dirty = true;
        }
        if dedupe_plugins_by_name(&mut plugins) {
            plugins_dirty = true;
        }
    }

    if plugins_dirty {
        config.ensure_data_dir()?;
        config.save_plugins(&plugins)?;
    }
    if config_dirty {
        config.ensure_data_dir()?;
        config.save(&path)?;
    }

    match command {
        PluginCommands::List => {
            list_plugins(&plugins);
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
            }
        }
        PluginCommands::Config(config_command) => match config_command {
            PluginConfigureCommands::Postgres(args) => {
                let label = display_label(args.name.as_deref().unwrap_or("default"));
                match find_plugin_mut(&mut plugins, PluginKind::Postgres, args.name.as_deref())? {
                    Some(plugin) => {
                        let field_mappings = match &plugin.config {
                            PluginConfig::Postgres(settings) => settings.field_mappings.clone(),
                            _ => BTreeMap::new(),
                        };
                        plugin.enabled = !args.disable;
                        plugin.name = args.name.clone();
                        plugin.config = PluginConfig::Postgres(PostgresPluginConfig {
                            connection_string: args.connection,
                            field_mappings,
                        });
                    }
                    None => {
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: args.name.clone(),
                            config: PluginConfig::Postgres(PostgresPluginConfig {
                                connection_string: args.connection,
                                field_mappings: BTreeMap::new(),
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "Postgres plugin '{}' {}",
                    label,
                    if args.disable {
                        "disabled"
                    } else {
                        "configured"
                    }
                );
            }
            PluginConfigureCommands::Csv(args) => {
                let name = args.name.trim();
                if name.is_empty() {
                    bail!("plugin name cannot be empty");
                }
                let name_owned = name.to_string();
                let label = display_label(name);
                match find_plugin_mut(&mut plugins, PluginKind::Csv, Some(name))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(name_owned.clone());
                        plugin.config = PluginConfig::Csv(CsvPluginConfig {
                            output_dir: args.output_dir.clone(),
                        });
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            config: PluginConfig::Csv(CsvPluginConfig {
                                output_dir: args.output_dir.clone(),
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "CSV plugin '{}' {}",
                    label,
                    if args.disable {
                        "disabled"
                    } else {
                        "configured"
                    }
                );
            }
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
            PluginConfigureCommands::Json(args) => {
                let name = args.name.trim();
                if name.is_empty() {
                    bail!("plugin name cannot be empty");
                }
                let name_owned = name.to_string();
                let label = display_label(name);
                match find_plugin_mut(&mut plugins, PluginKind::Json, Some(name))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(name_owned.clone());
                        plugin.config = PluginConfig::Json(JsonPluginConfig {
                            path: args.path,
                            pretty: args.pretty,
                        });
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            config: PluginConfig::Json(JsonPluginConfig {
                                path: args.path,
                                pretty: args.pretty,
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "JSON plugin '{}' {}",
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

            let base_types: Arc<ColumnTypes> = Arc::new(config.column_types.clone());

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
                let base_types_clone = Arc::clone(&base_types);

                let result = run_blocking(move || {
                    establish_connection(&definition_clone).map_err(anyhow::Error::from)?;
                    let plugin = instantiate_plugin(&definition_clone, base_types_clone);
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
        PluginCommands::Map(args) => match args.plugin {
            None => {
                config.set_column_type(&args.aggregate, &args.field, args.data_type.clone());
                config.ensure_data_dir()?;
                config.save(&path)?;
                println!(
                    "Mapped base {}.{} as {}",
                    args.aggregate, args.field, args.data_type
                );
            }
            Some(plugin_target) => match PluginKind::from(plugin_target) {
                PluginKind::Postgres => {
                    let plugin = find_plugin_mut(
                        &mut plugins,
                        PluginKind::Postgres,
                        args.plugin_name.as_deref(),
                    )?
                    .ok_or_else(|| {
                        anyhow!(
                            "configure postgres plugin before mapping fields (use --plugin-name if multiple instances exist)"
                        )
                    })?;

                    if let PluginConfig::Postgres(settings) = &mut plugin.config {
                        let mut field_config = PostgresColumnConfig::default();
                        field_config.data_type = Some(args.data_type.clone());

                        settings
                            .field_mappings
                            .entry(args.aggregate.clone())
                            .or_default()
                            .insert(args.field.clone(), field_config);

                        config.save_plugins(&plugins)?;
                        println!(
                            "Mapped {}.{} as {}",
                            args.aggregate, args.field, args.data_type
                        );
                    } else {
                        return Err(anyhow!(
                            "unexpected plugin variant; reconfigure the postgres plugin and try again"
                        ));
                    }
                }
                PluginKind::Csv => {
                    return Err(anyhow!("field mapping is not supported for the CSV plugin"));
                }
                PluginKind::Tcp | PluginKind::Http | PluginKind::Json | PluginKind::Log => {
                    return Err(anyhow!(
                        "field mapping is only supported for the Postgres plugin"
                    ));
                }
            },
        },
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
    let store = EventStore::open(config.event_store_path())?;
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

    let base_types: Arc<ColumnTypes> = Arc::new(config.column_types.clone());
    let plugin_instance = instantiate_plugin(&target_plugin, Arc::clone(&base_types));
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

fn list_plugins(plugins: &[PluginDefinition]) {
    if plugins.is_empty() {
        println!("(no plugins configured)");
        return;
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

#[derive(Default, Serialize, Deserialize)]
struct PluginQueueStatus {
    pending: usize,
    dead: usize,
    #[serde(default)]
    pending_events: Vec<PluginQueueEvent>,
    #[serde(default)]
    dead_events: Vec<PluginQueueEvent>,
}

#[derive(Serialize, Deserialize)]
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
        PluginKind::Postgres => "postgres",
        PluginKind::Csv => "csv",
        PluginKind::Tcp => "tcp",
        PluginKind::Http => "http",
        PluginKind::Json => "json",
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
