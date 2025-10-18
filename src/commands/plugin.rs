use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::PathBuf,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Subcommand};
use serde::Deserialize;
use uuid::Uuid;

use eventdbx::config::{
    CsvPluginConfig, HttpPluginConfig, JsonPluginConfig, LogPluginConfig, PluginConfig,
    PluginDefinition, PluginKind, TcpPluginConfig, load_or_default,
};
use eventdbx::plugin::establish_connection;

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
    /// Show plugin retry queue statistics
    #[command(name = "queue")]
    Queue,
    /// List enabled plugins
    #[command(name = "list")]
    List,
}

#[derive(Subcommand)]
pub enum PluginConfigureCommands {
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
        PluginCommands::Queue => {
            print_plugin_queue_status(config.plugin_queue_path())?;
        }
        PluginCommands::Config(config_command) => match config_command {
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

            establish_connection(plugin)
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
        PluginCommands::Map(args) => {
            config.set_column_type(&args.aggregate, &args.field, args.data_type.clone());
            config.ensure_data_dir()?;
            config.save(&path)?;
            println!(
                "Mapped base {}.{} as {}",
                args.aggregate, args.field, args.data_type
            );
        }
    }

    Ok(())
}

fn print_plugin_queue_status(path: PathBuf) -> Result<()> {
    if !path.exists() {
        println!("pending=0 dead=0");
        return Ok(());
    }

    let contents = fs::read_to_string(&path)
        .with_context(|| format!("failed to read queue file at {}", path.display()))?;

    if contents.trim().is_empty() {
        println!("pending=0 dead=0");
        return Ok(());
    }

    let status: PluginQueueStatus =
        serde_json::from_str(&contents).with_context(|| "failed to decode queue status JSON")?;

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

#[derive(Deserialize)]
struct PluginQueueStatus {
    pending: usize,
    dead: usize,
    #[serde(default)]
    pending_events: Vec<PluginQueueEvent>,
    #[serde(default)]
    dead_events: Vec<PluginQueueEvent>,
}

#[derive(Deserialize)]
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
