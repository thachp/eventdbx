use std::{collections::BTreeMap, fs, path::PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand};
use serde::Deserialize;
use uuid::Uuid;

use eventdbx::config::{
    CsvPluginConfig, HttpPluginConfig, JsonPluginConfig, LogPluginConfig, PluginConfig,
    PluginDefinition, PluginKind, TcpPluginConfig, load_or_default,
};

#[derive(Subcommand)]
pub enum PluginCommands {
    /// Configure the CSV plugin
    #[command(name = "csv")]
    CsvConfigure(PluginCsvConfigureArgs),
    /// Configure the TCP plugin
    #[command(name = "tcp")]
    TcpConfigure(PluginTcpConfigureArgs),
    /// Configure the HTTP plugin
    #[command(name = "http")]
    HttpConfigure(PluginHttpConfigureArgs),
    /// Configure the JSON file plugin
    #[command(name = "json")]
    JsonConfigure(PluginJsonConfigureArgs),
    /// Configure the logging plugin
    #[command(name = "log")]
    LogConfigure(PluginLogConfigureArgs),
    /// Configure per-plugin field mappings
    #[command(name = "map")]
    Map(PluginMapArgs),
    /// Show plugin retry queue statistics
    #[command(name = "queue")]
    Queue,
    /// List enabled plugins
    #[command(name = "list")]
    List,
}

#[derive(Args)]
pub struct PluginCsvConfigureArgs {
    /// Output directory for CSV files
    #[arg(long)]
    pub output_dir: PathBuf,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Optional label for this CSV plugin instance
    #[arg(long)]
    pub name: Option<String>,
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

    /// Optional label for this TCP plugin instance
    #[arg(long)]
    pub name: Option<String>,
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

    /// Optional label for this HTTP plugin instance
    #[arg(long)]
    pub name: Option<String>,
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

    /// Optional label for this JSON plugin instance
    #[arg(long)]
    pub name: Option<String>,
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

    /// Optional label for this Log plugin instance
    #[arg(long)]
    pub name: Option<String>,
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

pub fn execute(config_path: Option<PathBuf>, command: PluginCommands) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    let mut plugins = config.load_plugins()?;
    if plugins.is_empty() && !config.plugins.is_empty() {
        plugins = config.plugins.clone();
        config.plugins.clear();
        config.save_plugins(&plugins)?;
        config.ensure_data_dir()?;
        config.save(&path)?;
    }

    match command {
        PluginCommands::List => {
            list_enabled_plugins(&plugins);
        }
        PluginCommands::Queue => {
            print_plugin_queue_status(config.plugin_queue_path())?;
        }
        PluginCommands::CsvConfigure(args) => {
            let label = display_label(&args.name);
            match find_plugin_mut(&mut plugins, PluginKind::Csv, &args.name)? {
                Some(plugin) => {
                    plugin.enabled = !args.disable;
                    plugin.name = args.name.clone();
                    plugin.config = PluginConfig::Csv(CsvPluginConfig {
                        output_dir: args.output_dir.clone(),
                    });
                }
                None => {
                    plugins.push(PluginDefinition {
                        enabled: !args.disable,
                        name: args.name.clone(),
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
        PluginCommands::TcpConfigure(args) => {
            let label = display_label(&args.name);
            match find_plugin_mut(&mut plugins, PluginKind::Tcp, &args.name)? {
                Some(plugin) => {
                    plugin.enabled = !args.disable;
                    plugin.name = args.name.clone();
                    plugin.config = PluginConfig::Tcp(TcpPluginConfig {
                        host: args.host,
                        port: args.port,
                    });
                }
                None => {
                    plugins.push(PluginDefinition {
                        enabled: !args.disable,
                        name: args.name.clone(),
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
        PluginCommands::HttpConfigure(args) => {
            let mut headers = BTreeMap::new();
            for entry in args.headers {
                headers.insert(entry.key, entry.value);
            }
            let label = display_label(&args.name);
            match find_plugin_mut(&mut plugins, PluginKind::Http, &args.name)? {
                Some(plugin) => {
                    plugin.enabled = !args.disable;
                    plugin.name = args.name.clone();
                    plugin.config = PluginConfig::Http(HttpPluginConfig {
                        endpoint: args.endpoint,
                        headers,
                    });
                }
                None => {
                    plugins.push(PluginDefinition {
                        enabled: !args.disable,
                        name: args.name.clone(),
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
        PluginCommands::JsonConfigure(args) => {
            let label = display_label(&args.name);
            match find_plugin_mut(&mut plugins, PluginKind::Json, &args.name)? {
                Some(plugin) => {
                    plugin.enabled = !args.disable;
                    plugin.name = args.name.clone();
                    plugin.config = PluginConfig::Json(JsonPluginConfig {
                        path: args.path,
                        pretty: args.pretty,
                    });
                }
                None => {
                    plugins.push(PluginDefinition {
                        enabled: !args.disable,
                        name: args.name.clone(),
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
        PluginCommands::LogConfigure(args) => {
            let label = display_label(&args.name);
            match find_plugin_mut(&mut plugins, PluginKind::Log, &args.name)? {
                Some(plugin) => {
                    plugin.enabled = !args.disable;
                    plugin.name = args.name.clone();
                    plugin.config = PluginConfig::Log(LogPluginConfig {
                        level: args.level.clone(),
                        template: args.template.clone(),
                    });
                }
                None => {
                    plugins.push(PluginDefinition {
                        enabled: !args.disable,
                        name: args.name.clone(),
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

fn list_enabled_plugins(plugins: &[PluginDefinition]) {
    let mut any = false;
    for plugin in plugins.iter().filter(|plugin| plugin.enabled) {
        let kind = plugin_kind_name(plugin.config.kind());
        if let Some(name) = &plugin.name {
            println!("{} ({})", kind, name);
        } else {
            println!("{}", kind);
        }
        any = true;
    }

    if !any {
        println!("(no plugins enabled)");
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

fn display_label(name: &Option<String>) -> &str {
    name.as_deref().unwrap_or("default")
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
    name: &Option<String>,
) -> Result<Option<&'a mut PluginDefinition>> {
    if let Some(target) = name {
        let plugin = plugins
            .iter_mut()
            .find(|def| def.config.kind() == kind && def.name.as_deref() == Some(target.as_str()))
            .ok_or_else(|| {
                anyhow!(
                    "no {} plugin named '{}' is configured",
                    plugin_kind_name(kind),
                    target
                )
            })?;
        return Ok(Some(plugin));
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
