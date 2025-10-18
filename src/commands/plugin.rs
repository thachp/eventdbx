use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand, ValueEnum};
use serde::Deserialize;

use eventdbx::config::{
    CsvPluginConfig, HttpPluginConfig, JsonPluginConfig, LogPluginConfig, PluginConfig,
    PluginDefinition, PluginKind, PostgresColumnConfig, PostgresPluginConfig, SqlitePluginConfig,
    TcpPluginConfig, load_or_default,
};

#[derive(Subcommand)]
pub enum PluginCommands {
    /// Configure the Postgres plugin
    #[command(name = "postgres")]
    PostgresConfigure(PluginPostgresConfigureArgs),
    /// Configure the SQLite plugin
    #[command(name = "sqlite")]
    SqliteConfigure(PluginSqliteConfigureArgs),
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
}

#[derive(Args)]
pub struct PluginPostgresConfigureArgs {
    /// Connection string used to reach the Postgres database
    #[arg(long = "connection")]
    pub connection: String,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,
}

#[derive(Args)]
pub struct PluginSqliteConfigureArgs {
    /// Path to the SQLite database file
    #[arg(long)]
    pub path: PathBuf,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,
}

#[derive(Args)]
pub struct PluginCsvConfigureArgs {
    /// Output directory for CSV files
    #[arg(long)]
    pub output_dir: PathBuf,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,
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
    Sqlite,
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
            PluginTarget::Sqlite => PluginKind::Sqlite,
            PluginTarget::Csv => PluginKind::Csv,
            PluginTarget::Tcp => PluginKind::Tcp,
            PluginTarget::Http => PluginKind::Http,
            PluginTarget::Json => PluginKind::Json,
            PluginTarget::Log => PluginKind::Log,
        }
    }
}

pub fn execute(config_path: Option<PathBuf>, command: PluginCommands) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    match command {
        PluginCommands::Queue => {
            print_plugin_queue_status(config.plugin_queue_path())?;
        }
        PluginCommands::PostgresConfigure(args) => {
            let existing_mapping = config
                .plugins
                .iter()
                .find_map(|def| match &def.config {
                    PluginConfig::Postgres(settings) => Some(settings.field_mappings.clone()),
                    _ => None,
                })
                .unwrap_or_default();

            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Postgres(PostgresPluginConfig {
                    connection_string: args.connection,
                    field_mappings: existing_mapping,
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("Postgres plugin disabled");
            } else {
                println!("Postgres plugin configured");
            }
        }
        PluginCommands::SqliteConfigure(args) => {
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Sqlite(SqlitePluginConfig {
                    path: args.path.clone(),
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("SQLite plugin disabled");
            } else {
                println!("SQLite plugin configured");
            }
        }
        PluginCommands::CsvConfigure(args) => {
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Csv(CsvPluginConfig {
                    output_dir: args.output_dir.clone(),
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("CSV plugin disabled");
            } else {
                println!("CSV plugin configured");
            }
        }
        PluginCommands::TcpConfigure(args) => {
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Tcp(TcpPluginConfig {
                    host: args.host,
                    port: args.port,
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("TCP plugin disabled");
            } else {
                println!("TCP plugin configured");
            }
        }
        PluginCommands::HttpConfigure(args) => {
            let mut headers = BTreeMap::new();
            for entry in args.headers {
                headers.insert(entry.key, entry.value);
            }
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Http(HttpPluginConfig {
                    endpoint: args.endpoint,
                    headers,
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("HTTP plugin disabled");
            } else {
                println!("HTTP plugin configured");
            }
        }
        PluginCommands::JsonConfigure(args) => {
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Json(JsonPluginConfig {
                    path: args.path,
                    pretty: args.pretty,
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("JSON plugin disabled");
            } else {
                println!("JSON plugin configured");
            }
        }
        PluginCommands::LogConfigure(args) => {
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Log(LogPluginConfig {
                    level: args.level.clone(),
                    template: args.template.clone(),
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("Log plugin disabled");
            } else {
                println!("Log plugin configured");
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
            Some(plugin) => match PluginKind::from(plugin) {
                PluginKind::Postgres => {
                    let definition = config
                        .plugins
                        .iter_mut()
                        .find(|def| matches!(def.config, PluginConfig::Postgres(_)))
                        .ok_or_else(|| {
                        anyhow!(
                            "configure postgres plugin before mapping fields with `eventdbx plugin postgres --connection=...`"
                        )
                    })?;

                    match &mut definition.config {
                        PluginConfig::Postgres(settings) => {
                            let mut field_config = PostgresColumnConfig::default();
                            field_config.data_type = Some(args.data_type.clone());

                            settings
                                .field_mappings
                                .entry(args.aggregate.clone())
                                .or_default()
                                .insert(args.field.clone(), field_config);

                            config.ensure_data_dir()?;
                            config.save(&path)?;

                            println!(
                                "Mapped {}.{} as {}",
                                args.aggregate, args.field, args.data_type
                            );
                        }
                        _ => {
                            return Err(anyhow!(
                                "unexpected plugin configuration variant; reconfigure the postgres plugin and try again"
                            ));
                        }
                    }
                }
                PluginKind::Sqlite => {
                    return Err(anyhow!(
                        "field mapping is not supported for the SQLite plugin"
                    ));
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

    let status: PluginQueueStatus = serde_json::from_str(&contents)
        .with_context(|| "failed to decode queue status JSON")?;

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
    event_id: String,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    attempts: u32,
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
