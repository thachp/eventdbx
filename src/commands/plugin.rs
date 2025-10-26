use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    env,
    ffi::OsStr,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;

use eventdbx::config::{
    CapnpPluginConfig, Config, HttpPluginConfig, LogPluginConfig, PluginConfig, PluginDefinition,
    PluginKind, PluginPayloadMode, ProcessPluginConfig, TcpPluginConfig, load_or_default,
};
use eventdbx::plugin::{
    Plugin, PluginDelivery, establish_connection, instantiate_plugin,
    registry::{self, InstalledPluginRecord, PluginSource},
    status_file_path,
};
use eventdbx::store::{
    ActorClaims, AggregateState, EventMetadata, EventRecord, EventStore, payload_to_map,
};
use eventdbx::{
    error::EventError,
    schema::{AggregateSchema, SchemaManager},
    snowflake::SnowflakeId,
};
use std::any::Any;
use std::thread;

use tokio::runtime::Handle;
use zip::ZipArchive;

use flate2::read::GzDecoder;
use tar::Archive;
use tempfile::NamedTempFile;

use reqwest::{StatusCode, blocking::Client};
use sha2::{Digest, Sha256};

#[derive(Subcommand)]
pub enum PluginCommands {
    /// Install a plugin binary into the local registry
    #[command(name = "install")]
    Install(PluginInstallArgs),
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
    /// List enabled plugins
    #[command(name = "list")]
    List(PluginListArgs),
}

#[derive(Args, Clone, Debug)]
pub struct PluginInstallArgs {
    /// Unique plugin name (e.g. `search`)
    pub name: String,

    /// Plugin version identifier being installed
    pub version: String,

    /// Source URL or local file system path to the plugin bundle (omit to download from the registry)
    #[arg(long, short = 's')]
    pub source: Option<String>,

    /// Binary filename inside the archive (if ambiguous)
    #[arg(long = "bin")]
    pub binary_name: Option<String>,

    /// Optional SHA256 checksum for verification
    #[arg(long)]
    pub checksum: Option<String>,

    /// Overwrite an existing installation of the same name/version/target
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Args, Default)]
pub struct PluginListArgs {
    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum PayloadModeArg {
    All,
    EventOnly,
    StateOnly,
    SchemaOnly,
    EventAndSchema,
    ExtensionsOnly,
}

impl From<PayloadModeArg> for PluginPayloadMode {
    fn from(value: PayloadModeArg) -> Self {
        match value {
            PayloadModeArg::All => PluginPayloadMode::All,
            PayloadModeArg::EventOnly => PluginPayloadMode::EventOnly,
            PayloadModeArg::StateOnly => PluginPayloadMode::StateOnly,
            PayloadModeArg::SchemaOnly => PluginPayloadMode::SchemaOnly,
            PayloadModeArg::EventAndSchema => PluginPayloadMode::EventAndSchema,
            PayloadModeArg::ExtensionsOnly => PluginPayloadMode::ExtensionsOnly,
        }
    }
}

impl From<PluginPayloadMode> for PayloadModeArg {
    fn from(value: PluginPayloadMode) -> Self {
        match value {
            PluginPayloadMode::All => PayloadModeArg::All,
            PluginPayloadMode::EventOnly => PayloadModeArg::EventOnly,
            PluginPayloadMode::StateOnly => PayloadModeArg::StateOnly,
            PluginPayloadMode::SchemaOnly => PayloadModeArg::SchemaOnly,
            PluginPayloadMode::EventAndSchema => PayloadModeArg::EventAndSchema,
            PluginPayloadMode::ExtensionsOnly => PayloadModeArg::ExtensionsOnly,
        }
    }
}

impl Default for PayloadModeArg {
    fn default() -> Self {
        PayloadModeArg::All
    }
}

#[derive(Subcommand)]
pub enum PluginConfigureCommands {
    /// Configure the TCP plugin
    #[command(name = "tcp")]
    Tcp(PluginTcpConfigureArgs),
    /// Configure the Cap'n Proto plugin
    #[command(name = "capnp")]
    Capnp(PluginCapnpConfigureArgs),
    /// Configure the HTTP plugin
    #[command(name = "http")]
    Http(PluginHttpConfigureArgs),
    /// Configure the logging plugin
    #[command(name = "log")]
    Log(PluginLogConfigureArgs),
    /// Configure a subprocess plugin installed via the registry
    #[command(name = "process")]
    Process(PluginProcessConfigureArgs),
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

    /// Payload components to deliver to the plugin
    #[arg(long = "payload", value_enum)]
    pub payload: Option<PayloadModeArg>,
}

#[derive(Args)]
pub struct PluginCapnpConfigureArgs {
    /// Hostname or IP of the Cap'n Proto service
    #[arg(long)]
    pub host: String,

    /// Port of the Cap'n Proto service
    #[arg(long)]
    pub port: u16,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Name for this Cap'n Proto plugin instance
    #[arg(long)]
    pub name: String,

    /// Payload components to deliver to the plugin
    #[arg(long = "payload", value_enum)]
    pub payload: Option<PayloadModeArg>,
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

    /// Payload components to deliver to the plugin
    #[arg(long = "payload", value_enum)]
    pub payload: Option<PayloadModeArg>,
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

    /// Payload components to deliver to the plugin
    #[arg(long = "payload", value_enum)]
    pub payload: Option<PayloadModeArg>,
}

#[derive(Args)]
pub struct PluginProcessConfigureArgs {
    /// Installed plugin identifier to execute
    #[arg(value_name = "PLUGIN")]
    pub plugin_name: String,

    /// Plugin version to select from the local registry (defaults to the latest installed)
    #[arg(long, value_name = "VERSION")]
    pub version: Option<String>,

    /// Instance name for this process plugin (defaults to the plugin identifier)
    #[arg(long, value_name = "INSTANCE")]
    pub name: Option<String>,

    /// Additional arguments to pass when launching the plugin (repeatable)
    #[arg(long = "arg", value_name = "ARG", action = clap::ArgAction::Append)]
    pub args: Vec<String>,

    /// Additional environment variables (KEY=VALUE) provided at launch
    #[arg(long = "env", value_parser = parse_key_value, value_name = "KEY=VALUE")]
    pub env: Vec<KeyValue>,

    /// Working directory to set before launching the plugin
    #[arg(long = "working-dir")]
    pub working_dir: Option<PathBuf>,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,

    /// Payload components to deliver to the plugin
    #[arg(long = "payload", value_enum)]
    pub payload: Option<PayloadModeArg>,
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
        PluginCommands::Install(args) => {
            let config_clone = config.clone();
            run_blocking(move || install_plugin_bundle(&config_clone, args))?;
        }
        PluginCommands::List(args) => {
            let config_clone = config.clone();
            let plugins_clone = plugins.clone();
            let json = args.json;
            run_blocking(move || list_plugins(&config_clone, &plugins_clone, json))?;
        }
        PluginCommands::Config(config_command) => match config_command {
            PluginConfigureCommands::Tcp(args) => {
                let payload_mode = args.payload;
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
                        if let Some(mode) = payload_mode {
                            plugin.payload_mode = mode.into();
                        }
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            payload_mode: payload_mode.unwrap_or_default().into(),
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
            PluginConfigureCommands::Capnp(args) => {
                let payload_mode = args.payload;
                let name = args.name.trim();
                if name.is_empty() {
                    bail!("plugin name cannot be empty");
                }
                let name_owned = name.to_string();
                let label = display_label(name);
                match find_plugin_mut(&mut plugins, PluginKind::Capnp, Some(name))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(name_owned.clone());
                        plugin.config = PluginConfig::Capnp(CapnpPluginConfig {
                            host: args.host,
                            port: args.port,
                        });
                        if let Some(mode) = payload_mode {
                            plugin.payload_mode = mode.into();
                        }
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            payload_mode: payload_mode.unwrap_or_default().into(),
                            config: PluginConfig::Capnp(CapnpPluginConfig {
                                host: args.host,
                                port: args.port,
                            }),
                        });
                    }
                }
                config.save_plugins(&plugins)?;
                println!(
                    "Cap'n Proto plugin '{}' {}",
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
                let payload_mode = args.payload;
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
                        if let Some(mode) = payload_mode {
                            plugin.payload_mode = mode.into();
                        }
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            payload_mode: payload_mode.unwrap_or_default().into(),
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
            PluginConfigureCommands::Log(args) => {
                let payload_mode = args.payload;
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
                        if let Some(mode) = payload_mode {
                            plugin.payload_mode = mode.into();
                        }
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, name)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(name_owned.clone()),
                            payload_mode: payload_mode.unwrap_or_default().into(),
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
            PluginConfigureCommands::Process(args) => {
                let payload_mode = args.payload;
                let plugin_name = args.plugin_name.trim();
                if plugin_name.is_empty() {
                    bail!("plugin identifier cannot be empty");
                }

                let instance_fallback = plugin_name;
                let instance = args
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(instance_fallback);

                if instance.is_empty() {
                    bail!("plugin name cannot be empty");
                }

                let version = resolve_process_installation_version(
                    &config,
                    plugin_name,
                    args.version.as_deref(),
                )?;

                let mut env_map = BTreeMap::new();
                for entry in &args.env {
                    env_map.insert(entry.key.clone(), entry.value.clone());
                }

                let process_config = ProcessPluginConfig {
                    name: plugin_name.to_string(),
                    version: version.clone(),
                    args: args.args.clone(),
                    env: env_map,
                    working_dir: args.working_dir.clone(),
                };

                let label = display_label(instance);
                let instance_owned = instance.to_string();

                match find_plugin_mut(&mut plugins, PluginKind::Process, Some(instance))? {
                    Some(plugin) => {
                        plugin.enabled = !args.disable;
                        plugin.name = Some(instance_owned.clone());
                        plugin.config = PluginConfig::Process(process_config);
                        if let Some(mode) = payload_mode {
                            plugin.payload_mode = mode.into();
                        }
                    }
                    None => {
                        ensure_unique_plugin_name(&plugins, instance)?;
                        plugins.push(PluginDefinition {
                            enabled: !args.disable,
                            name: Some(instance_owned.clone()),
                            payload_mode: payload_mode.unwrap_or_default().into(),
                            config: PluginConfig::Process(process_config),
                        });
                    }
                }

                config.save_plugins(&plugins)?;
                println!(
                    "Process plugin '{}' {}",
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
                extensions: None,
                metadata: EventMetadata {
                    event_id: SnowflakeId::from_u64(1_234_567_890),
                    created_at: DateTime::parse_from_rfc3339("2024-12-01T17:22:43.512345Z")
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                    issued_by: Some(ActorClaims {
                        group: "admin".to_string(),
                        user: "jane".to_string(),
                    }),
                    note: None,
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
                let cfg = config.clone();

                let result = run_blocking(move || {
                    establish_connection(&definition_clone).map_err(anyhow::Error::from)?;
                    let plugin = instantiate_plugin(&definition_clone, &cfg);
                    let payload_mode = definition_clone.payload_mode;
                    let delivery = PluginDelivery {
                        record: payload_mode.includes_event().then_some(&record_clone),
                        state: payload_mode.includes_state().then_some(&state_clone),
                        schema: None,
                    };
                    plugin.notify_event(delivery).map_err(anyhow::Error::from)
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

fn install_plugin_bundle(config: &Config, args: PluginInstallArgs) -> Result<()> {
    config.ensure_data_dir()?;

    let PluginInstallArgs {
        name,
        version,
        source,
        binary_name,
        checksum,
        force,
    } = args;

    let target = current_target_triple();
    let release_target = release_target_triple();
    let plugins_root = config.data_dir.join("plugins");
    let install_dir = plugins_root.join(&name).join(&version).join(&target);
    let registry_path = plugins_root.join("registry.json");

    if install_dir.exists() {
        if !force {
            bail!(
                "plugin '{}' v{} for target {} is already installed; rerun with --force to overwrite",
                name,
                version,
                target
            );
        }
        fs::remove_dir_all(&install_dir).with_context(|| {
            format!(
                "failed to remove existing installation at {}",
                install_dir.display()
            )
        })?;
    }
    fs::create_dir_all(&install_dir)?;

    let (temp_file, source_hint, source_descriptor) =
        resolve_plugin_bundle_source(&name, &version, source.as_deref(), &release_target)?;

    if let Some(expected) = checksum.as_deref() {
        verify_checksum(temp_file.path(), expected)?;
    }

    let header = read_file_header(temp_file.path())?;
    let bundle_format = detect_bundle_format(&source_hint, &header);

    let binary_path = match bundle_format {
        BundleFormat::Binary => {
            let file_name = binary_name
                .clone()
                .unwrap_or_else(|| default_binary_name(&name));
            let dest_path = install_dir.join(&file_name);
            fs::copy(temp_file.path(), &dest_path).with_context(|| {
                format!("failed to copy plugin binary into {}", dest_path.display())
            })?;
            dest_path
        }
        BundleFormat::TarGz => {
            extract_tar_gz(temp_file.path(), &install_dir)?;
            locate_binary(&install_dir, binary_name.as_deref(), &name)?
        }
        BundleFormat::Zip => {
            extract_zip(temp_file.path(), &install_dir)?;
            locate_binary(&install_dir, binary_name.as_deref(), &name)?
        }
    };

    set_executable(&binary_path)?;

    let mut registry = load_installed_registry(&registry_path)?;
    registry.retain(|record| {
        !(record.name == name && record.version == version && record.target == target)
    });

    registry.push(InstalledPluginRecord {
        name: name.clone(),
        version: version.clone(),
        target: target.clone(),
        install_dir: install_dir.clone(),
        binary_path: binary_path.clone(),
        source: Some(source_descriptor),
        checksum,
        installed_at: Utc::now(),
    });

    registry.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then(a.version.cmp(&b.version))
            .then(a.target.cmp(&b.target))
    });

    save_installed_registry(&registry_path, &registry)?;

    println!(
        "installed plugin '{}' v{} for {} at {}",
        name,
        version,
        target,
        binary_path.display()
    );

    Ok(())
}

fn current_target_triple() -> String {
    format!("{}-{}", env::consts::OS, env::consts::ARCH)
}

fn release_target_triple() -> String {
    let arch = env::consts::ARCH;
    let os_suffix = match env::consts::OS {
        "macos" => "apple-darwin",
        "linux" => "unknown-linux-gnu",
        "windows" => "pc-windows-msvc",
        other => other,
    };
    format!("{}-{}", arch, os_suffix)
}

fn resolve_plugin_bundle_source(
    name: &str,
    version: &str,
    explicit_source: Option<&str>,
    release_target: &str,
) -> Result<(NamedTempFile, String, PluginSource)> {
    if let Some(source) = explicit_source {
        return fetch_plugin_source(source);
    }

    let mut attempts: Vec<String> = Vec::new();
    let mut last_error: Option<anyhow::Error> = None;

    if let Some(remote_asset_url) = find_release_asset_download_url(name, version, release_target)?
    {
        attempts.push(remote_asset_url.clone());
        match fetch_plugin_source(&remote_asset_url) {
            Ok(result) => return Ok(result),
            Err(err) => last_error = Some(err),
        }
    }

    let candidates = registry_asset_candidates(name, version, release_target);
    for candidate in &candidates {
        attempts.push(candidate.clone());
        match fetch_plugin_source(candidate) {
            Ok(result) => return Ok(result),
            Err(err) => last_error = Some(err),
        }
    }

    if attempts.is_empty() {
        bail!(
            "no registry download candidates could be constructed for plugin '{}' version {}",
            name,
            version
        );
    }

    let attempts_display = attempts.join(", ");
    match last_error {
        Some(err) => Err(err).context(format!(
            "failed to download plugin '{}' version {} from registry (attempted: {})",
            name, version, attempts_display
        )),
        None => bail!(
            "failed to download plugin '{}' version {} from registry (attempted: {})",
            name,
            version,
            attempts_display
        ),
    }
}

fn canonical_plugin_key(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn release_tag_for(name: &str, version: &str) -> Option<String> {
    let plugin_slug = canonical_plugin_key(name).replace(' ', "-");
    let version_slug = version.trim();
    if plugin_slug.is_empty() || version_slug.is_empty() {
        None
    } else {
        Some(format!("dbx_{}-{}", plugin_slug, version_slug))
    }
}

fn find_release_asset_download_url(
    name: &str,
    version: &str,
    release_target: &str,
) -> Result<Option<String>> {
    let Some(release_tag_guess) = release_tag_for(name, version) else {
        return Ok(None);
    };

    let client = github_api_client()?;
    if let Some(release) = fetch_release_by_tag(&client, &release_tag_guess)? {
        if let Some(url) = select_asset_url(&release, name, release_target) {
            return Ok(Some(url));
        }
    }

    let releases: Vec<GitHubRelease> = client
        .get("https://api.github.com/repos/thachp/dbx_plugins/releases")
        .header("Accept", "application/vnd.github+json")
        .send()
        .context("failed to list plugin releases from GitHub")?
        .error_for_status()
        .context("GitHub returned an error while listing plugin releases")?
        .json()
        .context("failed to decode plugin releases response from GitHub")?;

    let plugin_slug = canonical_plugin_key(name).replace(' ', "-");
    let version_slug = version.trim().to_ascii_lowercase();
    for release in releases {
        let tag_lower = release.tag_name.to_ascii_lowercase();
        if tag_lower.contains(&plugin_slug) && tag_lower.contains(&version_slug) {
            if let Some(url) = select_asset_url(&release, name, release_target) {
                return Ok(Some(url));
            }
        }
    }

    Ok(None)
}

fn fetch_release_by_tag(client: &Client, tag: &str) -> Result<Option<GitHubRelease>> {
    let url = format!(
        "https://api.github.com/repos/thachp/dbx_plugins/releases/tags/{}",
        tag
    );
    let response = client
        .get(&url)
        .header("Accept", "application/vnd.github+json")
        .send()
        .with_context(|| format!("failed to request plugin release metadata for tag {}", tag))?;

    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    if !response.status().is_success() {
        bail!(
            "GitHub returned {} while fetching plugin release metadata for tag {}",
            response.status(),
            tag
        );
    }

    let release = response
        .json()
        .with_context(|| format!("failed to decode plugin release metadata for tag {}", tag))?;
    Ok(Some(release))
}

fn select_asset_url(release: &GitHubRelease, name: &str, release_target: &str) -> Option<String> {
    let target_lower = release_target.to_ascii_lowercase();
    let plugin_key = canonical_plugin_key(name);
    let plugin_slug = plugin_key.replace(' ', "-");

    if let Some(asset) = release.assets.iter().find(|asset| {
        let asset_name = asset.name.to_ascii_lowercase();
        asset_name.contains(&target_lower)
            && (asset_name.contains(&plugin_key) || asset_name.contains(&plugin_slug))
    }) {
        return Some(asset.download_url.clone());
    }

    if let Some(asset) = release
        .assets
        .iter()
        .find(|asset| asset.name.to_ascii_lowercase().contains(&target_lower))
    {
        return Some(asset.download_url.clone());
    }

    release
        .assets
        .first()
        .map(|asset| asset.download_url.clone())
}

fn registry_asset_candidates(name: &str, version: &str, release_target: &str) -> Vec<String> {
    let plugin_slug = canonical_plugin_key(name).replace(' ', "-");
    let version_slug = version.trim();
    if plugin_slug.is_empty() || version_slug.is_empty() {
        return Vec::new();
    }

    let release_tag = format!("dbx_{}-{}", plugin_slug, version_slug);
    let asset_base = release_tag.clone();
    let asset_plain = format!("{}{}.zip", asset_base, release_target);
    let asset_with_dash = format!("{}-{}.zip", asset_base, release_target);
    let base_url = "https://github.com/thachp/dbx_plugins/releases/download";

    let mut candidates = vec![
        format!("{base_url}/{}", asset_plain),
        format!("{base_url}/{}/{}", release_tag, asset_plain),
        format!("{base_url}/{}", asset_with_dash),
        format!("{base_url}/{}/{}", release_tag, asset_with_dash),
    ];
    candidates.sort();
    candidates.dedup();
    candidates
}

fn fetch_plugin_source(source: &str) -> Result<(NamedTempFile, String, PluginSource)> {
    let mut file = NamedTempFile::new().context("failed to create temporary file")?;
    if is_url(source) {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("failed to build HTTP client")?;
        let mut response = client
            .get(source)
            .send()
            .with_context(|| format!("failed to download plugin bundle from {source}"))?
            .error_for_status()
            .with_context(|| format!("received error response from {source}"))?;
        response
            .copy_to(&mut file)
            .context("failed to stream plugin bundle to disk")?;
        let final_url = response.url().clone();
        let hint = final_url
            .path_segments()
            .and_then(|segments| segments.last())
            .filter(|segment| !segment.is_empty())
            .unwrap_or("plugin")
            .to_string();
        Ok((
            file,
            hint,
            PluginSource::Remote {
                registry: final_url.to_string(),
            },
        ))
    } else {
        let path = Path::new(source);
        let mut reader = fs::File::open(path)
            .with_context(|| format!("failed to open plugin bundle at {}", path.display()))?;
        io::copy(&mut reader, &mut file)
            .with_context(|| format!("failed to copy plugin bundle from {}", path.display()))?;
        let resolved = path
            .canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .to_string_lossy()
            .to_string();
        let hint = path
            .file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("plugin")
            .to_string();
        Ok((file, hint, PluginSource::Local { path: resolved }))
    }
}

fn is_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn verify_checksum(path: &Path, expected: &str) -> Result<()> {
    let mut file = fs::File::open(path).with_context(|| {
        format!(
            "failed to open {} for checksum verification",
            path.display()
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let actual = hex::encode(digest);
    if actual.eq_ignore_ascii_case(expected.trim()) {
        Ok(())
    } else {
        bail!(
            "checksum mismatch: expected {}, computed {}",
            expected.trim(),
            actual
        );
    }
}

fn read_file_header(path: &Path) -> Result<[u8; 4]> {
    let mut file = fs::File::open(path)?;
    let mut buffer = [0u8; 4];
    let _ = file.read(&mut buffer)?;
    Ok(buffer)
}

fn detect_bundle_format(hint: &str, header: &[u8; 4]) -> BundleFormat {
    let lowered = hint.to_ascii_lowercase();
    if lowered.ends_with(".tar.gz") || lowered.ends_with(".tgz") {
        return BundleFormat::TarGz;
    }
    if lowered.ends_with(".zip") {
        return BundleFormat::Zip;
    }
    if header.starts_with(&[0x1F, 0x8B]) {
        return BundleFormat::TarGz;
    }
    if header.starts_with(b"PK\x03\x04") {
        return BundleFormat::Zip;
    }
    BundleFormat::Binary
}

enum BundleFormat {
    TarGz,
    Zip,
    Binary,
}

fn extract_tar_gz(source: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest)?;
    let file = fs::File::open(source)?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    archive.unpack(dest)?;
    Ok(())
}

fn extract_zip(source: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest)?;
    let file = fs::File::open(source)?;
    let mut archive = ZipArchive::new(file)?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let entry_path = entry.mangled_name();
        let out_path = dest.join(&entry_path);
        if entry.is_dir() {
            fs::create_dir_all(&out_path)?;
        } else {
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut out_file = fs::File::create(&out_path)?;
            io::copy(&mut entry, &mut out_file)?;
            #[cfg(unix)]
            if let Some(mode) = entry.unix_mode() {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&out_path, fs::Permissions::from_mode(mode))?;
            }
        }
    }
    Ok(())
}

fn locate_binary(root: &Path, binary_name: Option<&str>, plugin_name: &str) -> Result<PathBuf> {
    let files = collect_files(root)?;
    if files.is_empty() {
        bail!("plugin bundle did not contain any files");
    }

    let selected = if let Some(name) = binary_name {
        let needle = OsStr::new(name);
        files
            .iter()
            .find(|path| path.file_name() == Some(needle))
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "could not find binary '{}' in plugin bundle; available files: {}",
                    name,
                    display_file_list(root, &files)
                )
            })?
    } else if files.len() == 1 {
        files[0].clone()
    } else {
        let exe_suffix = env::consts::EXE_SUFFIX;
        let sanitized = plugin_name.replace('-', "_");

        let mut base_names: Vec<String> = Vec::new();
        let mut push_base = |value: String| {
            if !value.is_empty() && !base_names.iter().any(|existing| existing == &value) {
                base_names.push(value);
            }
        };

        push_base(plugin_name.to_string());
        if plugin_name != sanitized {
            push_base(sanitized.clone());
        }
        push_base(format!("dbx_{}", sanitized));
        push_base(format!("dbx_{}", plugin_name));
        push_base(format!("dbx{}", sanitized));
        push_base(format!("dbx{}", plugin_name));
        push_base(format!("dbx-{}", sanitized));
        push_base(format!("dbx-{}", plugin_name));

        let mut candidate_names: Vec<String> = Vec::new();
        let mut register = |value: String| {
            if !value.is_empty() && !candidate_names.iter().any(|existing| existing == &value) {
                candidate_names.push(value);
            }
        };

        for base in &base_names {
            register(base.clone());
            if !exe_suffix.is_empty() {
                register(format!("{}{}", base, exe_suffix));
            }
        }

        let mut resolved: Option<PathBuf> = None;
        for candidate in candidate_names {
            if let Some(path) = files.iter().find(|entry| {
                entry
                    .file_name()
                    .and_then(|value| value.to_str())
                    .map(|value| value == candidate)
                    .unwrap_or(false)
            }) {
                resolved = Some(path.clone());
                break;
            }
        }

        resolved.ok_or_else(|| {
            anyhow!(
                "plugin bundle contains multiple files; specify --bin <filename>. candidates: {}",
                display_file_list(root, &files)
            )
        })?
    };

    if !selected.starts_with(root) {
        bail!(
            "resolved binary {} is outside installation directory",
            selected.display()
        );
    }

    Ok(selected)
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                walk(&path, files)?;
            } else if file_type.is_file() || file_type.is_symlink() {
                files.push(path);
            }
        }
        Ok(())
    }
    walk(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn display_file_list(root: &Path, files: &[PathBuf]) -> String {
    files
        .iter()
        .map(|path| {
            path.strip_prefix(root)
                .unwrap_or(path)
                .display()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn load_installed_registry(path: &Path) -> Result<Vec<InstalledPluginRecord>> {
    registry::load_registry(path).map_err(|err| anyhow!(err))
}

fn save_installed_registry(path: &Path, registry: &[InstalledPluginRecord]) -> Result<()> {
    registry::save_registry(path, registry).map_err(|err| anyhow!(err))
}

fn default_binary_name(plugin_name: &str) -> String {
    if env::consts::EXE_SUFFIX.is_empty() {
        plugin_name.to_string()
    } else {
        format!("{}{}", plugin_name, env::consts::EXE_SUFFIX)
    }
}

fn set_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)?.permissions();
        let mode = permissions.mode();
        permissions.set_mode(mode | 0o111);
        fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

fn resolve_process_installation_version(
    config: &Config,
    plugin_name: &str,
    requested_version: Option<&str>,
) -> Result<String> {
    let target = current_target_triple();
    let registry_path = registry::registry_path(&config.data_dir);
    let records = load_installed_registry(&registry_path)?;

    let mut matches: Vec<InstalledPluginRecord> = records
        .into_iter()
        .filter(|entry| entry.name == plugin_name && entry.target == target)
        .collect();

    if matches.is_empty() {
        bail!(
            "plugin '{}' is not installed for target {}; run `dbx plugin install` first",
            plugin_name,
            target
        );
    }

    if let Some(raw_version) = requested_version {
        let version = raw_version.trim();
        if version.is_empty() {
            bail!("plugin version cannot be empty");
        }
        let found = matches.iter().any(|entry| entry.version == version);
        if found {
            return Ok(version.to_string());
        }
        let available = matches
            .iter()
            .map(|entry| entry.version.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "plugin '{}' version {} is not installed for target {}; installed versions: {}",
            plugin_name,
            version,
            target,
            available
        );
    }

    matches.sort_by(|a, b| compare_versions(&a.version, &b.version));
    matches
        .last()
        .map(|entry| entry.version.clone())
        .ok_or_else(|| {
            anyhow!(
                "failed to determine installed versions for '{}'",
                plugin_name
            )
        })
}

fn compare_versions(a: &str, b: &str) -> Ordering {
    match (parse_semver_loose(a), parse_semver_loose(b)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => a.cmp(b),
    }
}

fn parse_semver_loose(value: &str) -> Option<Version> {
    Version::parse(value)
        .ok()
        .or_else(|| Version::parse(value.trim_start_matches('v')).ok())
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
    let store = EventStore::open(
        config.event_store_path(),
        config.encryption_key()?,
        config.snowflake_worker_id,
    )?;
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

    let plugin_instance = instantiate_plugin(&target_plugin, &config);
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
        plugin.notify_event(PluginDelivery {
            record: Some(&event),
            state: Some(&state),
            schema,
        })?;
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

fn list_plugins(config: &Config, plugins: &[PluginDefinition], json: bool) -> Result<()> {
    let registry_path = registry::registry_path(&config.data_dir);
    let installed_records = load_installed_registry(&registry_path)?;
    let mut installed_by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for record in installed_records {
        let key = canonical_plugin_key(&record.name);
        installed_by_name
            .entry(key)
            .or_default()
            .push(record.version.clone());
    }
    for versions in installed_by_name.values_mut() {
        versions.sort();
        versions.dedup();
    }

    let available = match fetch_available_plugins(&installed_by_name) {
        Ok(list) => list,
        Err(err) => {
            eprintln!("warning: failed to fetch remote plugin catalog: {}", err);
            Vec::new()
        }
    };

    let configured_with_runtime: Vec<ConfiguredPluginInfo> = plugins
        .iter()
        .cloned()
        .map(|definition| {
            let runtime = process_instance_identifier(&definition)
                .map(|identifier| read_process_runtime_state(&config.data_dir, &identifier));
            ConfiguredPluginInfo {
                definition,
                runtime,
            }
        })
        .collect();

    if json {
        let response = PluginListResponse {
            configured: configured_with_runtime.clone(),
            available: available.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }

    if !available.is_empty() {
        println!("Available plugins:");
        for entry in &available {
            let mut details: Vec<String> = Vec::new();
            if let Some(version) = &entry.latest_version {
                details.push(format!("latest {}", version));
            }
            if !entry.installed_versions.is_empty() {
                details.push(format!("installed {}", entry.installed_versions.join(", ")));
            }
            if details.is_empty() {
                println!("  - {}", entry.name);
            } else {
                println!("  - {} ({})", entry.name, details.join(", "));
            }
        }
        println!();
    }

    if configured_with_runtime.is_empty() {
        println!("(no plugins configured)");
        return Ok(());
    }

    println!("Configured plugins:");
    for info in &configured_with_runtime {
        let plugin = &info.definition;
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

        let runtime_note = info
            .runtime
            .as_ref()
            .map(|state| format_process_runtime(state));

        if let Some(name) = &plugin.name {
            match suggestion {
                Some(hint) => match &runtime_note {
                    Some(rt) => println!("  - {} ({}) - {}{}; {}", kind, name, status, hint, rt),
                    None => println!("  - {} ({}) - {}{}", kind, name, status, hint),
                },
                None => match &runtime_note {
                    Some(rt) => println!("  - {} ({}) - {}; {}", kind, name, status, rt),
                    None => println!("  - {} ({}) - {}", kind, name, status),
                },
            }
        } else {
            match suggestion {
                Some(hint) => match &runtime_note {
                    Some(rt) => println!("  - {} - {}{}; {}", kind, status, hint, rt),
                    None => println!("  - {} - {}{}", kind, status, hint),
                },
                None => match &runtime_note {
                    Some(rt) => println!("  - {} - {}; {}", kind, status, rt),
                    None => println!("  - {} - {}", kind, status),
                },
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct AvailablePluginStatus {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest_version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    installed_versions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ConfiguredPluginInfo {
    definition: PluginDefinition,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<ProcessRuntimeState>,
}

#[derive(Debug, Clone, Serialize)]
struct PluginListResponse {
    configured: Vec<ConfiguredPluginInfo>,
    available: Vec<AvailablePluginStatus>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
enum ProcessRuntimeState {
    Running { pid: u32 },
    Stopped,
    Unknown,
}

fn fetch_available_plugins(
    installed_by_name: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<AvailablePluginStatus>> {
    let client = github_api_client()?;

    let contents: Vec<GitHubContentItem> = client
        .get("https://api.github.com/repos/thachp/dbx_plugins/contents/plugins")
        .header("Accept", "application/vnd.github+json")
        .send()
        .context("failed to request plugin catalog from GitHub")?
        .error_for_status()
        .context("GitHub returned an error while fetching plugin catalog")?
        .json()
        .context("failed to decode plugin catalog response from GitHub")?;

    let mut names: BTreeMap<String, String> = BTreeMap::new();
    for item in contents {
        if let Some(name) = normalize_plugin_catalog_entry(&item) {
            let key = canonical_plugin_key(&name);
            names.entry(key).or_insert(name);
        }
    }

    let mut available: Vec<AvailablePluginStatus> = names
        .into_iter()
        .map(|(key, name)| -> Result<AvailablePluginStatus> {
            let latest = fetch_manifest_version(&client, &name)?;
            let installed_versions = installed_by_name.get(&key).cloned().unwrap_or_default();
            Ok(AvailablePluginStatus {
                name,
                latest_version: latest,
                installed_versions,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    available.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(available)
}

fn normalize_plugin_catalog_entry(item: &GitHubContentItem) -> Option<String> {
    match item.r#type.as_str() {
        "dir" => Some(item.name.clone()),
        "file" => item
            .name
            .strip_suffix(".toml")
            .or_else(|| item.name.strip_suffix(".json"))
            .or_else(|| item.name.strip_suffix(".yaml"))
            .or_else(|| item.name.strip_suffix(".yml"))
            .map(|value| value.to_string()),
        _ => None,
    }
}

fn process_instance_identifier(definition: &PluginDefinition) -> Option<String> {
    match &definition.config {
        PluginConfig::Process(settings) => Some(
            definition
                .name
                .clone()
                .unwrap_or_else(|| settings.name.clone()),
        ),
        _ => None,
    }
}

fn read_process_runtime_state(data_dir: &Path, identifier: &str) -> ProcessRuntimeState {
    let path = status_file_path(data_dir, identifier);
    match fs::read_to_string(&path) {
        Ok(contents) => {
            let pid_str = contents.trim();
            if let Ok(pid) = pid_str.parse::<u32>() {
                ProcessRuntimeState::Running { pid }
            } else {
                ProcessRuntimeState::Unknown
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => ProcessRuntimeState::Stopped,
        Err(_) => ProcessRuntimeState::Unknown,
    }
}

fn format_process_runtime(state: &ProcessRuntimeState) -> String {
    match state {
        ProcessRuntimeState::Running { pid } => format!("process running (pid {})", pid),
        ProcessRuntimeState::Stopped => "process stopped".to_string(),
        ProcessRuntimeState::Unknown => "process status unknown".to_string(),
    }
}

fn fetch_manifest_version(client: &Client, name: &str) -> Result<Option<String>> {
    let raw_slug = name.trim().replace(' ', "-");
    let canonical_slug = canonical_plugin_key(name).replace(' ', "-");
    let mut slugs = Vec::new();
    if !raw_slug.is_empty() {
        slugs.push(raw_slug);
    }
    if !canonical_slug.is_empty() && !slugs.contains(&canonical_slug) {
        slugs.push(canonical_slug);
    }

    if slugs.is_empty() {
        return Ok(None);
    }

    let mut last_error: Option<anyhow::Error> = None;
    for slug in slugs {
        let url = format!(
            "https://raw.githubusercontent.com/thachp/dbx_plugins/main/plugins/{}/Cargo.toml",
            slug
        );
        let response = match client
            .get(&url)
            .header("Accept", "application/octet-stream")
            .send()
        {
            Ok(resp) => resp,
            Err(err) => {
                last_error = Some(anyhow!(
                    "failed to request manifest for plugin '{}' at {}: {}",
                    name,
                    url,
                    err
                ));
                continue;
            }
        };

        if response.status() == StatusCode::NOT_FOUND {
            continue;
        }

        if !response.status().is_success() {
            last_error = Some(anyhow!(
                "GitHub returned {} while fetching manifest for plugin '{}' at {}",
                response.status(),
                name,
                url
            ));
            continue;
        }

        let body = match response.text() {
            Ok(text) => text,
            Err(err) => {
                last_error = Some(anyhow!(
                    "failed to read manifest response for plugin '{}' at {}: {}",
                    name,
                    url,
                    err
                ));
                continue;
            }
        };
        let manifest: toml::Value = match toml::from_str(&body) {
            Ok(value) => value,
            Err(err) => {
                last_error = Some(anyhow!(
                    "failed to parse manifest for plugin '{}' at {}: {}",
                    name,
                    url,
                    err
                ));
                continue;
            }
        };
        let version = manifest
            .get("package")
            .and_then(|pkg| pkg.get("version"))
            .and_then(|value| value.as_str())
            .map(|value| value.trim().to_string());

        if version.is_some() {
            return Ok(version);
        }
    }

    if let Some(err) = last_error {
        Err(err)
    } else {
        Ok(None)
    }
}

fn github_api_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent("eventdbx-cli")
        .build()
        .context("failed to construct GitHub HTTP client")
}

#[derive(Debug, Deserialize)]
struct GitHubContentItem {
    name: String,
    #[serde(rename = "type")]
    r#type: String,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    #[serde(default)]
    tag_name: String,
    #[serde(default)]
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    #[serde(rename = "browser_download_url")]
    download_url: String,
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

fn display_label(name: &str) -> &str {
    if name.trim().is_empty() {
        "default"
    } else {
        name
    }
}

pub(crate) fn plugin_kind_name(kind: PluginKind) -> &'static str {
    match kind {
        PluginKind::Tcp => "tcp",
        PluginKind::Capnp => "capnp",
        PluginKind::Http => "http",
        PluginKind::Log => "log",
        PluginKind::Process => "process",
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

pub(crate) fn normalize_plugin_names(plugins: &mut [PluginDefinition]) -> bool {
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

pub(crate) fn dedupe_plugins_by_name(plugins: &mut Vec<PluginDefinition>) -> bool {
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
