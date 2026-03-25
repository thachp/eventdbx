use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Args;
use serde::Serialize;

use eventdbx::config::{Config, ConfigUpdate, SocketConfigUpdate, load_or_default};

#[derive(Args)]
pub struct ConfigArgs {
    #[arg(long)]
    pub port: Option<u16>,

    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    #[arg(long = "cache-threshold")]
    pub cache_threshold: Option<usize>,

    #[arg(long, alias = "dek")]
    pub data_encryption_key: Option<String>,

    #[arg(long)]
    pub list_page_size: Option<usize>,

    #[arg(long = "page-limit")]
    pub page_limit: Option<usize>,

    #[arg(long = "snowflake-worker-id")]
    pub snowflake_worker_id: Option<u16>,

    #[arg(long = "bind-addr")]
    pub bind_addr: Option<String>,

    /// Disable Noise encryption for the control channel (Cap'n Proto framing only).
    /// WARNING: Sends control protocol messages in plaintext; use only on trusted networks
    /// such as localhost or private links.
    #[arg(long = "no-noise", action = clap::ArgAction::SetTrue)]
    pub no_noise: bool,

    #[arg(long = "noise", action = clap::ArgAction::SetTrue, conflicts_with = "no_noise")]
    pub noise: bool,
}

pub fn execute(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
    if !has_updates(&args, &config) {
        let contents = toml::to_string_pretty(&PrintableConfig::from(&config))?;
        println!("{contents}");
        return Ok(());
    }
    let was_initialized = config.is_initialized();

    let ConfigArgs {
        port,
        data_dir,
        cache_threshold,
        data_encryption_key,
        list_page_size,
        page_limit,
        snowflake_worker_id,
        bind_addr,
        no_noise,
        noise,
    } = args;

    let data_encryption_key = normalize_secret(data_encryption_key);
    let no_noise_update = if no_noise {
        Some(true)
    } else if noise {
        Some(false)
    } else {
        None
    };
    let socket_update = bind_addr.map(|bind_addr| SocketConfigUpdate {
        bind_addr: Some(bind_addr),
    });

    config.apply_update(ConfigUpdate {
        port,
        data_dir,
        cache_threshold,
        snapshot_threshold: None,
        data_encryption_key,
        restrict: None,
        list_page_size,
        page_limit,
        verbose_responses: None,
        reference_default_depth: None,
        reference_max_depth: None,
        plugin_max_attempts: None,
        socket: socket_update,
        snowflake_worker_id,
        tenants: None,
        no_noise: no_noise_update,
    });

    if !was_initialized && !config.is_initialized() {
        return Err(anyhow!(
            "initial setup requires --dek to be provided (32-byte base64 value)"
        ));
    }

    config.ensure_data_dir()?;
    config.save(&path)?;

    tracing::info!("Configuration saved to {}", path.display());
    Ok(())
}

pub fn ensure_secrets_configured(config: &Config) -> Result<()> {
    if config.is_initialized() {
        Ok(())
    } else {
        Err(anyhow!(
            "data encryption key must be configured.\nRun `eventdbx config --dek <value>` during initial setup."
        ))
    }
}

pub fn normalize_secret(input: Option<String>) -> Option<String> {
    input.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn has_updates(args: &ConfigArgs, config: &Config) -> bool {
    args.port.is_some()
        || args.data_dir.is_some()
        || args.cache_threshold.is_some()
        || args
            .data_encryption_key
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
        || args.list_page_size.is_some()
        || args.page_limit.is_some()
        || args.snowflake_worker_id.is_some()
        || args.bind_addr.is_some()
        || (args.no_noise && !config.no_noise)
        || (args.noise && config.no_noise)
}

#[derive(Serialize)]
struct PrintableConfig<'a> {
    port: u16,
    data_dir: &'a std::path::Path,
    cache_threshold: usize,
    list_page_size: usize,
    page_limit: usize,
    socket: &'a eventdbx::config::SocketConfig,
    auth: &'a eventdbx::config::AuthConfig,
    snowflake_worker_id: u16,
    no_noise: bool,
    restrict: eventdbx::restrict::RestrictMode,
}

impl<'a> From<&'a Config> for PrintableConfig<'a> {
    fn from(config: &'a Config) -> Self {
        Self {
            port: config.port,
            data_dir: config.data_dir.as_path(),
            cache_threshold: config.cache_threshold,
            list_page_size: config.list_page_size,
            page_limit: config.page_limit,
            socket: &config.socket,
            auth: &config.auth,
            snowflake_worker_id: config.snowflake_worker_id,
            no_noise: config.no_noise,
            restrict: config.restrict,
        }
    }
}
