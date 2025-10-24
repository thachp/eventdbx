use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Args;

use eventdbx::config::{AdminApiConfigUpdate, Config, ConfigUpdate, load_or_default};

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

    #[arg(long = "plugin-max-attempts")]
    pub plugin_max_attempts: Option<u32>,

    #[arg(long = "snapshot-threshold")]
    pub snapshot_threshold: Option<u64>,

    #[arg(
        long = "clear-snapshot-threshold",
        conflicts_with = "snapshot_threshold"
    )]
    pub clear_snapshot_threshold: bool,
    #[arg(long = "admin-enabled")]
    pub admin_enabled: Option<bool>,
    #[arg(long = "admin-bind")]
    pub admin_bind: Option<String>,
    #[arg(long = "admin-port")]
    pub admin_port: Option<u16>,
    #[arg(long = "snowflake-worker-id")]
    pub snowflake_worker_id: Option<u16>,
}

pub fn execute(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
    if !has_updates(&args) {
        let contents = toml::to_string_pretty(&config)?;
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
        plugin_max_attempts,
        snapshot_threshold,
        clear_snapshot_threshold,
        admin_enabled,
        admin_bind,
        admin_port,
        snowflake_worker_id,
    } = args;

    let data_encryption_key = normalize_secret(data_encryption_key);
    let snapshot_threshold = if clear_snapshot_threshold {
        Some(None)
    } else {
        snapshot_threshold.map(Some)
    };
    let admin_bind = normalize_secret(admin_bind);
    let admin_port_update = admin_port.map(|port| if port == 0 { None } else { Some(port) });
    let admin_update =
        if admin_enabled.is_some() || admin_bind.is_some() || admin_port_update.is_some() {
            Some(AdminApiConfigUpdate {
                enabled: admin_enabled,
                bind_addr: admin_bind,
                port: admin_port_update,
            })
        } else {
            None
        };

    config.apply_update(ConfigUpdate {
        port,
        data_dir,
        cache_threshold,
        snapshot_threshold,
        data_encryption_key,
        restrict: None,
        list_page_size,
        page_limit,
        plugin_max_attempts,
        api: None,
        grpc: None,
        socket: None,
        admin: admin_update,
        snowflake_worker_id,
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

fn has_updates(args: &ConfigArgs) -> bool {
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
        || args.plugin_max_attempts.is_some()
        || args.snapshot_threshold.is_some()
        || args.clear_snapshot_threshold
        || args.admin_enabled.is_some()
        || args
            .admin_bind
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
        || args.admin_port.is_some()
        || args.snowflake_worker_id.is_some()
}
