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

    #[arg(long = "cache-threshold", alias = "memory-threshold")]
    pub cache_threshold: Option<usize>,

    #[arg(long, alias = "dek")]
    pub data_encryption_key: Option<String>,

    #[arg(long)]
    pub list_page_size: Option<usize>,

    #[arg(long = "page-limit", alias = "event-page-limit")]
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
    #[arg(long = "admin-master-key")]
    pub admin_master_key: Option<String>,
    #[arg(long = "clear-admin-master-key", conflicts_with = "admin_master_key")]
    pub clear_admin_master_key: bool,
}

pub fn execute(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
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
        admin_master_key,
        clear_admin_master_key,
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
    let admin_master_key = normalize_secret(admin_master_key);

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
        api_mode: None,
        hidden_aggregate_types: None,
        hidden_fields: None,
        grpc: None,
        admin: admin_update,
    });

    if let Some(master_key) = admin_master_key {
        config.set_admin_master_key(&master_key)?;
    } else if clear_admin_master_key {
        config.clear_admin_master_key();
    }

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
