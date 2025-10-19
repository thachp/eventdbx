use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Args;

use eventdbx::config::{Config, ConfigUpdate, load_or_default};

#[derive(Args)]
pub struct ConfigArgs {
    #[arg(long)]
    pub port: Option<u16>,

    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    #[arg(long)]
    pub memory_threshold: Option<usize>,

    #[arg(long, alias = "dek")]
    pub data_encryption_key: Option<String>,

    #[arg(long)]
    pub list_page_size: Option<usize>,

    #[arg(long = "page-limit", alias = "event-page-limit")]
    pub page_limit: Option<usize>,

    #[arg(long = "plugin-max-attempts")]
    pub plugin_max_attempts: Option<u32>,
}

pub fn execute(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
    let was_initialized = config.is_initialized();

    let ConfigArgs {
        port,
        data_dir,
        memory_threshold,
        data_encryption_key,
        list_page_size,
        page_limit,
        plugin_max_attempts,
    } = args;

    let data_encryption_key = normalize_secret(data_encryption_key);

    config.apply_update(ConfigUpdate {
        port,
        data_dir,
        memory_threshold,
        data_encryption_key,
        restrict: None,
        list_page_size,
        page_limit,
        plugin_max_attempts,
        api_mode: None,
        hidden_aggregate_types: None,
        hidden_fields: None,
        grpc: None,
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
