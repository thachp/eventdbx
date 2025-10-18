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
    pub master_key: Option<String>,

    #[arg(long)]
    pub memory_threshold: Option<usize>,

    #[arg(long, alias = "dek")]
    pub data_encryption_key: Option<String>,

    #[arg(long)]
    pub list_page_size: Option<usize>,
}

pub fn execute(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
    let was_initialized = config.is_initialized();

    let ConfigArgs {
        port,
        data_dir,
        master_key,
        memory_threshold,
        data_encryption_key,
        list_page_size,
    } = args;

    let master_key = normalize_secret(master_key);
    let data_encryption_key = normalize_secret(data_encryption_key);

    config.apply_update(ConfigUpdate {
        port,
        data_dir,
        master_key,
        memory_threshold,
        data_encryption_key,
        restrict: None,
        list_page_size,
    });

    if !was_initialized && !config.is_initialized() {
        return Err(anyhow!(
            "initial setup requires both --master-key and --dek to be provided"
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
            "master key and data encryption key must be configured.\nRun `eventdbx config --master-key <value> --dek <value>` during initial setup."
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
