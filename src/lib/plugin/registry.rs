use std::{
    fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PluginSource {
    Local { path: String },
    Remote { registry: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledPluginRecord {
    pub name: String,
    pub version: String,
    pub target: String,
    pub install_dir: PathBuf,
    pub binary_path: PathBuf,
    #[serde(default)]
    pub source: Option<PluginSource>,
    #[serde(default)]
    pub checksum: Option<String>,
    pub installed_at: DateTime<Utc>,
}

pub fn load_registry(path: &Path) -> Result<Vec<InstalledPluginRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let contents = fs::read_to_string(path)?;
    if contents.trim().is_empty() {
        return Ok(Vec::new());
    }
    let registry = serde_json::from_str(&contents)?;
    Ok(registry)
}

pub fn save_registry(path: &Path, registry: &[InstalledPluginRecord]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_string_pretty(registry)?;
    fs::write(path, payload)?;
    Ok(())
}

pub fn registry_path(data_dir: &Path) -> PathBuf {
    data_dir.join("plugins").join("registry.json")
}
