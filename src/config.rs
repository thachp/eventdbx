use std::{
    env, fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{EventfulError, Result};

pub const DEFAULT_PORT: u16 = 7070;
pub const DEFAULT_MEMORY_THRESHOLD: usize = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    pub master_key: Option<String>,
    pub memory_threshold: usize,
    pub data_encryption_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Default for Config {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            port: DEFAULT_PORT,
            data_dir: default_data_dir(),
            master_key: None,
            memory_threshold: DEFAULT_MEMORY_THRESHOLD,
            data_encryption_key: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConfigUpdate {
    pub port: Option<u16>,
    pub data_dir: Option<PathBuf>,
    pub master_key: Option<String>,
    pub memory_threshold: Option<usize>,
    pub data_encryption_key: Option<String>,
}

pub fn default_config_path() -> Result<PathBuf> {
    let mut path = env::current_dir().map_err(|err| EventfulError::Config(err.to_string()))?;
    path.push(".eventful");
    path.push("config.toml");
    Ok(path)
}

pub fn load_or_default(path: Option<PathBuf>) -> Result<(Config, PathBuf)> {
    let config_path = if let Some(path) = path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        path
    } else {
        default_config_path()?
    };

    if config_path.exists() {
        let contents = fs::read_to_string(&config_path)?;
        let cfg: Config = toml::from_str(&contents)?;
        cfg.ensure_data_dir()?;
        Ok((cfg, config_path))
    } else {
        let cfg = Config::default();
        cfg.ensure_data_dir()?;
        cfg.save(&config_path)?;
        Ok((cfg, config_path))
    }
}

impl Config {
    pub fn save(&self, path: &Path) -> Result<()> {
        let contents = toml::to_string_pretty(self)?;
        fs::write(path, contents)?;
        Ok(())
    }

    pub fn apply_update(&mut self, update: ConfigUpdate) {
        if let Some(port) = update.port {
            self.port = port;
        }
        if let Some(dir) = update.data_dir {
            self.data_dir = dir;
        }
        if let Some(master_key) = update.master_key {
            self.master_key = Some(master_key);
        }
        if let Some(threshold) = update.memory_threshold {
            self.memory_threshold = threshold;
        }
        if let Some(dek) = update.data_encryption_key {
            self.data_encryption_key = Some(dek);
        }
        self.updated_at = Utc::now();
    }

    pub fn ensure_data_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;
        Ok(())
    }

    pub fn event_store_path(&self) -> PathBuf {
        self.data_dir.join("event_store")
    }

    pub fn tokens_path(&self) -> PathBuf {
        self.data_dir.join("tokens.json")
    }

    pub fn schema_store_path(&self) -> PathBuf {
        self.data_dir.join("schemas.json")
    }

    pub fn pid_file_path(&self) -> PathBuf {
        self.data_dir.join("eventful.pid")
    }

    pub fn is_initialized(&self) -> bool {
        self.master_key
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
            && self
                .data_encryption_key
                .as_ref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
    }
}

fn default_data_dir() -> PathBuf {
    let Ok(current_dir) = env::current_dir() else {
        return PathBuf::from(".eventful");
    };
    current_dir.join(".eventful")
}
