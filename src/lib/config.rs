use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

use super::error::{EventfulError, Result};

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
    #[serde(
        default = "default_restrict",
        deserialize_with = "deserialize_restrict"
    )]
    pub restrict: bool,
    #[serde(default)]
    pub plugins: Vec<PluginDefinition>,
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
            restrict: default_restrict(),
            plugins: Vec::new(),
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
    pub restrict: Option<bool>,
}

pub fn default_config_path() -> Result<PathBuf> {
    let mut path = env::current_dir().map_err(|err| EventfulError::Config(err.to_string()))?;
    path.push(".eventdb");
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
        if let Some(restrict) = update.restrict {
            self.restrict = restrict;
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
        self.data_dir.join("eventdb.pid")
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

    pub fn set_plugin(&mut self, plugin: PluginDefinition) {
        if let Some(existing) = self
            .plugins
            .iter_mut()
            .find(|item| item.config.kind() == plugin.config.kind())
        {
            *existing = plugin;
        } else {
            self.plugins.push(plugin);
        }
    }
}

fn default_data_dir() -> PathBuf {
    let Ok(current_dir) = env::current_dir() else {
        return PathBuf::from(".eventdb");
    };
    current_dir.join(".eventdb")
}

fn default_restrict() -> bool {
    true
}

fn deserialize_restrict<'de, D>(deserializer: D) -> std::result::Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RestrictInput {
        Bool(bool),
        String(String),
    }

    let value = Option::<RestrictInput>::deserialize(deserializer)?;
    Ok(match value {
        Some(RestrictInput::Bool(value)) => value,
        Some(RestrictInput::String(value)) => match value.trim().to_ascii_lowercase().as_str() {
            "prod" | "restricted" | "true" | "1" | "yes" | "on" => true,
            "dev" | "unrestricted" | "false" | "0" | "no" | "off" => false,
            _ => default_restrict(),
        },
        None => default_restrict(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDefinition {
    pub enabled: bool,
    pub config: PluginConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PluginConfig {
    Postgres(PostgresPluginConfig),
    Sqlite(SqlitePluginConfig),
    Csv(CsvPluginConfig),
    Tcp(TcpPluginConfig),
    Http(HttpPluginConfig),
    Json(JsonPluginConfig),
    Log(LogPluginConfig),
}

impl PluginConfig {
    pub fn kind(&self) -> PluginKind {
        match self {
            PluginConfig::Postgres(_) => PluginKind::Postgres,
            PluginConfig::Sqlite(_) => PluginKind::Sqlite,
            PluginConfig::Csv(_) => PluginKind::Csv,
            PluginConfig::Tcp(_) => PluginKind::Tcp,
            PluginConfig::Http(_) => PluginKind::Http,
            PluginConfig::Json(_) => PluginKind::Json,
            PluginConfig::Log(_) => PluginKind::Log,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PluginKind {
    Postgres,
    Sqlite,
    Csv,
    Tcp,
    Http,
    Json,
    Log,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresPluginConfig {
    pub connection_string: String,
    #[serde(default)]
    pub field_mappings: BTreeMap<String, BTreeMap<String, PostgresColumnConfig>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostgresColumnConfig {
    pub data_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlitePluginConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvPluginConfig {
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpPluginConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpPluginConfig {
    pub endpoint: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonPluginConfig {
    pub path: PathBuf,
    #[serde(default)]
    pub pretty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogPluginConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub template: Option<String>,
}

fn default_log_level() -> String {
    "info".into()
}
