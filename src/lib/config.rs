use std::{
    collections::BTreeMap,
    convert::TryInto,
    env, fs,
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use rand_core::OsRng;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

use super::error::{EventError, Result};

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
    #[serde(default)]
    pub column_types: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(default = "default_list_page_size")]
    pub list_page_size: usize,
    #[serde(default = "default_page_limit")]
    pub page_limit: usize,
    #[serde(default = "default_plugin_max_attempts")]
    pub plugin_max_attempts: u32,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub remotes: BTreeMap<String, RemoteConfig>,
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
            column_types: BTreeMap::new(),
            list_page_size: default_list_page_size(),
            page_limit: default_page_limit(),
            plugin_max_attempts: default_plugin_max_attempts(),
            replication: ReplicationConfig::default(),
            remotes: BTreeMap::new(),
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
    pub list_page_size: Option<usize>,
    pub page_limit: Option<usize>,
    pub plugin_max_attempts: Option<u32>,
}

pub fn default_config_path() -> Result<PathBuf> {
    let mut path = env::current_dir().map_err(|err| EventError::Config(err.to_string()))?;
    path.push(".eventdbx");
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
        cfg.ensure_replication_identity()?;
        Ok((cfg, config_path))
    } else {
        let cfg = Config::default();
        cfg.ensure_data_dir()?;
        cfg.ensure_replication_identity()?;
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
        if let Some(list_page_size) = update.list_page_size {
            self.list_page_size = list_page_size;
        }
        if let Some(page_limit) = update.page_limit {
            self.page_limit = page_limit;
        }
        if let Some(max_attempts) = update.plugin_max_attempts {
            self.plugin_max_attempts = max_attempts.max(1);
        }
        self.updated_at = Utc::now();
    }

    pub fn set_column_type(&mut self, aggregate: &str, field: &str, data_type: String) {
        self.column_types
            .entry(aggregate.to_string())
            .or_default()
            .insert(field.to_string(), data_type);
    }

    pub fn column_type(&self, aggregate: &str, field: &str) -> Option<&String> {
        self.column_types
            .get(aggregate)
            .and_then(|fields| fields.get(field))
    }

    pub fn ensure_data_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;
        Ok(())
    }

    pub fn identity_key_path(&self) -> PathBuf {
        let path = &self.replication.identity_key;
        if path.is_absolute() {
            path.clone()
        } else {
            self.data_dir.join(path)
        }
    }

    pub fn public_key_path(&self) -> PathBuf {
        let mut path = self.identity_key_path();
        path.set_extension("pub");
        path
    }

    pub fn ensure_replication_identity(&self) -> Result<()> {
        let key_path = self.identity_key_path();
        if key_path.exists() {
            self.ensure_public_key(&key_path)?;
            return Ok(());
        }

        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let signing_key = SigningKey::generate(&mut OsRng);
        write_private_key(&key_path, signing_key.as_bytes())?;

        let verifying_key = signing_key.verifying_key();
        write_public_key(&self.public_key_path(), verifying_key.as_bytes())?;
        Ok(())
    }

    fn ensure_public_key(&self, key_path: &Path) -> Result<()> {
        let pub_path = self.public_key_path();
        if pub_path.exists() {
            return Ok(());
        }

        let bytes = fs::read(key_path)?;
        if bytes.len() != 32 {
            return Err(EventError::Config(format!(
                "invalid replication key length at {}",
                key_path.display()
            )));
        }

        let key_bytes: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| EventError::Config("failed to parse replication key".into()))?;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();
        write_public_key(&pub_path, verifying_key.as_bytes())?;
        Ok(())
    }

    pub fn load_public_key(&self) -> Result<String> {
        self.ensure_replication_identity()?;
        let path = self.public_key_path();
        let contents = fs::read_to_string(&path)?;
        Ok(contents.trim().to_string())
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

    pub fn staging_path(&self) -> PathBuf {
        self.data_dir.join("staged_events.json")
    }

    pub fn plugins_path(&self) -> PathBuf {
        self.data_dir.join("plugins.json")
    }

    pub fn plugin_queue_path(&self) -> PathBuf {
        self.data_dir.join("plugin_queue.json")
    }

    pub fn pid_file_path(&self) -> PathBuf {
        self.data_dir.join("eventdbx.pid")
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

    pub fn load_plugins(&self) -> Result<Vec<PluginDefinition>> {
        let path = self.plugins_path();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let contents = fs::read_to_string(&path)?;
        if contents.trim().is_empty() {
            return Ok(Vec::new());
        }

        let plugins = serde_json::from_str(&contents)?;
        Ok(plugins)
    }

    pub fn save_plugins(&self, plugins: &[PluginDefinition]) -> Result<()> {
        let path = self.plugins_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let payload = serde_json::to_string_pretty(plugins)?;
        fs::write(path, payload)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    #[serde(default = "default_identity_key")]
    pub identity_key: PathBuf,
    #[serde(default = "default_replication_bind")]
    pub bind_addr: String,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            identity_key: default_identity_key(),
            bind_addr: default_replication_bind(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    pub endpoint: String,
    pub public_key: String,
}

fn default_data_dir() -> PathBuf {
    let Ok(current_dir) = env::current_dir() else {
        return PathBuf::from(".eventdbx");
    };
    current_dir.join(".eventdbx")
}

fn default_restrict() -> bool {
    true
}

fn default_list_page_size() -> usize {
    10
}

fn default_page_limit() -> usize {
    1000
}

fn default_plugin_max_attempts() -> u32 {
    10
}

fn default_identity_key() -> PathBuf {
    PathBuf::from("replication.key")
}

fn default_replication_bind() -> String {
    "127.0.0.1:7443".to_string()
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
    #[serde(default)]
    pub name: Option<String>,
    pub config: PluginConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PluginConfig {
    Postgres(PostgresPluginConfig),
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
    #[serde(default)]
    pub https: bool,
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

fn write_private_key(path: &Path, data: &[u8]) -> Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write as _;

    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = file.metadata()?.permissions();
        permissions.set_mode(0o600);
        file.set_permissions(permissions)?;
    }

    file.write_all(data)?;
    Ok(())
}

fn write_public_key(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let encoded = STANDARD_NO_PAD.encode(data);
    fs::write(path, encoded)?;
    Ok(())
}
