use std::{
    collections::BTreeMap,
    convert::TryInto,
    env, fs,
    path::{Path, PathBuf},
};

use argon2::{
    Argon2,
    password_hash::{
        Error as PasswordHashError, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
    },
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use rand_core::{OsRng, RngCore};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{
    encryption::Encryptor,
    error::{EventError, Result},
};

pub const DEFAULT_PORT: u16 = 7070;
pub const DEFAULT_CACHE_THRESHOLD: usize = 10_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ApiMode {
    Rest,
    Graphql,
    Grpc,
    All,
}

impl ApiMode {
    pub fn rest_enabled(self) -> bool {
        matches!(self, ApiMode::Rest | ApiMode::All)
    }

    pub fn graphql_enabled(self) -> bool {
        matches!(self, ApiMode::Graphql | ApiMode::All)
    }

    pub fn grpc_enabled(self) -> bool {
        matches!(self, ApiMode::Grpc | ApiMode::All)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    #[serde(default = "default_cache_threshold", alias = "memory_threshold")]
    pub cache_threshold: usize,
    #[serde(default)]
    pub snapshot_threshold: Option<u64>,
    pub data_encryption_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(
        default = "default_restrict",
        deserialize_with = "deserialize_restrict"
    )]
    pub restrict: bool,
    #[serde(default, skip_serializing)]
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
    #[serde(default)]
    pub grpc: GrpcApiConfig,
    #[serde(default = "default_api_mode")]
    pub api_mode: ApiMode,
    #[serde(default)]
    pub hidden_aggregate_types: Vec<String>,
    #[serde(default)]
    pub hidden_fields: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub admin: AdminApiConfig,
}

impl Default for Config {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            port: DEFAULT_PORT,
            data_dir: default_data_dir(),
            cache_threshold: default_cache_threshold(),
            snapshot_threshold: None,
            data_encryption_key: Some(generate_data_encryption_key()),
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
            grpc: GrpcApiConfig::default(),
            api_mode: default_api_mode(),
            hidden_aggregate_types: Vec::new(),
            hidden_fields: BTreeMap::new(),
            admin: AdminApiConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConfigUpdate {
    pub port: Option<u16>,
    pub data_dir: Option<PathBuf>,
    pub cache_threshold: Option<usize>,
    pub snapshot_threshold: Option<Option<u64>>,
    pub data_encryption_key: Option<String>,
    pub restrict: Option<bool>,
    pub list_page_size: Option<usize>,
    pub page_limit: Option<usize>,
    pub plugin_max_attempts: Option<u32>,
    pub api_mode: Option<ApiMode>,
    pub hidden_aggregate_types: Option<Vec<String>>,
    pub hidden_fields: Option<BTreeMap<String, Vec<String>>>,
    pub grpc: Option<GrpcApiConfigUpdate>,
    pub admin: Option<AdminApiConfigUpdate>,
}

pub fn default_config_path() -> Result<PathBuf> {
    let mut path = default_config_root()?;
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
        let mut cfg: Config = toml::from_str(&contents)?;
        let updated = cfg.ensure_encryption_key();
        cfg.ensure_data_dir()?;
        cfg.ensure_replication_identity()?;
        let migrated = cfg.migrate_plugins()?;
        if updated || migrated {
            cfg.save(&config_path)?;
        }
        Ok((cfg, config_path))
    } else {
        let mut cfg = Config::default();
        let _ = cfg.ensure_encryption_key();
        cfg.ensure_data_dir()?;
        cfg.ensure_replication_identity()?;
        let _ = cfg.migrate_plugins()?;
        cfg.save(&config_path)?;
        Ok((cfg, config_path))
    }
}

impl Config {
    pub fn migrate_plugins(&mut self) -> Result<bool> {
        if self.plugins.is_empty() {
            return Ok(false);
        }

        let existing = self.load_plugins()?;
        if existing.is_empty() {
            self.save_plugins(&self.plugins)?;
        }

        self.plugins.clear();
        self.updated_at = Utc::now();
        Ok(true)
    }

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
        if let Some(threshold) = update.cache_threshold {
            self.cache_threshold = threshold;
        }
        if let Some(snapshot_threshold) = update.snapshot_threshold {
            self.snapshot_threshold = snapshot_threshold;
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
        if let Some(api_mode) = update.api_mode {
            self.api_mode = api_mode;
        }
        if let Some(grpc) = update.grpc {
            if let Some(enabled) = grpc.enabled {
                self.grpc.enabled = enabled;
            }
            if let Some(bind_addr) = grpc.bind_addr {
                self.grpc.bind_addr = bind_addr;
            }
        }
        if let Some(hidden_types) = update.hidden_aggregate_types {
            self.hidden_aggregate_types = hidden_types
                .into_iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect();
        }
        if let Some(hidden_fields) = update.hidden_fields {
            let mut normalized = BTreeMap::new();
            for (aggregate, fields) in hidden_fields {
                let aggregate = aggregate.trim();
                if aggregate.is_empty() {
                    continue;
                }
                let mut normalized_fields: Vec<String> = fields
                    .into_iter()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .collect();
                if normalized_fields.is_empty() {
                    continue;
                }
                normalized_fields.sort();
                normalized_fields.dedup();
                normalized.insert(aggregate.to_string(), normalized_fields);
            }
            self.hidden_fields = normalized;
        }
        if let Some(admin) = update.admin {
            if let Some(enabled) = admin.enabled {
                self.admin.enabled = enabled;
            }
            if let Some(bind_addr) = admin.bind_addr {
                let trimmed = bind_addr.trim();
                if !trimmed.is_empty() {
                    self.admin.bind_addr = trimmed.to_string();
                }
            }
            if let Some(port) = admin.port {
                self.admin.port = port;
            }
        }
        self.updated_at = Utc::now();
    }

    pub fn is_field_hidden(&self, aggregate: &str, field: &str) -> bool {
        self.hidden_fields
            .get(aggregate)
            .map(|fields| fields.iter().any(|item| item == field))
            .unwrap_or(false)
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

    pub fn ensure_encryption_key(&mut self) -> bool {
        if self.is_initialized() {
            return false;
        }

        let encoded = generate_data_encryption_key();
        self.data_encryption_key = Some(encoded);
        self.updated_at = Utc::now();
        true
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
        self.data_encryption_key
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    }

    pub fn encryption_key(&self) -> Result<Option<Encryptor>> {
        match self
            .data_encryption_key
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            Some(value) => Ok(Some(Encryptor::new_from_base64(value)?)),
            None => Ok(None),
        }
    }

    pub fn set_admin_master_key(&mut self, key: &str) -> Result<()> {
        let normalized = key.trim();
        if normalized.is_empty() {
            return Err(EventError::Config(
                "admin master key cannot be empty".to_string(),
            ));
        }

        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(normalized.as_bytes(), &salt)
            .map_err(|err| EventError::Config(format!("failed to hash admin master key: {err}")))?;
        self.admin.master_key_hash = Some(hash.to_string());
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn clear_admin_master_key(&mut self) {
        self.admin.master_key_hash = None;
        self.updated_at = Utc::now();
    }

    pub fn verify_admin_master_key(&self, candidate: &str) -> Result<bool> {
        let Some(stored) = self.admin.master_key_hash.as_ref() else {
            return Ok(false);
        };

        let parsed = PasswordHash::new(stored).map_err(|err| {
            EventError::Config(format!("stored admin master key hash is invalid: {err}"))
        })?;
        let argon2 = Argon2::default();
        let normalized = candidate.trim();
        if normalized.is_empty() {
            return Ok(false);
        }
        match argon2.verify_password(normalized.as_bytes(), &parsed) {
            Ok(_) => Ok(true),
            Err(PasswordHashError::Password) => Ok(false),
            Err(err) => Err(EventError::Config(format!(
                "failed to verify admin master key: {err}"
            ))),
        }
    }

    pub fn admin_master_key_configured(&self) -> bool {
        self.admin
            .master_key_hash
            .as_ref()
            .map(|hash| !hash.trim().is_empty())
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
pub struct GrpcApiConfig {
    #[serde(default = "default_grpc_enabled")]
    pub enabled: bool,
    #[serde(default = "default_grpc_bind_addr")]
    pub bind_addr: String,
}

impl Default for GrpcApiConfig {
    fn default() -> Self {
        Self {
            enabled: default_grpc_enabled(),
            bind_addr: default_grpc_bind_addr(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GrpcApiConfigUpdate {
    pub enabled: Option<bool>,
    pub bind_addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminApiConfig {
    #[serde(default = "default_admin_enabled")]
    pub enabled: bool,
    #[serde(default = "default_admin_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_admin_port")]
    pub port: Option<u16>,
    #[serde(default)]
    pub master_key_hash: Option<String>,
}

impl Default for AdminApiConfig {
    fn default() -> Self {
        Self {
            enabled: default_admin_enabled(),
            bind_addr: default_admin_bind_addr(),
            port: default_admin_port(),
            master_key_hash: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AdminApiConfigUpdate {
    pub enabled: Option<bool>,
    pub bind_addr: Option<String>,
    pub port: Option<Option<u16>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    pub endpoint: String,
    pub public_key: String,
}

fn default_config_root() -> Result<PathBuf> {
    if let Some(home) = dirs::home_dir() {
        Ok(home.join(".eventdbx"))
    } else {
        env::current_dir()
            .map(|dir| dir.join(".eventdbx"))
            .map_err(|err| EventError::Config(err.to_string()))
    }
}

fn generate_data_encryption_key() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    STANDARD.encode(bytes)
}

fn default_data_dir() -> PathBuf {
    default_config_root().unwrap_or_else(|_| PathBuf::from(".eventdbx"))
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

fn default_grpc_enabled() -> bool {
    false
}

fn default_grpc_bind_addr() -> String {
    "127.0.0.1:7442".to_string()
}

fn default_cache_threshold() -> usize {
    DEFAULT_CACHE_THRESHOLD
}

fn default_api_mode() -> ApiMode {
    ApiMode::All
}

fn default_identity_key() -> PathBuf {
    PathBuf::from("replication.key")
}

fn default_replication_bind() -> String {
    "127.0.0.1:7443".to_string()
}

fn default_admin_enabled() -> bool {
    false
}

fn default_admin_bind_addr() -> String {
    "127.0.0.1".to_string()
}

fn default_admin_port() -> Option<u16> {
    Some(7171)
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
    Tcp(TcpPluginConfig),
    Http(HttpPluginConfig),
    Grpc(GrpcPluginConfig),
    Log(LogPluginConfig),
}

impl PluginConfig {
    pub fn kind(&self) -> PluginKind {
        match self {
            PluginConfig::Tcp(_) => PluginKind::Tcp,
            PluginConfig::Http(_) => PluginKind::Http,
            PluginConfig::Grpc(_) => PluginKind::Grpc,
            PluginConfig::Log(_) => PluginKind::Log,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PluginKind {
    Tcp,
    Http,
    Grpc,
    Log,
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
pub struct GrpcPluginConfig {
    pub endpoint: String,
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
        match file.metadata() {
            Ok(metadata) => {
                let mut permissions = metadata.permissions();
                permissions.set_mode(0o600);
                if let Err(err) = file.set_permissions(permissions) {
                    warn!("failed to set permissions on {}: {}", path.display(), err);
                }
            }
            Err(err) => warn!(
                "failed to read metadata for {} when setting permissions: {}",
                path.display(),
                err
            ),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_snapshot_threshold_updates() {
        let mut config = Config::default();
        assert_eq!(config.snapshot_threshold, None);

        config.apply_update(ConfigUpdate {
            snapshot_threshold: Some(Some(42)),
            ..ConfigUpdate::default()
        });
        assert_eq!(config.snapshot_threshold, Some(42));

        config.apply_update(ConfigUpdate {
            snapshot_threshold: Some(None),
            ..ConfigUpdate::default()
        });
        assert_eq!(config.snapshot_threshold, None);
    }
}
