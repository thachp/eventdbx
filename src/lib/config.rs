use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Duration, Utc};
use rand_core::{OsRng, RngCore};
use ring::{
    rand::SystemRandom,
    signature::{ED25519_PUBLIC_KEY_LEN, Ed25519KeyPair, KeyPair},
};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

use super::{
    encryption::Encryptor,
    error::{EventError, Result},
    restrict::{self, RestrictMode},
    token::JwtManagerConfig,
};

pub const DEFAULT_PORT: u16 = 7070;
pub const DEFAULT_SOCKET_PORT: u16 = 6363;
pub const DEFAULT_CACHE_THRESHOLD: usize = 10_000;
pub const DEFAULT_DOMAIN_NAME: &str = "default";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    #[serde(default = "default_jwt_issuer")]
    pub issuer: String,
    #[serde(default = "default_jwt_audience")]
    pub audience: String,
    #[serde(default)]
    pub private_key: Option<String>,
    #[serde(default)]
    pub public_key: Option<String>,
    #[serde(default)]
    pub key_id: Option<String>,
    #[serde(default = "default_jwt_ttl_secs")]
    pub default_ttl_secs: u64,
    #[serde(default = "default_jwt_clock_skew_secs")]
    pub clock_skew_secs: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        let (private_key, public_key) = generate_jwt_keypair()
            .unwrap_or_else(|err| panic!("failed to generate default jwt key pair: {err}"));
        Self {
            issuer: default_jwt_issuer(),
            audience: default_jwt_audience(),
            private_key: Some(private_key),
            public_key: Some(public_key),
            key_id: Some(format!("key-{}", Utc::now().format("%Y%m%d%H%M%S"))),
            default_ttl_secs: default_jwt_ttl_secs(),
            clock_skew_secs: default_jwt_clock_skew_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginQueueConfig {
    #[serde(default = "default_plugin_queue_prune")]
    pub prune: PluginQueuePruneConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginQueuePruneConfig {
    #[serde(default = "default_queue_prune_done_ttl_secs")]
    pub done_ttl_secs: u64,
    #[serde(default = "default_queue_prune_interval_secs")]
    pub interval_secs: u64,
    #[serde(default)]
    pub max_done_jobs: Option<usize>,
}

impl Default for PluginQueuePruneConfig {
    fn default() -> Self {
        Self {
            done_ttl_secs: default_queue_prune_done_ttl_secs(),
            interval_secs: default_queue_prune_interval_secs(),
            max_done_jobs: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    #[serde(default = "default_domain")]
    pub domain: String,
    #[serde(default = "default_cache_threshold", alias = "memory_threshold")]
    pub cache_threshold: usize,
    #[serde(default)]
    pub snapshot_threshold: Option<u64>,
    pub data_encryption_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(
        default = "default_restrict",
        deserialize_with = "deserialize_restrict_mode"
    )]
    pub restrict: RestrictMode,
    #[serde(default, skip_serializing)]
    pub plugins: Vec<PluginDefinition>,
    #[serde(default = "default_list_page_size")]
    pub list_page_size: usize,
    #[serde(default = "default_page_limit")]
    pub page_limit: usize,
    #[serde(default = "default_plugin_max_attempts")]
    pub plugin_max_attempts: u32,
    #[serde(default)]
    pub plugin_queue: PluginQueueConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub remotes: BTreeMap<String, RemoteConfig>,
    #[serde(default)]
    pub socket: SocketConfig,
    #[serde(default)]
    pub admin: AdminApiConfig,
    #[serde(default = "default_verbose_responses")]
    pub verbose_responses: bool,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default = "default_snowflake_worker_id")]
    pub snowflake_worker_id: u16,
}

impl Default for Config {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            port: DEFAULT_PORT,
            data_dir: default_data_dir(),
            domain: default_domain(),
            cache_threshold: default_cache_threshold(),
            snapshot_threshold: None,
            data_encryption_key: Some(generate_data_encryption_key()),
            created_at: now,
            updated_at: now,
            restrict: default_restrict(),
            plugins: Vec::new(),
            list_page_size: default_list_page_size(),
            page_limit: default_page_limit(),
            plugin_max_attempts: default_plugin_max_attempts(),
            plugin_queue: PluginQueueConfig::default(),
            replication: ReplicationConfig::default(),
            remotes: BTreeMap::new(),
            socket: SocketConfig::default(),
            admin: AdminApiConfig::default(),
            verbose_responses: default_verbose_responses(),
            auth: AuthConfig::default(),
            snowflake_worker_id: default_snowflake_worker_id(),
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
    pub restrict: Option<RestrictMode>,
    pub list_page_size: Option<usize>,
    pub page_limit: Option<usize>,
    pub verbose_responses: Option<bool>,
    pub plugin_max_attempts: Option<u32>,
    pub socket: Option<SocketConfigUpdate>,
    pub admin: Option<AdminApiConfigUpdate>,
    pub snowflake_worker_id: Option<u16>,
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
        let auth_updated = cfg.ensure_auth_keys()?;
        let replication_updated = if cfg.socket.bind_addr == "0.0.0.0:7443" {
            cfg.socket.bind_addr = default_socket_bind_addr();
            cfg.updated_at = Utc::now();
            true
        } else {
            false
        };
        cfg.ensure_data_dir()?;
        let migrated = cfg.migrate_plugins()?;
        if updated || migrated || auth_updated || replication_updated {
            cfg.save(&config_path)?;
        }
        Ok((cfg, config_path))
    } else {
        let mut cfg = Config::default();
        let _ = cfg.ensure_encryption_key();
        cfg.ensure_auth_keys()?;
        cfg.ensure_data_dir()?;
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
        if let Some(verbose_responses) = update.verbose_responses {
            self.verbose_responses = verbose_responses;
        }
        if let Some(max_attempts) = update.plugin_max_attempts {
            self.plugin_max_attempts = max_attempts.max(1);
        }
        if let Some(worker_id) = update.snowflake_worker_id {
            self.snowflake_worker_id = worker_id;
        }
        if let Some(socket) = update.socket {
            if let Some(bind_addr) = socket.bind_addr {
                self.socket.bind_addr = bind_addr;
            }
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

    pub fn ensure_data_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;
        fs::create_dir_all(self.domain_data_dir().as_path())?;
        Ok(())
    }

    pub fn active_domain(&self) -> &str {
        &self.domain
    }

    pub fn is_default_domain(&self) -> bool {
        self.domain == DEFAULT_DOMAIN_NAME
    }

    pub fn domains_root(&self) -> PathBuf {
        self.data_dir.join("domains")
    }

    pub fn domain_data_dir(&self) -> PathBuf {
        if self.is_default_domain() {
            self.data_dir.clone()
        } else {
            self.domains_root().join(&self.domain)
        }
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

    pub fn ensure_auth_keys(&mut self) -> Result<bool> {
        let needs_private = self
            .auth
            .private_key
            .as_ref()
            .map(|value| value.trim().is_empty())
            .unwrap_or(true);
        let needs_public = self
            .auth
            .public_key
            .as_ref()
            .map(|value| value.trim().is_empty())
            .unwrap_or(true);
        if !needs_private && !needs_public {
            return Ok(false);
        }

        let (private_key, public_key) = generate_jwt_keypair()?;
        self.auth.private_key = Some(private_key);
        self.auth.public_key = Some(public_key);
        if self.auth.key_id.is_none() {
            self.auth.key_id = Some(format!("key-{}", Utc::now().format("%Y%m%d%H%M%S")));
        }
        self.updated_at = Utc::now();
        Ok(true)
    }

    pub fn event_store_path(&self) -> PathBuf {
        self.domain_data_dir().join("event_store")
    }

    pub fn tokens_path(&self) -> PathBuf {
        self.domain_data_dir().join("tokens.json")
    }

    pub fn cli_token_path(&self) -> PathBuf {
        self.domain_data_dir().join("cli.token")
    }

    pub fn jwt_revocations_path(&self) -> PathBuf {
        self.domain_data_dir().join("jwt_revocations.json")
    }

    pub fn jwt_manager_config(&self) -> Result<JwtManagerConfig> {
        let private = self
            .auth
            .private_key
            .as_ref()
            .ok_or_else(|| EventError::Config("jwt private key not configured".to_string()))?;
        let private_key = STANDARD
            .decode(private.trim())
            .map_err(|err| EventError::Config(format!("invalid jwt private key: {err}")))?;
        if private_key.is_empty() {
            return Err(EventError::Config(
                "jwt private key must contain data".to_string(),
            ));
        }
        let public = self
            .auth
            .public_key
            .as_ref()
            .ok_or_else(|| EventError::Config("jwt public key not configured".to_string()))?;
        let public_key = STANDARD
            .decode(public.trim())
            .map_err(|err| EventError::Config(format!("invalid jwt public key: {err}")))?;
        if public_key.len() != ED25519_PUBLIC_KEY_LEN {
            return Err(EventError::Config(format!(
                "jwt public key must decode to {} bytes (got {})",
                ED25519_PUBLIC_KEY_LEN,
                public_key.len()
            )));
        }
        let key_pair = Ed25519KeyPair::from_pkcs8(private_key.as_slice())
            .map_err(|err| EventError::Config(format!("invalid jwt private key: {err}")))?;
        if key_pair.public_key().as_ref() != public_key.as_slice() {
            return Err(EventError::Config(
                "jwt public key does not match private key".to_string(),
            ));
        }
        let ttl = self.auth.default_ttl_secs.max(60);
        let clock_skew = self.auth.clock_skew_secs.clamp(0, 300);
        Ok(JwtManagerConfig {
            issuer: self.auth.issuer.clone(),
            audience: self.auth.audience.clone(),
            private_key,
            public_key,
            key_id: self.auth.key_id.clone(),
            default_ttl: Duration::seconds(ttl as i64),
            clock_skew: Duration::seconds(clock_skew as i64),
        })
    }

    pub fn schema_store_path(&self) -> PathBuf {
        self.domain_data_dir().join("schemas.json")
    }

    pub fn staging_path(&self) -> PathBuf {
        self.domain_data_dir().join("staged_events.json")
    }

    pub fn plugins_path(&self) -> PathBuf {
        self.domain_data_dir().join("plugins.json")
    }

    pub fn plugin_queue_path(&self) -> PathBuf {
        self.domain_data_dir().join("plugin_queue.json")
    }

    pub fn plugin_queue_db_path(&self) -> PathBuf {
        self.domain_data_dir().join("plugin_queue.db")
    }

    pub fn pid_file_path(&self) -> PathBuf {
        self.domain_data_dir().join("eventdbx.pid")
    }

    pub fn verbose_responses(&self) -> bool {
        self.verbose_responses
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
    #[serde(default = "default_replication_bind")]
    pub bind_addr: String,
    #[serde(default, alias = "identity_key", skip_serializing)]
    pub legacy_identity_key: Option<PathBuf>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_replication_bind(),
            legacy_identity_key: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocketConfig {
    #[serde(default = "default_socket_bind_addr")]
    pub bind_addr: String,
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_socket_bind_addr(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SocketConfigUpdate {
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
    #[serde(default, skip_serializing, alias = "master_key_hash")]
    _deprecated_master_key_hash: Option<String>,
}

impl Default for AdminApiConfig {
    fn default() -> Self {
        Self {
            enabled: default_admin_enabled(),
            bind_addr: default_admin_bind_addr(),
            port: default_admin_port(),
            _deprecated_master_key_hash: None,
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
    #[serde(default, alias = "public_key")]
    pub token: String,
    #[serde(default)]
    pub locator: Option<String>,
    #[serde(default)]
    pub remote_domain: Option<String>,
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

fn default_jwt_issuer() -> String {
    "eventdbx://self".to_string()
}

fn default_jwt_audience() -> String {
    "eventdbx-clients".to_string()
}

fn default_jwt_ttl_secs() -> u64 {
    3_600
}

fn default_jwt_clock_skew_secs() -> u64 {
    30
}

fn generate_jwt_keypair() -> Result<(String, String)> {
    let rng = SystemRandom::new();
    let pkcs8_doc = Ed25519KeyPair::generate_pkcs8(&rng)
        .map_err(|err| EventError::Config(format!("failed to generate jwt key pair: {err}")))?;
    let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_doc.as_ref())
        .map_err(|err| EventError::Config(format!("failed to parse jwt key pair: {err}")))?;
    let private_key = STANDARD.encode(pkcs8_doc.as_ref());
    let public_key = STANDARD.encode(key_pair.public_key().as_ref());
    Ok((private_key, public_key))
}

fn default_data_dir() -> PathBuf {
    default_config_root().unwrap_or_else(|_| PathBuf::from(".eventdbx"))
}

fn default_domain() -> String {
    DEFAULT_DOMAIN_NAME.to_string()
}

fn default_restrict() -> RestrictMode {
    RestrictMode::default()
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

fn default_plugin_payload_mode() -> PluginPayloadMode {
    PluginPayloadMode::All
}

fn default_snowflake_worker_id() -> u16 {
    0
}

fn default_socket_bind_addr() -> String {
    format!("0.0.0.0:{}", DEFAULT_SOCKET_PORT)
}

fn default_cache_threshold() -> usize {
    DEFAULT_CACHE_THRESHOLD
}

fn default_plugin_queue_prune() -> PluginQueuePruneConfig {
    PluginQueuePruneConfig::default()
}

fn default_queue_prune_done_ttl_secs() -> u64 {
    86_400
}

fn default_queue_prune_interval_secs() -> u64 {
    300
}

fn default_replication_bind() -> String {
    format!("0.0.0.0:{}", DEFAULT_SOCKET_PORT)
}

fn default_admin_enabled() -> bool {
    false
}

fn default_admin_bind_addr() -> String {
    "0.0.0.0".to_string()
}

fn default_admin_port() -> Option<u16> {
    Some(7171)
}

fn default_verbose_responses() -> bool {
    true
}

fn deserialize_restrict_mode<'de, D>(deserializer: D) -> std::result::Result<RestrictMode, D::Error>
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
        Some(RestrictInput::Bool(value)) => restrict::legacy_bool(value),
        Some(RestrictInput::String(value)) => {
            restrict::parse_mode(&value).unwrap_or_else(default_restrict)
        }
        None => default_restrict(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDefinition {
    pub enabled: bool,
    #[serde(default = "default_emit_events")]
    pub emit_events: bool,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default = "default_plugin_payload_mode")]
    pub payload_mode: PluginPayloadMode,
    pub config: PluginConfig,
}

fn default_emit_events() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PluginConfig {
    Tcp(TcpPluginConfig),
    Capnp(CapnpPluginConfig),
    Http(HttpPluginConfig),
    Log(LogPluginConfig),
    Process(ProcessPluginConfig),
}

impl PluginConfig {
    pub fn kind(&self) -> PluginKind {
        match self {
            PluginConfig::Tcp(_) => PluginKind::Tcp,
            PluginConfig::Capnp(_) => PluginKind::Capnp,
            PluginConfig::Http(_) => PluginKind::Http,
            PluginConfig::Log(_) => PluginKind::Log,
            PluginConfig::Process(_) => PluginKind::Process,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PluginKind {
    Tcp,
    Capnp,
    Http,
    Log,
    Process,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PluginPayloadMode {
    All,
    EventOnly,
    StateOnly,
    SchemaOnly,
    EventAndSchema,
    ExtensionsOnly,
}

impl PluginPayloadMode {
    pub fn includes_event(self) -> bool {
        matches!(
            self,
            PluginPayloadMode::All
                | PluginPayloadMode::EventOnly
                | PluginPayloadMode::EventAndSchema
        )
    }

    pub fn includes_state(self) -> bool {
        matches!(self, PluginPayloadMode::All | PluginPayloadMode::StateOnly)
    }

    pub fn includes_schema(self) -> bool {
        matches!(
            self,
            PluginPayloadMode::All
                | PluginPayloadMode::SchemaOnly
                | PluginPayloadMode::EventAndSchema
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpPluginConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapnpPluginConfig {
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
pub struct LogPluginConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub template: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessPluginConfig {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
}

fn default_log_level() -> String {
    "info".into()
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

    #[test]
    fn applies_verbose_response_updates() {
        let mut config = Config::default();
        assert!(config.verbose_responses());

        config.apply_update(ConfigUpdate {
            verbose_responses: Some(false),
            ..ConfigUpdate::default()
        });
        assert!(!config.verbose_responses());
    }

    #[derive(Deserialize)]
    struct RestrictWrapper {
        #[serde(
            default = "default_restrict",
            deserialize_with = "deserialize_restrict_mode"
        )]
        restrict: RestrictMode,
    }

    #[test]
    fn restrict_mode_deserializes_from_bool() {
        let wrapper: RestrictWrapper =
            toml::from_str("restrict = false").expect("bool value should parse");
        assert_eq!(wrapper.restrict, RestrictMode::Off);

        let wrapper: RestrictWrapper =
            toml::from_str("restrict = true").expect("bool value should parse");
        assert_eq!(wrapper.restrict, RestrictMode::Default);
    }

    #[test]
    fn restrict_mode_deserializes_from_string() {
        let wrapper: RestrictWrapper =
            toml::from_str("restrict = \"strict\"").expect("string value should parse");
        assert_eq!(wrapper.restrict, RestrictMode::Strict);

        let wrapper: RestrictWrapper = toml::from_str("restrict = \"unknown\"")
            .expect("unknown value should fall back to default");
        assert_eq!(wrapper.restrict, RestrictMode::Default);
    }

    #[test]
    fn restrict_mode_serializes_as_lowercase_string() {
        let mut config = Config::default();
        config.restrict = RestrictMode::Strict;
        let payload = toml::to_string(&config).expect("config should serialize");
        assert!(payload.contains("restrict = \"strict\""));
    }
}
