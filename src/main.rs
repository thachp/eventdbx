mod config;
mod error;
mod schema;
mod server;
mod store;
mod token;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::json;
use tracing::info;

use crate::{
    config::{Config, ConfigUpdate, load_or_default},
    schema::{CreateSchemaInput, SchemaManager, SchemaUpdate},
    store::{AppendEvent, EventStore},
    token::{IssueTokenInput, TokenManager},
};

const RUN_MODE_ENV: &str = "EVENTFUL_RUN_MODE";

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RunMode {
    /// Unrestricted development mode (no schema enforcement)
    Dev,
    /// Restricted production mode (schema required)
    Prod,
}

impl RunMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Prod => "prod",
        }
    }

    fn requires_schema(self) -> bool {
        matches!(self, Self::Prod)
    }

    fn from_env() -> Self {
        std::env::var(RUN_MODE_ENV)
            .ok()
            .and_then(|value| RunMode::from_str(&value, true).ok())
            .unwrap_or(RunMode::Prod)
    }
}

fn set_run_mode_env(mode: RunMode) {
    unsafe {
        std::env::set_var(RUN_MODE_ENV, mode.as_str());
    }
}

#[derive(Parser)]
#[command(author, version, about = "EventfulDB server CLI")]
struct Cli {
    /// Path to the configuration file. Defaults to ~/.config/eventful/config.toml
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the EventfulDB server
    Start(StartArgs),
    /// Stop the EventfulDB server
    Stop,
    /// Display EventfulDB server status
    Status,
    /// Restart the EventfulDB server
    Restart(StartArgs),
    /// Destroy all EventfulDB data and configuration
    Destroy(DestroyArgs),
    /// Update system configuration
    Config(ConfigArgs),
    /// Manage access tokens
    Token {
        #[command(subcommand)]
        command: TokenCommands,
    },
    /// Manage schemas
    Schema {
        #[command(subcommand)]
        command: SchemaCommands,
    },
    /// Manage aggregates
    Aggregate {
        #[command(subcommand)]
        command: AggregateCommands,
    },
    /// Internal command used for daemonized server execution
    #[command(name = "__internal:server", hide = true)]
    InternalServer,
}

#[derive(Args, Clone)]
struct StartArgs {
    /// Override the configured server port
    #[arg(long)]
    port: Option<u16>,

    /// Override the configured data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Run the server in the foreground instead of daemonizing
    #[arg(long)]
    foreground: bool,

    /// Run mode (dev = unrestricted, prod = schema-enforced)
    #[arg(long, value_enum, default_value_t = RunMode::Prod)]
    mode: RunMode,
}

#[derive(Args)]
struct DestroyArgs {
    /// Skip confirmation prompt
    #[arg(long)]
    yes: bool,
}

impl Default for StartArgs {
    fn default() -> Self {
        Self {
            port: None,
            data_dir: None,
            foreground: false,
            mode: RunMode::from_env(),
        }
    }
}

#[derive(Args)]
struct ConfigArgs {
    #[arg(long)]
    port: Option<u16>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    master_key: Option<String>,

    #[arg(long)]
    memory_threshold: Option<usize>,

    #[arg(long, alias = "dek")]
    data_encryption_key: Option<String>,
}

#[derive(Subcommand)]
enum TokenCommands {
    /// Generate a new token
    Generate(TokenGenerateArgs),
    /// List configured tokens
    List,
    /// Revoke an active token
    Revoke {
        /// Token value to revoke
        token: String,
    },
    /// Refresh an existing token
    Refresh(TokenRefreshArgs),
}

#[derive(Args)]
struct TokenGenerateArgs {
    #[arg(long)]
    identifier_type: String,

    #[arg(long)]
    identifier_id: String,

    #[arg(long)]
    expiration: Option<u64>,

    #[arg(long)]
    limit: Option<u64>,

    #[arg(long, default_value_t = false)]
    keep_alive: bool,
}

#[derive(Args)]
struct TokenRefreshArgs {
    #[arg(long)]
    token: String,

    #[arg(long)]
    expiration: Option<u64>,

    #[arg(long)]
    limit: Option<u64>,
}

#[derive(Subcommand)]
enum SchemaCommands {
    /// Create a new schema definition
    Create(SchemaCreateArgs),
    /// Alter an existing schema definition
    Alter(SchemaAlterArgs),
    /// Remove an event definition from an aggregate
    Remove(SchemaRemoveEventArgs),
    /// List available schemas
    List,
    /// Show a specific schema
    Show {
        /// Aggregate name
        aggregate: String,
    },
}

#[derive(Args)]
struct SchemaCreateArgs {
    #[arg(long)]
    aggregate: String,

    #[arg(long, value_delimiter = ',')]
    events: Vec<String>,

    #[arg(long)]
    snapshot_threshold: Option<u64>,
}

#[derive(Args)]
struct SchemaAlterArgs {
    /// Aggregate name
    aggregate: String,

    /// Optional event name to alter
    #[arg(long, short = 'e')]
    event: Option<String>,

    /// Set the snapshot threshold
    #[arg(long)]
    snapshot_threshold: Option<u64>,

    /// Lock or unlock the aggregate (or field when --field is specified)
    #[arg(long)]
    lock: Option<bool>,

    /// Field name to lock/unlock
    #[arg(long)]
    field: Option<String>,

    /// Add fields to an event definition
    #[arg(long, short = 'a', value_delimiter = ',')]
    add: Vec<String>,

    /// Remove fields from an event definition
    #[arg(long, short = 'r', value_delimiter = ',')]
    remove: Vec<String>,
}

#[derive(Args)]
struct SchemaRemoveEventArgs {
    /// Aggregate name
    aggregate: String,

    /// Event name to remove
    event: String,
}

#[derive(Subcommand)]
enum AggregateCommands {
    /// Apply an event to an aggregate instance
    Apply(AggregateApplyArgs),
    /// List aggregates in the store
    List,
    /// Retrieve the state of an aggregate
    Get(AggregateGetArgs),
    /// Replay events for an aggregate instance
    Replay(AggregateReplayArgs),
    /// Verify an aggregate's Merkle root
    Verify(AggregateVerifyArgs),
    /// Create a snapshot of the aggregate state
    Snapshot(AggregateSnapshotArgs),
    /// Archive an aggregate instance
    Archive(AggregateArchiveArgs),
    /// Restore an archived aggregate instance
    Restore(AggregateArchiveArgs),
    /// Commit staged events (no-op placeholder)
    Commit,
}

#[derive(Args)]
struct AggregateSnapshotArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Optional comment to record with the snapshot
    #[arg(long)]
    comment: Option<String>,
}

#[derive(Args)]
struct AggregateArchiveArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Optional comment recorded with the action
    #[arg(long)]
    comment: Option<String>,
}

#[derive(Args)]
struct AggregateApplyArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Event type to append
    event: String,

    /// Event fields expressed as KEY=VALUE pairs
    #[arg(long = "field", value_parser = parse_key_value, value_name = "KEY=VALUE")]
    fields: Vec<KeyValue>,
}

#[derive(Args)]
struct AggregateGetArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Optional version to compute state at
    #[arg(long)]
    version: Option<u64>,

    /// Include event history in the output
    #[arg(long, default_value_t = false)]
    include_events: bool,
}

#[derive(Args)]
struct AggregateReplayArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,

    /// Number of events to skip
    #[arg(long, default_value_t = 0)]
    skip: usize,

    /// Number of events to return
    #[arg(long)]
    take: Option<usize>,
}

#[derive(Args)]
struct AggregateVerifyArgs {
    /// Aggregate type
    aggregate: String,

    /// Aggregate identifier
    aggregate_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) => start_command(cli.config, args).await?,
        Commands::Stop => stop_command(cli.config)?,
        Commands::Status => status_command(cli.config)?,
        Commands::Restart(args) => restart_command(cli.config, args).await?,
        Commands::Destroy(args) => destroy_command(cli.config, args)?,
        Commands::Config(args) => config_command(cli.config, args)?,
        Commands::Token { command } => token_command(cli.config, command)?,
        Commands::Schema { command } => schema_command(cli.config, command)?,
        Commands::Aggregate { command } => aggregate_command(cli.config, command)?,
        Commands::InternalServer => start_foreground(cli.config, StartArgs::default()).await?,
    }

    Ok(())
}

async fn start_command(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    if args.foreground {
        start_foreground(config_path, args).await
    } else {
        start_daemon(config_path, args)?;
        Ok(())
    }
}

async fn restart_command(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    if let Err(err) = stop_command(config_path.clone()) {
        tracing::warn!("failed to stop EventfulDB server before restart: {err}");
    }
    start_command(config_path, args).await
}

async fn start_foreground(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    set_run_mode_env(args.mode);
    let (config, _) = load_and_update_config(config_path, &args)?;
    server::run(config, args.mode).await?;
    Ok(())
}

fn start_daemon(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    set_run_mode_env(args.mode);
    let (config, path) = load_and_update_config(config_path, &args)?;
    let pid_path = config.pid_file_path();

    if let Some(existing) = read_pid_record(&pid_path)? {
        if process_is_running(existing.pid) {
            return Err(anyhow!(
                "EventfulDB server already running (pid {})",
                existing.pid
            ));
        }
        fs::remove_file(&pid_path)?;
    }

    let mut command = Command::new(env::current_exe()?);
    command.arg("--config").arg(&path);
    command.arg("__internal:server");
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());
    command.env(RUN_MODE_ENV, args.mode.as_str());

    let child = command.spawn()?;
    let pid = child.id();
    drop(child);

    let started_at = Utc::now();
    let record = PidRecord {
        pid,
        started_at: Some(started_at),
    };
    write_pid_record(&pid_path, &record)?;
    println!(
        "EventfulDB server is running on port {} (pid {}) since {}",
        config.port,
        pid,
        started_at.to_rfc3339()
    );
    Ok(())
}

fn load_and_update_config(
    config_path: Option<PathBuf>,
    args: &StartArgs,
) -> Result<(Config, PathBuf)> {
    let (mut config, path) = load_or_default(config_path)?;
    apply_start_overrides(&mut config, args);
    ensure_secrets_configured(&config)?;
    config.ensure_data_dir()?;
    config.save(&path)?;
    Ok((config, path))
}

fn apply_start_overrides(config: &mut Config, args: &StartArgs) {
    config.apply_update(ConfigUpdate {
        port: args.port,
        data_dir: args.data_dir.clone(),
        run_mode: Some(args.mode),
        ..ConfigUpdate::default()
    });
}

#[derive(Debug, Serialize, Deserialize)]
struct PidRecord {
    pid: u32,
    #[serde(default)]
    started_at: Option<DateTime<Utc>>,
}

fn stop_command(config_path: Option<PathBuf>) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let pid_path = config.pid_file_path();

    let Some(record) = read_pid_record(&pid_path)? else {
        println!("No running EventfulDB server found.");
        return Ok(());
    };
    let pid = record.pid;

    if !process_is_running(pid) {
        fs::remove_file(&pid_path)?;
        println!("Removed stale EventfulDB server pid file.");
        return Ok(());
    }

    terminate_process(pid)?;
    if !wait_for_exit(pid, Duration::from_secs(5)) {
        #[cfg(unix)]
        {
            force_kill_process(pid)?;
            if !wait_for_exit(pid, Duration::from_secs(2)) {
                return Err(anyhow!(
                    "failed to stop EventfulDB server (pid {pid}); process is still running"
                ));
            }
        }
        #[cfg(not(unix))]
        {
            return Err(anyhow!(
                "failed to stop EventfulDB server (pid {pid}); process is still running"
            ));
        }
    }

    fs::remove_file(&pid_path)?;
    if let Some(started_at) = record.started_at {
        println!(
            "EventfulDB server stopped (pid {}) after {} (started {})",
            pid,
            describe_uptime(started_at),
            started_at.to_rfc3339()
        );
    } else {
        println!("EventfulDB server stopped (pid {})", pid);
    }
    Ok(())
}

fn status_command(config_path: Option<PathBuf>) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let pid_path = config.pid_file_path();

    match read_pid_record(&pid_path)? {
        Some(record) => {
            let pid = record.pid;
            if process_is_running(pid) {
                if let Some(started_at) = record.started_at {
                    println!(
                        "EventfulDB server is running on port {} (pid {}) — mode={} — up for {} (since {})",
                        config.port,
                        pid,
                        config.run_mode.as_str(),
                        describe_uptime(started_at),
                        started_at.to_rfc3339()
                    );
                } else {
                    println!(
                        "EventfulDB server is running on port {} (pid {}) — mode={}",
                        config.port,
                        pid,
                        config.run_mode.as_str()
                    );
                }
            } else {
                let _ = fs::remove_file(&pid_path);
                println!("EventfulDB server is not running (removed stale pid file).");
            }
        }
        None => println!("EventfulDB server is not running."),
    }

    Ok(())
}

fn destroy_command(config_path: Option<PathBuf>, args: DestroyArgs) -> Result<()> {
    let (config, path) = load_or_default(config_path)?;

    if !args.yes {
        eprint!(
            "This will permanently delete all EventfulDB data under {} and remove the config file at {}.\nType \"destroy\" to continue: ",
            config.data_dir.display(),
            path.display()
        );
        io::stderr().flush()?;
        let mut confirmation = String::new();
        io::stdin().read_line(&mut confirmation)?;
        if confirmation.trim() != "destroy" {
            println!("Destroy command cancelled.");
            return Ok(());
        }
    }

    if let Err(err) = stop_command(Some(path.clone())) {
        tracing::warn!("failed to stop running server before destroy: {err}");
    }

    if config.data_dir.exists() {
        fs::remove_dir_all(&config.data_dir)?;
    }

    if path.exists() {
        fs::remove_file(&path)?;
    }

    println!(
        "All EventfulDB data and configuration removed from {}",
        config.data_dir.display()
    );
    Ok(())
}

fn write_pid_record(path: &Path, record: &PidRecord) -> Result<()> {
    let contents = serde_json::to_string(record)?;
    fs::write(path, contents)?;
    Ok(())
}

fn read_pid_record(path: &Path) -> Result<Option<PidRecord>> {
    if !path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(path)?;
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if let Ok(record) = serde_json::from_str::<PidRecord>(trimmed) {
        return Ok(Some(record));
    }

    if let Ok(pid) = trimmed.parse::<u32>() {
        return Ok(Some(PidRecord {
            pid,
            started_at: None,
        }));
    }

    Err(anyhow!("invalid pid file at {}", path.display()))
}

fn wait_for_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if !process_is_running(pid) {
            return true;
        }
        if Instant::now() >= deadline {
            return !process_is_running(pid);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    unsafe {
        if libc::kill(pid as libc::pid_t, 0) == 0 {
            true
        } else {
            let err = io::Error::last_os_error();
            !matches!(err.raw_os_error(), Some(libc::ESRCH))
        }
    }
}

#[cfg(windows)]
fn process_is_running(pid: u32) -> bool {
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION},
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle == 0 {
            false
        } else {
            CloseHandle(handle);
            true
        }
    }
}

#[cfg(not(any(unix, windows)))]
fn process_is_running(_pid: u32) -> bool {
    false
}

fn describe_uptime(started_at: DateTime<Utc>) -> String {
    let now = Utc::now();
    let elapsed = now.signed_duration_since(started_at);
    match elapsed.to_std() {
        Ok(duration) => format_human_duration(duration),
        Err(_) => "unknown duration".to_string(),
    }
}

fn format_human_duration(duration: Duration) -> String {
    let mut secs = duration.as_secs();
    if secs == 0 {
        return "under 1s".to_string();
    }

    let days = secs / 86_400;
    secs %= 86_400;
    let hours = secs / 3_600;
    secs %= 3_600;
    let minutes = secs / 60;
    let seconds = secs % 60;

    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if minutes > 0 {
        parts.push(format!("{}m", minutes));
    }
    if seconds > 0 && parts.len() < 3 {
        parts.push(format!("{}s", seconds));
    }

    if parts.is_empty() {
        "under 1s".to_string()
    } else {
        parts.join(" ")
    }
}

#[cfg(unix)]
fn terminate_process(pid: u32) -> Result<()> {
    unsafe {
        if libc::kill(pid as libc::pid_t, libc::SIGTERM) == 0 {
            Ok(())
        } else {
            let err = io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::ESRCH)) {
                Ok(())
            } else {
                Err(anyhow!("failed to send SIGTERM to pid {pid}: {err}"))
            }
        }
    }
}

#[cfg(windows)]
fn terminate_process(pid: u32) -> Result<()> {
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::Threading::{OpenProcess, PROCESS_TERMINATE, TerminateProcess},
    };

    unsafe {
        let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
        if handle == 0 {
            return Err(anyhow!("failed to open process {pid} for termination"));
        }
        let result = TerminateProcess(handle, 0);
        CloseHandle(handle);
        if result == 0 {
            return Err(anyhow!("failed to terminate process {pid}"));
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn terminate_process(pid: u32) -> Result<()> {
    Err(anyhow!(
        "process control is not supported on this platform (pid {pid})"
    ))
}

#[cfg(unix)]
fn force_kill_process(pid: u32) -> Result<()> {
    unsafe {
        if libc::kill(pid as libc::pid_t, libc::SIGKILL) == 0 {
            Ok(())
        } else {
            let err = io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::ESRCH)) {
                Ok(())
            } else {
                Err(anyhow!("failed to send SIGKILL to pid {pid}: {err}"))
            }
        }
    }
}

fn config_command(config_path: Option<PathBuf>, args: ConfigArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
    let was_initialized = config.is_initialized();

    let ConfigArgs {
        port,
        data_dir,
        master_key,
        memory_threshold,
        data_encryption_key,
    } = args;

    let master_key = normalize_secret(master_key);
    let data_encryption_key = normalize_secret(data_encryption_key);

    config.apply_update(ConfigUpdate {
        port,
        data_dir,
        master_key,
        memory_threshold,
        data_encryption_key,
        run_mode: None,
    });

    if !was_initialized && !config.is_initialized() {
        return Err(anyhow!(
            "initial setup requires both --master-key and --dek to be provided"
        ));
    }

    config.ensure_data_dir()?;
    config.save(&path)?;

    info!("Configuration saved to {}", path.display());
    Ok(())
}

fn normalize_secret(input: Option<String>) -> Option<String> {
    input.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn ensure_secrets_configured(config: &Config) -> Result<()> {
    if config.is_initialized() {
        Ok(())
    } else {
        Err(anyhow!(
            "master key and data encryption key must be configured.\nRun `eventful config --master-key <value> --dek <value>` during initial setup."
        ))
    }
}

fn token_command(config_path: Option<PathBuf>, command: TokenCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = TokenManager::load(config.tokens_path())?;

    match command {
        TokenCommands::Generate(args) => {
            ensure_secrets_configured(&config)?;
            let record = manager.issue(IssueTokenInput {
                identifier_type: args.identifier_type,
                identifier_id: args.identifier_id,
                expiration_secs: args.expiration,
                limit: args.limit,
                keep_alive: args.keep_alive,
            })?;
            println!(
                "token={} identifier_type={} identifier_id={} expires_at={} remaining_writes={}",
                record.token,
                record.identifier_type,
                record.identifier_id,
                record
                    .expires_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "never".into()),
                record
                    .remaining_writes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unlimited".into())
            );
        }
        TokenCommands::List => {
            for record in manager.list() {
                println!(
                    "token={} status={:?} issued_at={} expires_at={} remaining_writes={}",
                    record.token,
                    record.status,
                    record.issued_at.to_rfc3339(),
                    record
                        .expires_at
                        .map(|ts| ts.to_rfc3339())
                        .unwrap_or_else(|| "never".into()),
                    record
                        .remaining_writes
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unlimited".into())
                );
            }
        }
        TokenCommands::Revoke { token } => {
            manager.revoke(&token)?;
            println!("token {} revoked", token);
        }
        TokenCommands::Refresh(args) => {
            let record = manager.refresh(&args.token, args.expiration, args.limit)?;
            println!(
                "token={} expires_at={} remaining_writes={}",
                record.token,
                record
                    .expires_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "never".into()),
                record
                    .remaining_writes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unlimited".into())
            );
        }
    }

    Ok(())
}

fn schema_command(config_path: Option<PathBuf>, command: SchemaCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = SchemaManager::load(config.schema_store_path())?;

    match command {
        SchemaCommands::Create(args) => {
            let schema = manager.create(CreateSchemaInput {
                aggregate: args.aggregate,
                events: args.events,
                snapshot_threshold: args.snapshot_threshold,
            })?;
            println!(
                "schema={} events={} snapshot_threshold={:?}",
                schema.aggregate,
                schema.events.len(),
                schema.snapshot_threshold
            );
        }
        SchemaCommands::Alter(args) => {
            let mut update = SchemaUpdate::default();

            if let Some(value) = args.snapshot_threshold {
                update.snapshot_threshold = Some(Some(value));
            }

            if args.event.is_none() && args.field.is_none() {
                if let Some(lock) = args.lock {
                    update.locked = Some(lock);
                }
            }

            if let Some(field) = args.field {
                if let Some(lock) = args.lock {
                    update.field_lock = Some((field, lock));
                } else {
                    return Err(anyhow::anyhow!(
                        "--lock must be provided when using --field"
                    ));
                }
            }

            if let Some(event) = args.event {
                if !args.add.is_empty() {
                    update
                        .event_add_fields
                        .insert(event.clone(), args.add.clone());
                }
                if !args.remove.is_empty() {
                    update
                        .event_remove_fields
                        .insert(event.clone(), args.remove.clone());
                }
                if args.add.is_empty() && args.remove.is_empty() {
                    return Err(anyhow::anyhow!(
                        "provide --add or --remove when specifying an event"
                    ));
                }
            } else if (!args.add.is_empty() || !args.remove.is_empty()) && args.event.is_none() {
                return Err(anyhow::anyhow!(
                    "--event must be provided when adding or removing fields"
                ));
            }

            let schema = manager.update(&args.aggregate, update)?;
            println!(
                "schema={} updated_at={} version_events={}",
                schema.aggregate,
                schema.updated_at.to_rfc3339(),
                schema.events.len()
            );
        }
        SchemaCommands::Remove(args) => {
            let schema = manager.remove_event(&args.aggregate, &args.event)?;
            println!(
                "schema={} removed_event={} remaining_events={}",
                schema.aggregate,
                args.event,
                schema.events.len()
            );
        }
        SchemaCommands::List => {
            for schema in manager.list() {
                println!(
                    "schema={} events={} locked={} snapshot_threshold={:?}",
                    schema.aggregate,
                    schema.events.len(),
                    schema.locked,
                    schema.snapshot_threshold
                );
            }
        }
        SchemaCommands::Show { aggregate } => {
            let schema = manager.get(&aggregate)?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
    }

    Ok(())
}

fn aggregate_command(config_path: Option<PathBuf>, command: AggregateCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    match command {
        AggregateCommands::List => {
            let store = EventStore::open_read_only(config.event_store_path())?;
            for aggregate in store.aggregates() {
                println!(
                    "aggregate_type={} aggregate_id={} version={} merkle_root={} archived={}",
                    aggregate.aggregate_type,
                    aggregate.aggregate_id,
                    aggregate.version,
                    aggregate.merkle_root,
                    aggregate.archived
                );
            }
        }
        AggregateCommands::Get(args) => {
            let store = EventStore::open_read_only(config.event_store_path())?;
            let mut state = store.get_aggregate_state(&args.aggregate, &args.aggregate_id)?;
            let mut events_cache = None;

            if args.version.is_some() || args.include_events {
                events_cache = Some(store.list_events(&args.aggregate, &args.aggregate_id)?);
            }

            if let Some(version) = args.version {
                let events = events_cache
                    .as_ref()
                    .expect("events cache should be populated");
                let (target_version, target_state, merkle_root) = state_at_version(events, version);
                state.version = target_version;
                state.state = target_state;
                state.merkle_root = merkle_root;
            }

            let mut output = json!({
                "aggregate_type": state.aggregate_type,
                "aggregate_id": state.aggregate_id,
                "version": state.version,
                "state": state.state,
                "merkle_root": state.merkle_root,
            });

            if args.include_events {
                let events = match events_cache.take() {
                    Some(events) => events,
                    None => store.list_events(&args.aggregate, &args.aggregate_id)?,
                };
                let filtered: Vec<_> = match args.version {
                    Some(version) => events
                        .into_iter()
                        .filter(|event| event.version <= version)
                        .collect(),
                    None => events,
                };
                output["events"] = serde_json::to_value(filtered)?;
            }

            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        AggregateCommands::Apply(args) => {
            let payload = collect_payload(args.fields);
            let schema_manager = SchemaManager::load(config.schema_store_path())?;
            if config.run_mode.requires_schema() {
                schema_manager.validate_event(&args.aggregate, &args.event, &payload)?;
            }

            let store = EventStore::open(config.event_store_path())?;
            let record = store.append(AppendEvent {
                aggregate_type: args.aggregate,
                aggregate_id: args.aggregate_id,
                event_type: args.event,
                payload,
                issued_by: None,
            })?;

            println!("{}", serde_json::to_string_pretty(&record)?);
        }
        AggregateCommands::Replay(args) => {
            let store = EventStore::open_read_only(config.event_store_path())?;
            let events = store.list_events(&args.aggregate, &args.aggregate_id)?;
            let iter = events.into_iter().skip(args.skip);
            let events: Vec<_> = if let Some(limit) = args.take {
                iter.take(limit).collect()
            } else {
                iter.collect()
            };

            for event in events {
                println!("{}", serde_json::to_string_pretty(&event)?);
            }
        }
        AggregateCommands::Verify(args) => {
            let store = EventStore::open_read_only(config.event_store_path())?;
            let merkle_root = store.verify(&args.aggregate, &args.aggregate_id)?;
            println!(
                "aggregate_type={} aggregate_id={} merkle_root={}",
                args.aggregate, args.aggregate_id, merkle_root
            );
        }
        AggregateCommands::Snapshot(args) => {
            let store = EventStore::open(config.event_store_path())?;
            let snapshot =
                store.create_snapshot(&args.aggregate, &args.aggregate_id, args.comment.clone())?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        AggregateCommands::Archive(args) => {
            let store = EventStore::open(config.event_store_path())?;
            let meta = store.set_archive(
                &args.aggregate,
                &args.aggregate_id,
                true,
                args.comment.clone(),
            )?;
            println!(
                "aggregate_type={} aggregate_id={} archived={} comment={}",
                meta.aggregate_type,
                meta.aggregate_id,
                meta.archived,
                args.comment.unwrap_or_default()
            );
        }
        AggregateCommands::Restore(args) => {
            let store = EventStore::open(config.event_store_path())?;
            let meta = store.set_archive(
                &args.aggregate,
                &args.aggregate_id,
                false,
                args.comment.clone(),
            )?;
            println!(
                "aggregate_type={} aggregate_id={} archived={} comment={}",
                meta.aggregate_type,
                meta.aggregate_id,
                meta.archived,
                args.comment.unwrap_or_default()
            );
        }
        AggregateCommands::Commit => {
            println!("all events are already committed");
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct KeyValue {
    key: String,
    value: String,
}

fn parse_key_value(raw: &str) -> Result<KeyValue, String> {
    let mut parts = raw.splitn(2, '=');
    let key = parts
        .next()
        .ok_or_else(|| "missing key".to_string())?
        .trim();
    let value = parts
        .next()
        .ok_or_else(|| "missing value".to_string())?
        .trim();

    if key.is_empty() {
        return Err("field key cannot be empty".to_string());
    }

    Ok(KeyValue {
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn collect_payload(fields: Vec<KeyValue>) -> BTreeMap<String, String> {
    let mut payload = BTreeMap::new();
    for kv in fields {
        payload.insert(kv.key, kv.value);
    }
    payload
}

fn state_at_version(
    events: &[store::EventRecord],
    version: u64,
) -> (u64, BTreeMap<String, String>, String) {
    let mut state = BTreeMap::new();
    let mut hashes = Vec::new();
    let mut last_version = 0;

    for event in events {
        if event.version > version {
            break;
        }
        last_version = event.version;
        hashes.push(event.hash.clone());
        for (key, value) in &event.payload {
            state.insert(key.clone(), value.clone());
        }
    }

    let merkle_root = store::compute_merkle_root(&hashes);
    (last_version, state, merkle_root)
}

#[cfg(test)]
mod tests {
    use super::parse_key_value;

    #[test]
    fn parses_key_value_pairs() {
        let pair = parse_key_value("name=Alice").unwrap();
        assert_eq!(pair.key, "name");
        assert_eq!(pair.value, "Alice");

        assert!(parse_key_value("invalid").is_err());
    }
}
