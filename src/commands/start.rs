use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};

use eventdbx::{
    config::{ApiMode, Config, ConfigUpdate, load_or_default},
    plugin::PluginManager,
    restrict::{self, RESTRICT_ENV},
    server,
};

#[derive(Args, Clone)]
pub struct StartArgs {
    /// Override the configured server port
    #[arg(long)]
    pub port: Option<u16>,

    /// Override the configured data directory
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Run the server in the foreground instead of daemonizing
    #[arg(long)]
    pub foreground: bool,

    /// Require schema enforcement (use `--restrict=false` to disable)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub restrict: bool,
    /// Select which API surfaces to expose
    #[arg(long = "api", value_enum)]
    pub api: Option<ApiModeArg>,
}

impl Default for StartArgs {
    fn default() -> Self {
        Self {
            port: None,
            data_dir: None,
            foreground: false,
            restrict: restrict::from_env(),
            api: None,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ApiModeArg {
    Rest,
    Graphql,
    Both,
}

impl From<ApiModeArg> for ApiMode {
    fn from(value: ApiModeArg) -> Self {
        match value {
            ApiModeArg::Rest => ApiMode::Rest,
            ApiModeArg::Graphql => ApiMode::Graphql,
            ApiModeArg::Both => ApiMode::Both,
        }
    }
}

#[derive(Args)]
pub struct DestroyArgs {
    /// Skip confirmation prompt
    #[arg(long)]
    pub yes: bool,
}

pub async fn execute(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    restrict::set_env(args.restrict);
    if args.foreground {
        start_foreground(config_path, args).await
    } else {
        start_daemon(config_path, args)?;
        Ok(())
    }
}

pub async fn run_internal(config_path: Option<PathBuf>) -> Result<()> {
    let args = StartArgs::default();
    restrict::set_env(args.restrict);
    start_foreground(config_path, args).await
}

pub fn stop(config_path: Option<PathBuf>) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let pid_path = config.pid_file_path();

    let Some(record) = read_pid_record(&pid_path)? else {
        println!("No running EventDBX server found.");
        return Ok(());
    };
    let pid = record.pid;

    if !process_is_running(pid) {
        fs::remove_file(&pid_path)?;
        println!("Removed stale EventDBX server pid file.");
        return Ok(());
    }

    terminate_process(pid)?;
    if !wait_for_exit(pid, Duration::from_secs(5)) {
        #[cfg(unix)]
        {
            force_kill_process(pid)?;
            if !wait_for_exit(pid, Duration::from_secs(2)) {
                return Err(anyhow!(
                    "failed to stop EventDBX server (pid {pid}); process is still running"
                ));
            }
        }
        #[cfg(not(unix))]
        {
            return Err(anyhow!(
                "failed to stop EventDBX server (pid {pid}); process is still running"
            ));
        }
    }

    fs::remove_file(&pid_path)?;
    if let Some(started_at) = record.started_at {
        println!(
            "EventDBX server stopped (pid {}) after {} (started {})",
            pid,
            describe_uptime(started_at),
            started_at.to_rfc3339()
        );
    } else {
        println!("EventDBX server stopped (pid {})", pid);
    }
    Ok(())
}

pub fn status(config_path: Option<PathBuf>) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let pid_path = config.pid_file_path();

    match read_pid_record(&pid_path)? {
        Some(record) => {
            let pid = record.pid;
            if process_is_running(pid) {
                if let Some(started_at) = record.started_at {
                    println!(
                        "EventDBX server is running on port {} (pid {}) — restrict={} — up for {} (since {})",
                        config.port,
                        pid,
                        config.restrict,
                        describe_uptime(started_at),
                        started_at.to_rfc3339()
                    );
                } else {
                    println!(
                        "EventDBX server is running on port {} (pid {}) — restrict={}",
                        config.port, pid, config.restrict
                    );
                }
            } else {
                let _ = fs::remove_file(&pid_path);
                println!("EventDBX server is not running (removed stale pid file).");
            }
        }
        None => println!("EventDBX server is not running."),
    }

    Ok(())
}

pub fn destroy(config_path: Option<PathBuf>, args: DestroyArgs) -> Result<()> {
    let (config, path) = load_or_default(config_path)?;

    if !args.yes {
        eprint!(
            "This will permanently delete all EventDBX data under {} and remove the config file at {}.\nType \"destroy\" to continue: ",
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

    if let Err(err) = stop(Some(path.clone())) {
        tracing::warn!("failed to stop running server before destroy: {err}");
    }

    if config.data_dir.exists() {
        fs::remove_dir_all(&config.data_dir)?;
    }

    if path.exists() {
        fs::remove_file(&path)?;
    }

    println!(
        "All EventDBX data and configuration removed from {}",
        config.data_dir.display()
    );
    Ok(())
}

async fn start_foreground(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    let (config, _) = load_and_update_config(config_path, &args)?;
    eprintln!(
        "configuration loaded; starting server (pid={})",
        std::process::id()
    );
    let plugins = PluginManager::from_config(&config)?;
    server::run(config, plugins).await?;
    Ok(())
}

fn start_daemon(config_path: Option<PathBuf>, args: StartArgs) -> Result<()> {
    let (config, path) = load_and_update_config(config_path, &args)?;
    let pid_path = config.pid_file_path();

    if let Some(existing) = read_pid_record(&pid_path)? {
        if process_is_running(existing.pid) {
            return Err(anyhow!(
                "EventDBX server already running (pid {})",
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
    command.env(RESTRICT_ENV, restrict::as_str(args.restrict));

    let mut child = command.spawn()?;
    let pid = child.id();

    let wait_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait()? {
            let message = if let Some(code) = status.code() {
                format!(
                    "EventDBX server failed to start (process exited with status {code}). \
                     Re-run with `eventdbx start --foreground` for details."
                )
            } else {
                "EventDBX server failed to start (process terminated unexpectedly). \
                 Re-run with `eventdbx start --foreground` for details."
                    .to_string()
            };
            return Err(anyhow!(message));
        }

        if Instant::now() >= wait_deadline {
            break;
        }

        thread::sleep(Duration::from_millis(100));
    }

    let started_at = chrono::Utc::now();
    let record = PidRecord {
        pid,
        started_at: Some(started_at),
    };
    write_pid_record(&pid_path, &record)?;

    drop(child);

    println!(
        "EventDBX server is running on port {} (pid {}) since {} (restrict={})",
        config.port,
        pid,
        started_at.to_rfc3339(),
        args.restrict
    );
    Ok(())
}

fn load_and_update_config(
    config_path: Option<PathBuf>,
    args: &StartArgs,
) -> Result<(Config, PathBuf)> {
    let (mut config, path) = load_or_default(config_path)?;
    apply_start_overrides(&mut config, args);
    config.ensure_data_dir()?;
    config.save(&path)?;
    Ok((config, path))
}

fn apply_start_overrides(config: &mut Config, args: &StartArgs) {
    config.apply_update(ConfigUpdate {
        port: args.port,
        data_dir: args.data_dir.clone(),
        memory_threshold: None,
        data_encryption_key: None,
        restrict: Some(args.restrict),
        list_page_size: None,
        page_limit: None,
        plugin_max_attempts: None,
        api_mode: args.api.map(Into::into),
        hidden_aggregate_types: None,
        hidden_fields: None,
    });
}

#[derive(Debug, Serialize, Deserialize)]
struct PidRecord {
    pid: u32,
    #[serde(default)]
    started_at: Option<chrono::DateTime<chrono::Utc>>,
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

fn describe_uptime(started_at: chrono::DateTime<chrono::Utc>) -> String {
    let now = chrono::Utc::now();
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
