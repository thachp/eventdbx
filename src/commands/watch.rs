use std::{
    collections::HashSet,
    env, fmt, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use eventdbx::config::{Config, load_or_default};
#[cfg(unix)]
use libc;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(windows)]
use std::os::windows::process::CommandExt;

use crate::commands::domain::{self, PullCommand, PushCommand};

const STATE_FILE_NAME: &str = "watch.json";

#[derive(Args, Debug)]
#[command(subcommand_negates_reqs = true)]
pub struct WatchArgs {
    #[command(subcommand)]
    pub action: Option<WatchSubcommand>,

    #[command(flatten)]
    pub run: WatchRunArgs,
}

#[derive(Subcommand, Debug)]
pub enum WatchSubcommand {
    /// Inspect the persisted watch status
    Status(WatchStatusArgs),
}

#[derive(Args, Debug, Clone)]
pub struct WatchRunArgs {
    #[command(flatten)]
    pub selection: WatchSelectionArgs,

    /// Replication direction to perform each cycle
    #[arg(long = "mode", value_enum, default_value = "bidirectional")]
    pub mode: WatchMode,

    /// Seconds to wait between cycles
    #[arg(
        long = "interval",
        value_name = "SECS",
        default_value_t = 300,
        env = "EVENTDBX_WATCH_INTERVAL"
    )]
    pub interval_secs: u64,

    /// Run a single cycle and exit
    #[arg(long = "run-once", default_value_t = false)]
    pub run_once: bool,

    /// Fork into the background (detach STDIO)
    #[arg(long = "background", default_value_t = false)]
    pub background: bool,

    /// Skip the cycle if a previous watch is still running
    #[arg(long = "skip-if-active", default_value_t = false)]
    pub skip_if_active: bool,
}

#[derive(Args, Debug, Clone)]
pub struct WatchSelectionArgs {
    /// Tenant to replicate (legacy positional domain argument)
    #[arg(value_name = "TENANT", conflicts_with = "tenant")]
    pub domain: Option<String>,

    /// Tenant to replicate (preferred flag; overrides positional argument)
    #[arg(long = "tenant", value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Aggregate type to limit the operation to
    #[arg(long = "aggregate", value_name = "AGGREGATE")]
    pub aggregate: Option<String>,

    /// Aggregate identifier to limit the operation to (requires --aggregate)
    #[arg(long = "id", value_name = "AGGREGATE_ID", requires = "aggregate")]
    pub aggregate_id: Option<String>,

    /// Number of aggregates to process concurrently
    #[arg(
        long = "concurrency",
        value_name = "THREADS",
        value_parser = clap::value_parser!(usize)
    )]
    pub concurrency: Option<usize>,
}

#[derive(Args, Debug)]
pub struct WatchStatusArgs {
    /// Tenant to inspect (omit when using --all)
    #[arg(value_name = "TENANT", conflicts_with = "tenant")]
    pub domain: Option<String>,

    /// Tenant to inspect (preferred flag; overrides positional argument)
    #[arg(long = "tenant", value_name = "TENANT")]
    pub tenant: Option<String>,

    /// Show every watch entry found under the data directory
    #[arg(long, default_value_t = false)]
    pub all: bool,

    /// Emit JSON instead of human-readable text
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum WatchMode {
    Push,
    Pull,
    Bidirectional,
}

impl fmt::Display for WatchMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WatchMode::Push => write!(f, "push"),
            WatchMode::Pull => write!(f, "pull"),
            WatchMode::Bidirectional => write!(f, "bidirectional"),
        }
    }
}

pub fn execute(config_path: Option<PathBuf>, args: WatchArgs) -> Result<()> {
    if let Some(action) = args.action {
        return match action {
            WatchSubcommand::Status(status_args) => watch_status(config_path, status_args),
        };
    }
    run_watch_entry(config_path, args.run)
}

fn run_watch_entry(config_path: Option<PathBuf>, args: WatchRunArgs) -> Result<()> {
    let domain = args
        .selection
        .resolved_tenant()
        .ok_or_else(|| anyhow!("tenant (or domain) must be provided when running watch cycles"))?
        .to_string();
    let (config, _) = load_or_default(config_path.clone())?;
    let interval_secs = args.interval_secs.max(1);
    let state_path = watch_state_path(&config, &domain);

    if args.skip_if_active {
        if let Some(existing) = read_watch_state(&state_path)? {
            if existing.pid.is_some() && existing.stopped_at.is_none() {
                println!(
                    "[watch] skipping domain '{}' because an active watcher is still running (pid {}).",
                    domain,
                    existing.pid.unwrap()
                );
                return Ok(());
            }
        }
    }

    if args.background {
        spawn_background(config_path, &args, interval_secs, &state_path, &domain)
    } else {
        run_watch(config_path, args, interval_secs, state_path, domain)
    }
}

fn run_watch(
    config_path: Option<PathBuf>,
    args: WatchRunArgs,
    interval_secs: u64,
    state_path: PathBuf,
    domain: String,
) -> Result<()> {
    let mut cycle = 0u64;
    let interval = Duration::from_secs(interval_secs);
    let mut state = WatchState::new(&domain, &args, interval_secs)?;
    state.pid = Some(std::process::id());
    state.started_at = Some(Utc::now());
    state.stopped_at = None;
    write_watch_state(&state_path, &state)?;

    loop {
        cycle += 1;
        let started = Instant::now();
        state.last_cycle_started_at = Some(Utc::now());
        state.last_cycle_error = None;
        write_watch_state(&state_path, &state)?;

        println!(
            "[watch] cycle #{cycle} starting for domain '{}' (mode: {})",
            domain, args.mode
        );

        match run_cycle(config_path.clone(), &args.selection, args.mode) {
            Ok(stats) => {
                let elapsed = started.elapsed();
                state.last_cycle_completed_at = Some(Utc::now());
                state.last_cycle_push = stats.push_ran;
                state.last_cycle_pull = stats.pull_ran;
                state.last_cycle_error = None;
                write_watch_state(&state_path, &state)?;
                println!(
                    "[watch] cycle #{cycle} completed in {:?} (push={}, pull={})",
                    elapsed, stats.push_ran, stats.pull_ran
                );
            }
            Err(err) => {
                state.last_cycle_completed_at = Some(Utc::now());
                state.last_cycle_error = Some(format!("{err:#}"));
                write_watch_state(&state_path, &state)?;
                eprintln!("[watch] cycle #{cycle} failed: {:#}", err);
            }
        }

        if args.run_once {
            break;
        }

        println!(
            "[watch] sleeping {} second(s) before next cycle",
            interval_secs
        );
        thread::sleep(interval);
    }

    state.pid = None;
    state.stopped_at = Some(Utc::now());
    write_watch_state(&state_path, &state)?;

    Ok(())
}

fn spawn_background(
    config_path: Option<PathBuf>,
    args: &WatchRunArgs,
    interval_secs: u64,
    state_path: &Path,
    domain: &str,
) -> Result<()> {
    if env::var("DBX_WATCH_BACKGROUND").is_ok() {
        println!(
            "[watch] refusing to background domain '{}' because it is already detached.",
            domain
        );
        return Ok(());
    }

    let exe = env::current_exe().context("failed to resolve current executable")?;
    let mut command = Command::new(exe);
    command.env("DBX_WATCH_BACKGROUND", "1");

    if let Some(config) = config_path.as_ref() {
        command.arg("--config").arg(config);
    }

    command.arg("watch");
    command.arg(domain);
    for arg in args.selection.additional_cli_args() {
        command.arg(arg);
    }
    command.arg("--mode").arg(args.mode.to_string());
    command.arg("--interval").arg(interval_secs.to_string());
    command.arg("--background=false");
    if args.run_once {
        command.arg("--run-once");
    }

    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());

    #[cfg(unix)]
    {
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error().into());
                }
                Ok(())
            });
        }
    }
    #[cfg(windows)]
    {
        const DETACHED_PROCESS: u32 = 0x00000008;
        command.creation_flags(DETACHED_PROCESS);
    }

    let child = command
        .spawn()
        .context("failed to spawn background watch process")?;

    println!(
        "[watch] spawned background process with pid {} (mode {}, interval {}s, state {})",
        child.id(),
        args.mode,
        interval_secs,
        state_path.display()
    );
    Ok(())
}

#[derive(Default)]
struct CycleStats {
    push_ran: bool,
    pull_ran: bool,
}

fn run_cycle(
    config_path: Option<PathBuf>,
    selection: &WatchSelectionArgs,
    mode: WatchMode,
) -> Result<CycleStats> {
    let mut stats = CycleStats::default();
    match mode {
        WatchMode::Push => {
            run_push(config_path, selection)?;
            stats.push_ran = true;
        }
        WatchMode::Pull => {
            run_pull(config_path, selection)?;
            stats.pull_ran = true;
        }
        WatchMode::Bidirectional => {
            run_push(config_path.clone(), selection)?;
            stats.push_ran = true;
            run_pull(config_path, selection)?;
            stats.pull_ran = true;
        }
    }
    Ok(stats)
}

fn run_push(config_path: Option<PathBuf>, selection: &WatchSelectionArgs) -> Result<()> {
    let tenant = selection
        .resolved_tenant()
        .ok_or_else(|| anyhow!("tenant (or domain) must be provided when running watch cycles"))?;
    let mut argv = vec![tenant.to_string()];
    argv.extend(selection.additional_cli_args());
    domain::push(config_path, PushCommand::External(argv))
        .with_context(|| format!("failed to push tenant '{tenant}'"))
}

fn run_pull(config_path: Option<PathBuf>, selection: &WatchSelectionArgs) -> Result<()> {
    let tenant = selection
        .resolved_tenant()
        .ok_or_else(|| anyhow!("tenant (or domain) must be provided when running watch cycles"))?;
    let mut argv = vec![tenant.to_string()];
    argv.extend(selection.additional_cli_args());
    domain::pull(config_path, PullCommand::External(argv))
        .with_context(|| format!("failed to pull tenant '{tenant}'"))
}

impl WatchSelectionArgs {
    fn additional_cli_args(&self) -> Vec<String> {
        let mut argv = Vec::new();
        if let Some(aggregate) = &self.aggregate {
            argv.push("--aggregate".to_string());
            argv.push(aggregate.clone());
        }
        if let Some(aggregate_id) = &self.aggregate_id {
            argv.push("--id".to_string());
            argv.push(aggregate_id.clone());
        }
        if let Some(concurrency) = self.concurrency {
            argv.push("--concurrency".to_string());
            argv.push(concurrency.to_string());
        }
        argv
    }

    fn resolved_tenant(&self) -> Option<&str> {
        self.tenant.as_deref().or_else(|| self.domain.as_deref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WatchState {
    domain: String,
    pid: Option<u32>,
    mode: WatchMode,
    interval_secs: u64,
    aggregate: Option<String>,
    aggregate_id: Option<String>,
    concurrency: Option<usize>,
    run_once: bool,
    background: bool,
    started_at: Option<DateTime<Utc>>,
    stopped_at: Option<DateTime<Utc>>,
    last_cycle_started_at: Option<DateTime<Utc>>,
    last_cycle_completed_at: Option<DateTime<Utc>>,
    last_cycle_error: Option<String>,
    last_cycle_push: bool,
    last_cycle_pull: bool,
}

impl WatchState {
    fn new(domain: &str, args: &WatchRunArgs, interval_secs: u64) -> Result<Self> {
        Ok(Self {
            domain: domain.to_string(),
            pid: None,
            mode: args.mode,
            interval_secs,
            aggregate: args.selection.aggregate.clone(),
            aggregate_id: args.selection.aggregate_id.clone(),
            concurrency: args.selection.concurrency,
            run_once: args.run_once,
            background: args.background,
            started_at: None,
            stopped_at: None,
            last_cycle_started_at: None,
            last_cycle_completed_at: None,
            last_cycle_error: None,
            last_cycle_push: false,
            last_cycle_pull: false,
        })
    }
}

fn watch_state_path(config: &Config, domain: &str) -> PathBuf {
    domain_data_dir_for(config, domain).join(STATE_FILE_NAME)
}

fn write_watch_state(path: &Path, state: &WatchState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let payload =
        serde_json::to_vec_pretty(state).context("failed to serialize watch state payload")?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, payload)
        .with_context(|| format!("failed to write temporary watch state to {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("failed to persist watch state to {}", path.display()))?;
    Ok(())
}

fn read_watch_state(path: &Path) -> Result<Option<WatchState>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read watch state at {}", path.display()))?;
    let state: WatchState = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse watch state at {}", path.display()))?;
    Ok(Some(state))
}

fn watch_status(config_path: Option<PathBuf>, args: WatchStatusArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let mut states = Vec::new();

    if args.all {
        states.extend(collect_all_states(&config)?);
        if states.is_empty() {
            println!(
                "No watch state files found under {}.",
                config.data_dir.display()
            );
            return Ok(());
        }
    } else {
        let domain = args
            .tenant
            .as_deref()
            .or_else(|| args.domain.as_deref())
            .ok_or_else(|| anyhow!("tenant must be provided unless --all is specified"))?;
        let path = watch_state_path(&config, domain);
        let state = read_watch_state(&path)?
            .ok_or_else(|| anyhow!("watch state not found for domain '{}'", domain))?;
        states.push(state);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&states)?);
        return Ok(());
    }

    for state in &states {
        print_state(state);
    }
    Ok(())
}

fn print_state(state: &WatchState) {
    println!("Tenant: {}", state.domain);
    match state.pid {
        Some(pid) => println!("  PID: {pid}"),
        None => println!("  PID: <not running>"),
    }
    println!("  Mode: {}", state.mode);
    println!("  Interval: {}s", state.interval_secs);
    if let Some(aggregate) = &state.aggregate {
        let suffix = state
            .aggregate_id
            .as_deref()
            .map(|id| format!("::{id}"))
            .unwrap_or_default();
        println!("  Aggregate filter: {aggregate}{suffix}");
    }
    if let Some(concurrency) = state.concurrency {
        println!("  Concurrency: {concurrency}");
    }
    println!(
        "  Started at: {}",
        state
            .started_at
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "<unknown>".to_string())
    );
    if let Some(stopped) = state.stopped_at {
        println!("  Stopped at: {}", stopped.to_rfc3339());
    }
    if let Some(started) = state.last_cycle_started_at {
        println!("  Last cycle started: {}", started.to_rfc3339());
    }
    if let Some(completed) = state.last_cycle_completed_at {
        println!("  Last cycle completed: {}", completed.to_rfc3339());
    }
    if let Some(err) = &state.last_cycle_error {
        println!("  Last error: {err}");
    } else {
        println!(
            "  Last result: push={} pull={}",
            state.last_cycle_push, state.last_cycle_pull
        );
    }
    println!();
}

fn collect_all_states(config: &Config) -> Result<Vec<WatchState>> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    let shards_root = config.shards_root();
    if shards_root.is_dir() {
        for shard in fs::read_dir(&shards_root).context("failed to read shard directory")? {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            let tenants_dir = shard.path().join("tenants");
            if !tenants_dir.is_dir() {
                continue;
            }
            for tenant in fs::read_dir(&tenants_dir).with_context(|| {
                format!("failed to read tenants under {}", tenants_dir.display())
            })? {
                let tenant = tenant?;
                if !tenant.file_type()?.is_dir() {
                    continue;
                }
                let path = tenant.path().join(STATE_FILE_NAME);
                if !seen.insert(path.clone()) {
                    continue;
                }
                if let Some(state) = read_watch_state(&path)? {
                    entries.push(state);
                }
            }
        }
    }

    Ok(entries)
}

fn domain_data_dir_for(config: &Config, domain: &str) -> PathBuf {
    config.domain_data_dir_for(domain)
}
