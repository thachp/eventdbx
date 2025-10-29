use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    path::PathBuf,
    process::Command,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use clap::{Args, Subcommand};

use eventdbx::{
    config::{Config, RemoteConfig, load_or_default},
    error::EventError,
    replication_capnp_client::{CapnpReplicationClient, decode_schemas, normalize_capnp_endpoint},
    schema::{AggregateSchema, SchemaManager},
    store::{AggregatePositionEntry, EventStore},
};
use serde::Deserialize;
use serde_json;

use crate::commands::{cli_token, domain::normalize_domain_name};

#[derive(Subcommand)]
pub enum RemoteCommands {
    /// Add a replication remote
    Add(RemoteAddArgs),
    /// Remove a replication remote
    #[command(name = "rm")]
    Remove(RemoteRemoveArgs),
    /// List configured remotes
    #[command(name = "ls")]
    List(RemoteListArgs),
    /// Show details for a remote
    Show(RemoteShowArgs),
}

#[derive(Args)]
pub struct RemoteAddArgs {
    /// Remote alias (e.g. standby1)
    pub name: String,
    /// Remote replication target (IP or locator such as dbx@host:owner/domain.dbx)
    #[arg(value_name = "TARGET")]
    pub target: String,
    /// Access token granting replication privileges on the remote (required for direct targets)
    #[arg(long = "token")]
    pub token: Option<String>,
    /// Remote replication port
    #[arg(long, default_value_t = 6363)]
    pub port: u16,
    /// Replace existing remote with the same name
    #[arg(long, default_value_t = false)]
    pub replace: bool,
}

#[derive(Args)]
pub struct RemoteRemoveArgs {
    /// Remote alias to remove
    pub name: String,
}

#[derive(Args)]
pub struct RemoteShowArgs {
    /// Remote alias to display
    pub name: String,
}

#[derive(Args)]
pub struct RemotePushArgs {
    /// Remote alias to push to
    pub name: String,
    /// Domain to push from (defaults to the active domain)
    #[arg(value_name = "DOMAIN")]
    pub domain: Option<String>,
    /// Perform a dry run without sending data
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    /// Number of events per batch when pushing
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,
    /// Limit the push to specific aggregate types
    #[arg(short = 'a', long = "aggregate", value_name = "TYPE")]
    pub aggregates: Vec<String>,
    /// Limit the push to specific aggregate identifiers (TYPE:ID)
    #[arg(short = 'i', long = "aggregate-id", value_name = "TYPE:ID")]
    pub aggregate_ids: Vec<String>,
    /// Synchronize schemas on the remote before pushing events
    #[arg(long, default_value_t = false)]
    pub schema: bool,
    /// Synchronize only schemas, skipping event data
    #[arg(long = "schema-only", default_value_t = false)]
    pub schema_only: bool,
}

#[derive(Args)]
pub struct RemotePullArgs {
    /// Remote alias to pull from
    pub name: String,
    /// Domain to pull into (defaults to the active domain)
    #[arg(value_name = "DOMAIN")]
    pub domain: Option<String>,
    /// Perform a dry run without writing data
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    /// Maximum events to request per gRPC call
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,
    /// Limit the pull to specific aggregate types
    #[arg(short = 'a', long = "aggregate", value_name = "TYPE")]
    pub aggregates: Vec<String>,
    /// Limit the pull to specific aggregate identifiers (TYPE:ID)
    #[arg(short = 'i', long = "aggregate-id", value_name = "TYPE:ID")]
    pub aggregate_ids: Vec<String>,
    /// Synchronize schemas from the remote
    #[arg(long, default_value_t = false)]
    pub schema: bool,
    /// Synchronize only schemas, skipping event data
    #[arg(long = "schema-only", default_value_t = false)]
    pub schema_only: bool,
}

pub async fn execute(config_path: Option<PathBuf>, command: RemoteCommands) -> Result<()> {
    match command {
        RemoteCommands::Add(args) => add_remote(config_path, args),
        RemoteCommands::Remove(args) => remove_remote(config_path, args),
        RemoteCommands::List(args) => list_remotes(config_path, args.json),
        RemoteCommands::Show(args) => show_remote(config_path, args),
    }
}

fn add_remote(config_path: Option<PathBuf>, args: RemoteAddArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    let name = args.name.trim();
    if name.is_empty() {
        bail!("remote name cannot be empty");
    }

    if config.remotes.contains_key(name) && !args.replace {
        bail!(
            "remote '{}' already exists (use --replace to overwrite)",
            name
        );
    }

    let target = args.target.trim();
    if target.is_empty() {
        bail!("remote target cannot be empty");
    }

    let (endpoint, token, locator, remote_domain, summary_target) =
        if let Some(locator) = RemoteLocator::parse(target) {
            if args.token.is_some() {
                bail!("--token cannot be supplied when using a locator target");
            }
            let resolved = resolve_remote_locator(&locator)?;
            let normalized_token = normalize_token(&resolved.token)?;
            let endpoint = resolved.endpoint.trim().to_string();
            if endpoint.is_empty() {
                bail!(
                    "remote locator '{}' returned an empty endpoint",
                    locator.display()
                );
            }
            (
                endpoint,
                normalized_token,
                Some(locator.to_string()),
                resolved.remote_domain.clone(),
                locator.to_string(),
            )
        } else {
            if target.contains("://") {
                bail!("direct remote targets must not include a protocol scheme");
            }
            if args.port == 0 {
                bail!("remote port must be greater than zero");
            }
            let token_input = args
                .token
                .as_deref()
                .ok_or_else(|| anyhow!("--token is required when adding a direct remote"))?;
            let normalized_token = normalize_token(token_input)?;
            let endpoint = format!("tcp://{}:{}", target, args.port);
            (endpoint, normalized_token, None, None, target.to_string())
        };

    config.remotes.insert(
        name.to_string(),
        RemoteConfig {
            endpoint: endpoint.clone(),
            token: token.clone(),
            locator,
            remote_domain: remote_domain.clone(),
        },
    );
    config.updated_at = Utc::now();
    config.save(&path)?;

    let token_summary = summarize_token(&token);
    match remote_domain {
        Some(domain) => println!(
            "Remote '{}' resolved '{}' to endpoint {} (domain {}) with token {}",
            name, summary_target, endpoint, domain, token_summary
        ),
        None => println!(
            "Remote '{}' resolved '{}' to endpoint {} with token {}",
            name, summary_target, endpoint, token_summary
        ),
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct RemoteLocator {
    original: String,
    user: Option<String>,
    host: String,
    path: String,
}

impl RemoteLocator {
    fn parse(input: &str) -> Option<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty()
            || trimmed.contains("://")
            || !trimmed.contains(':')
            || !trimmed.contains('/')
        {
            return None;
        }

        let (user_host, path) = trimmed.split_once(':')?;
        if path.trim().is_empty() {
            return None;
        }

        let (user, host) = if let Some((user, host)) = user_host.split_once('@') {
            let host = host.trim();
            if host.is_empty() {
                return None;
            }
            let user = user.trim();
            let user = if user.is_empty() {
                None
            } else {
                Some(user.to_string())
            };
            (user, host.to_string())
        } else {
            let host = user_host.trim();
            if host.is_empty() {
                return None;
            }
            (None, host.to_string())
        };

        Some(Self {
            original: trimmed.to_string(),
            user,
            host,
            path: path.trim().to_string(),
        })
    }

    fn destination(&self) -> String {
        match &self.user {
            Some(user) => format!("{}@{}", user, self.host),
            None => self.host.clone(),
        }
    }

    fn display(&self) -> &str {
        &self.original
    }
}

impl fmt::Display for RemoteLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

#[derive(Debug)]
struct RemoteLocatorResolution {
    endpoint: String,
    token: String,
    remote_domain: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RemoteLocatorAdvertisement {
    endpoint: String,
    token: String,
    #[serde(default)]
    remote_domain: Option<String>,
}

fn resolve_remote_locator(locator: &RemoteLocator) -> Result<RemoteLocatorResolution> {
    let destination = locator.destination();
    let output = Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg(&destination)
        .arg("dbx")
        .arg("remote")
        .arg("advertise")
        .arg("--format")
        .arg("json")
        .arg(&locator.path)
        .output()
        .with_context(|| format!("failed to invoke ssh to {}", destination))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let status = output
            .status
            .code()
            .map(|code| code.to_string())
            .unwrap_or_else(|| "signal".to_string());
        bail!(
            "locator '{}' rejected advertise request (ssh exit {}): {}",
            locator.display(),
            status,
            stderr.trim()
        );
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| {
        anyhow!(
            "locator '{}' returned invalid UTF-8 payload: {}",
            locator.display(),
            err
        )
    })?;
    let payload = stdout.trim();
    if payload.is_empty() {
        bail!(
            "locator '{}' returned an empty advertise payload",
            locator.display()
        );
    }

    let advertisement: RemoteLocatorAdvertisement =
        serde_json::from_str(payload).with_context(|| {
            format!(
                "locator '{}' returned invalid JSON advertise payload",
                locator.display()
            )
        })?;

    let endpoint = advertisement.endpoint.trim().to_string();
    if endpoint.is_empty() {
        bail!(
            "locator '{}' advertise payload omitted endpoint",
            locator.display()
        );
    }
    let token = advertisement.token.trim().to_string();
    if token.is_empty() {
        bail!(
            "locator '{}' advertise payload omitted token",
            locator.display()
        );
    }

    let remote_domain = advertisement
        .remote_domain
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    Ok(RemoteLocatorResolution {
        endpoint,
        token,
        remote_domain,
    })
}

fn remove_remote(config_path: Option<PathBuf>, args: RemoteRemoveArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
    let name = args.name.trim();
    if name.is_empty() {
        bail!("remote name cannot be empty");
    }

    if config.remotes.remove(name).is_some() {
        config.updated_at = Utc::now();
        config.save(&path)?;
        println!("Removed remote '{}'", name);
    } else {
        bail!("no remote named '{}' is configured", name);
    }
    Ok(())
}

fn list_remotes(config_path: Option<PathBuf>, json: bool) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    if config.remotes.is_empty() {
        if json {
            let empty: BTreeMap<String, RemoteConfig> = BTreeMap::new();
            println!("{}", serde_json::to_string_pretty(&empty)?);
        } else {
            println!("(no remotes configured)");
        }
        return Ok(());
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&config.remotes)?);
    } else {
        for (name, remote) in &config.remotes {
            let mut line = format!("{} {}", name, remote.endpoint);
            if let Some(locator) = &remote.locator {
                line.push_str(&format!(" [{}]", locator));
            }
            if let Some(domain) = &remote.remote_domain {
                line.push_str(&format!(" (domain {})", domain));
            }
            println!("{}", line);
        }
    }
    Ok(())
}

fn show_remote(config_path: Option<PathBuf>, args: RemoteShowArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let name = args.name.trim();
    if name.is_empty() {
        bail!("remote name cannot be empty");
    }

    let remote = config
        .remotes
        .get(name)
        .ok_or_else(|| anyhow!("no remote named '{}' is configured", name))?;

    println!("name: {}", name);
    println!("endpoint: {}", remote.endpoint);
    println!("token: {}", summarize_token(&remote.token));
    if let Some(locator) = &remote.locator {
        println!("locator: {}", locator);
    }
    if let Some(domain) = &remote.remote_domain {
        println!("remote_domain: {}", domain);
    }
    Ok(())
}

pub async fn push(config_path: Option<PathBuf>, mut args: RemotePushArgs) -> Result<()> {
    if args.schema_only {
        args.schema = true;
    }

    if args.schema_only && (!args.aggregates.is_empty() || !args.aggregate_ids.is_empty()) {
        bail!("--schema-only cannot be combined with aggregate filters");
    }

    let (mut config, _) = load_or_default(config_path)?;
    if let Some(domain) = args.domain.take() {
        let normalized = normalize_domain_name(&domain)?;
        config.domain = normalized;
    }
    config.ensure_data_dir()?;
    let remote = config
        .remotes
        .get(&args.name)
        .cloned()
        .ok_or_else(|| anyhow!("no remote named '{}' is configured", args.name))?;

    let remote_name = args.name.clone();

    if args.schema {
        let manager = Arc::new(SchemaManager::load(config.schema_store_path())?);
        push_remote_schemas(manager, remote.clone(), &remote_name, args.dry_run).await?;
        if args.schema_only {
            return Ok(());
        }
    }

    let store = Arc::new(EventStore::open_read_only(
        config.event_store_path(),
        config.encryption_key()?,
    )?);
    let local_positions = store.aggregate_positions()?;

    let filter = normalize_filter(&args.aggregates, &args.aggregate_ids)?;

    push_remote_impl(
        store,
        local_positions,
        remote,
        remote_name,
        args.dry_run,
        args.batch_size.max(1),
        filter,
    )
    .await
}

pub async fn pull(config_path: Option<PathBuf>, mut args: RemotePullArgs) -> Result<()> {
    if args.schema_only {
        args.schema = true;
    }

    if args.schema_only && (!args.aggregates.is_empty() || !args.aggregate_ids.is_empty()) {
        bail!("--schema-only cannot be combined with aggregate filters");
    }

    let (mut config, _) = load_or_default(config_path)?;
    let original_domain = config.active_domain().to_string();
    let domain_overridden = if let Some(domain) = args.domain.take() {
        let normalized = normalize_domain_name(&domain)?;
        let overridden = normalized != original_domain;
        config.domain = normalized;
        overridden
    } else {
        false
    };
    config.ensure_data_dir()?;
    let remote = config
        .remotes
        .get(&args.name)
        .cloned()
        .ok_or_else(|| anyhow!("no remote named '{}' is configured", args.name))?;

    let remote_name = args.name.clone();

    let mut proxied_via_server = false;

    if !args.schema_only {
        let filter = normalize_filter(&args.aggregates, &args.aggregate_ids)?;
        let batch_size = args.batch_size.max(1);
        if args.dry_run {
            let store = Arc::new(EventStore::open_read_only(
                config.event_store_path(),
                config.encryption_key()?,
            )?);
            let local_positions = store.aggregate_positions()?;
            pull_remote_impl(
                store,
                local_positions,
                remote.clone(),
                remote_name.clone(),
                true,
                batch_size,
                filter,
            )
            .await?;
        } else {
            match EventStore::open(
                config.event_store_path(),
                config.encryption_key()?,
                config.snowflake_worker_id,
            ) {
                Ok(store) => {
                    let store = Arc::new(store);
                    let local_positions = store.aggregate_positions()?;
                    pull_remote_impl(
                        store,
                        local_positions,
                        remote.clone(),
                        remote_name.clone(),
                        false,
                        batch_size,
                        filter.clone(),
                    )
                    .await?;
                }
                Err(EventError::Storage(message)) if is_lock_error(&message) => {
                    if domain_overridden {
                        bail!(
                            "cannot proxy pull via running server when operating on domain '{}'; stop the daemon for that domain and retry",
                            config.active_domain()
                        );
                    }
                    pull_remote_via_daemon(
                        &config,
                        remote.clone(),
                        remote_name.clone(),
                        false,
                        batch_size,
                        filter,
                    )
                    .await?;
                    proxied_via_server = true;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    if args.schema {
        if proxied_via_server {
            pull_remote_schemas_via_daemon(&config, remote, &remote_name, args.dry_run).await?;
        } else {
            let manager = Arc::new(SchemaManager::load(config.schema_store_path())?);
            pull_remote_schemas(manager, remote, &remote_name, args.dry_run).await?;
        }
    }

    Ok(())
}

fn normalize_token(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("token cannot be empty");
    }
    if trimmed.split('.').count() != 3 {
        bail!(
            "token must be a JWT with three segments (received '{}')",
            trimmed
        );
    }
    Ok(trimmed.to_string())
}

fn summarize_token(token: &str) -> String {
    if token.len() <= 8 {
        return token.to_string();
    }
    let prefix_len = 4.min(token.len());
    let suffix_len = 4.min(token.len().saturating_sub(prefix_len));
    let prefix = &token[..prefix_len];
    let suffix = &token[token.len() - suffix_len..];
    format!("{}…{}", prefix, suffix)
}

fn normalize_filter(
    aggregates: &[String],
    aggregate_ids: &[String],
) -> Result<Option<ReplicationFilter>> {
    let mut aggregate_types: HashSet<String> = aggregates
        .iter()
        .map(|agg| agg.trim())
        .filter(|agg| !agg.is_empty())
        .map(|agg| agg.to_string())
        .collect();

    let mut id_map: HashMap<String, HashSet<String>> = HashMap::new();
    for spec in aggregate_ids {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (aggregate_type, aggregate_id) = if let Some((ty, id)) = trimmed.split_once("::") {
            (ty, id)
        } else if let Some((ty, id)) = trimmed.split_once(':') {
            (ty, id)
        } else {
            bail!(
                "aggregate id specification '{}' must be in TYPE:ID or TYPE::ID format",
                spec
            );
        };

        let ty = aggregate_type.trim();
        let id = aggregate_id.trim();
        if ty.is_empty() || id.is_empty() {
            bail!("aggregate id specification '{}' has empty type or id", spec);
        }

        aggregate_types.insert(ty.to_string());
        id_map
            .entry(ty.to_string())
            .or_default()
            .insert(id.to_string());
    }

    if aggregate_types.is_empty() && id_map.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ReplicationFilter {
            aggregate_types,
            aggregate_ids: id_map,
        }))
    }
}

async fn push_remote_impl(
    store: Arc<EventStore>,
    local_positions: Vec<AggregatePositionEntry>,
    remote: RemoteConfig,
    remote_name: String,
    dry_run: bool,
    batch_size: usize,
    filter: Option<ReplicationFilter>,
) -> Result<()> {
    let mut client = connect_client(&remote).await?;
    let remote_positions = client
        .list_positions()
        .await
        .map_err(|err| anyhow!("remote {} list_positions failed: {}", remote_name, err))?;

    let filter_ref = filter.as_ref();
    let remote_map = positions_to_map(&remote_positions, filter_ref);
    let local_map = positions_to_map(&local_positions, filter_ref);

    validate_fast_forward(&remote_name, &local_map, &remote_map)?;

    let plans = build_push_plans(&local_map, &remote_map);
    if plans.is_empty() {
        println!("remote '{}' is up to date", remote_name);
        return Ok(());
    }

    let stats = summarize_plans(&plans);
    if dry_run {
        print_summary(
            &format!("Push to remote '{}' (dry run)", remote_name),
            &stats,
        );
        return Ok(());
    }

    let mut sequence = 0u64;
    let mut total_sent = 0u64;

    for plan in &plans {
        let mut current = plan.from_version;
        while current < plan.to_version {
            let events = store.events_after(
                &plan.aggregate_type,
                &plan.aggregate_id,
                current,
                Some(batch_size),
            )?;
            if events.is_empty() {
                break;
            }

            sequence = sequence.wrapping_add(1);
            client
                .apply_events(sequence, &events)
                .await
                .map_err(|err| {
                    anyhow!("failed to apply events on remote {}: {}", remote_name, err)
                })?;

            total_sent += events.len() as u64;
            current = events.last().map(|event| event.version).unwrap_or(current);
        }
    }

    print_summary(
        &format!("Pushed {} event(s) to '{}'", total_sent, remote_name),
        &stats,
    );
    Ok(())
}

async fn pull_remote_impl(
    store: Arc<EventStore>,
    local_positions: Vec<AggregatePositionEntry>,
    remote: RemoteConfig,
    remote_name: String,
    dry_run: bool,
    batch_size: usize,
    filter: Option<ReplicationFilter>,
) -> Result<()> {
    let mut client = connect_client(&remote).await?;
    let remote_positions: Vec<AggregatePositionEntry> = client
        .list_positions()
        .await
        .map_err(|err| anyhow!("remote {} list_positions failed: {}", remote_name, err))?;

    let filter_ref = filter.as_ref();
    let remote_map = positions_to_map(&remote_positions, filter_ref);
    let local_map = positions_to_map(&local_positions, filter_ref);

    let plans = build_pull_plans(&local_map, &remote_map);
    if plans.is_empty() {
        println!(
            "local store already includes remote '{}' changes",
            remote_name
        );
        return Ok(());
    }

    let stats = summarize_plans(&plans);
    if dry_run {
        print_summary(
            &format!("Pull from remote '{}' (dry run)", remote_name),
            &stats,
        );
        return Ok(());
    }

    let mut total_applied = 0u64;
    for plan in &plans {
        let mut current = plan.from_version;
        while current < plan.to_version {
            let events = client
                .pull_events(
                    &plan.aggregate_type,
                    &plan.aggregate_id,
                    current,
                    Some(batch_size),
                )
                .await
                .map_err(|err| {
                    anyhow!("failed to pull events from remote {}: {}", remote_name, err)
                })?;

            if events.is_empty() {
                break;
            }

            for record in events {
                current = record.version;
                store.append_replica(record)?;
                total_applied += 1;
            }
        }
    }

    print_summary(
        &format!("Pulled {} event(s) from '{}'", total_applied, remote_name),
        &stats,
    );
    Ok(())
}

async fn pull_remote_via_daemon(
    config: &Config,
    remote: RemoteConfig,
    remote_name: String,
    dry_run: bool,
    batch_size: usize,
    filter: Option<ReplicationFilter>,
) -> Result<()> {
    let mut remote_client = connect_client(&remote).await?;
    let remote_positions = remote_client
        .list_positions()
        .await
        .map_err(|err| anyhow!("remote {} list_positions failed: {}", remote_name, err))?;

    let mut local_client = connect_local_replication_client(config).await?;
    let local_positions = local_client
        .list_positions()
        .await
        .map_err(|err| anyhow!("local list_positions failed: {}", err))?;

    let filter_ref = filter.as_ref();
    let remote_map = positions_to_map(&remote_positions, filter_ref);
    let local_map = positions_to_map(&local_positions, filter_ref);

    let plans = build_pull_plans(&local_map, &remote_map);
    if plans.is_empty() {
        println!(
            "local store already includes remote '{}' changes",
            remote_name
        );
        return Ok(());
    }

    let stats = summarize_plans(&plans);
    if dry_run {
        print_summary(
            &format!("Pull from remote '{}' (dry run)", remote_name),
            &stats,
        );
        return Ok(());
    }

    let mut sequence = 0u64;
    let mut total_applied = 0u64;
    for plan in &plans {
        let mut current = plan.from_version;
        while current < plan.to_version {
            let events = remote_client
                .pull_events(
                    &plan.aggregate_type,
                    &plan.aggregate_id,
                    current,
                    Some(batch_size),
                )
                .await
                .map_err(|err| {
                    anyhow!("failed to pull events from remote {}: {}", remote_name, err)
                })?;

            if events.is_empty() {
                break;
            }

            sequence = sequence.wrapping_add(1);
            local_client
                .apply_events(sequence, &events)
                .await
                .map_err(|err| anyhow!("failed to apply events via local server: {}", err))?;

            total_applied += events.len() as u64;
            current = events.last().map(|event| event.version).unwrap_or(current);
        }
    }

    print_summary(
        &format!("Pulled {} event(s) from '{}'", total_applied, remote_name),
        &stats,
    );
    Ok(())
}

async fn pull_remote_schemas(
    manager: Arc<SchemaManager>,
    remote: RemoteConfig,
    remote_name: &str,
    dry_run: bool,
) -> Result<()> {
    let mut client = connect_client(&remote).await?;
    let payload = client
        .pull_schemas()
        .await
        .map_err(|err| anyhow!("remote {} pull_schemas failed: {}", remote_name, err))?;

    let remote_map: BTreeMap<String, AggregateSchema> =
        decode_schemas(&payload).map_err(|err| {
            anyhow!(
                "remote {} returned invalid schema payload: {}",
                remote_name,
                err
            )
        })?;

    let local_map = manager.snapshot();
    let changes = diff_schemas(&local_map, &remote_map);
    if changes.is_empty() {
        println!("Schema store already matches remote '{}'", remote_name);
        return Ok(());
    }

    if dry_run {
        print_schema_changes(
            &format!("Schema pull from '{}' (dry run)", remote_name),
            &changes,
        );
        return Ok(());
    }

    manager.replace_all(remote_map)?;
    print_schema_changes(
        &format!("Pulled schema changes from '{}'", remote_name),
        &changes,
    );
    Ok(())
}

async fn pull_remote_schemas_via_daemon(
    config: &Config,
    remote: RemoteConfig,
    remote_name: &str,
    dry_run: bool,
) -> Result<()> {
    let mut remote_client = connect_client(&remote).await?;
    let payload = remote_client
        .pull_schemas()
        .await
        .map_err(|err| anyhow!("remote {} pull_schemas failed: {}", remote_name, err))?;
    let remote_map: BTreeMap<String, AggregateSchema> =
        decode_schemas(&payload).map_err(|err| {
            anyhow!(
                "remote {} returned invalid schema payload: {}",
                remote_name,
                err
            )
        })?;

    let mut local_client = connect_local_replication_client(config).await?;
    let local_payload = local_client
        .pull_schemas()
        .await
        .map_err(|err| anyhow!("failed to snapshot local schemas via server: {}", err))?;
    let local_map = decode_schemas(&local_payload)
        .map_err(|err| anyhow!("failed to decode local schema snapshot: {}", err))?;

    let changes = diff_schemas(&local_map, &remote_map);
    if changes.is_empty() {
        println!("Schema store already matches remote '{}'", remote_name);
        return Ok(());
    }

    if dry_run {
        print_schema_changes(
            &format!("Schema pull from '{}' (dry run)", remote_name),
            &changes,
        );
        return Ok(());
    }

    let payload = serde_json::to_vec(&remote_map)?;
    local_client
        .apply_schemas(&payload)
        .await
        .map_err(|err| anyhow!("failed to apply schemas via local server: {}", err))?;
    print_schema_changes(
        &format!("Pulled schema changes from '{}'", remote_name),
        &changes,
    );
    Ok(())
}

async fn push_remote_schemas(
    manager: Arc<SchemaManager>,
    remote: RemoteConfig,
    remote_name: &str,
    dry_run: bool,
) -> Result<()> {
    let mut client = connect_client(&remote).await?;
    let payload = client
        .pull_schemas()
        .await
        .map_err(|err| anyhow!("remote {} pull_schemas failed: {}", remote_name, err))?;

    let remote_map: BTreeMap<String, AggregateSchema> =
        decode_schemas(&payload).map_err(|err| {
            anyhow!(
                "remote {} returned invalid schema payload: {}",
                remote_name,
                err
            )
        })?;

    let local_map = manager.snapshot();
    let changes = diff_schemas(&remote_map, &local_map);
    if changes.is_empty() {
        println!(
            "Remote '{}' schema store already matches local",
            remote_name
        );
        return Ok(());
    }

    if dry_run {
        print_schema_changes(
            &format!("Schema push to '{}' (dry run)", remote_name),
            &changes,
        );
        return Ok(());
    }

    let payload = serde_json::to_vec(&local_map)?;
    client
        .apply_schemas(&payload)
        .await
        .map_err(|err| anyhow!("remote {} apply_schemas failed: {}", remote_name, err))?;

    print_schema_changes(
        &format!("Pushed schema changes to '{}'", remote_name),
        &changes,
    );
    Ok(())
}

async fn connect_local_replication_client(config: &Config) -> Result<CapnpReplicationClient> {
    let endpoint = normalize_capnp_endpoint(&config.socket.bind_addr)?;
    let token = cli_token::ensure_bootstrap_token(config)?;
    CapnpReplicationClient::connect(&endpoint, &token)
        .await
        .map_err(|err| anyhow!("failed to connect to local replication endpoint: {}", err))
}

async fn connect_client(remote: &RemoteConfig) -> Result<CapnpReplicationClient> {
    let endpoint = normalize_capnp_endpoint(&remote.endpoint)?;
    if remote.token.trim().is_empty() {
        bail!(
            "remote '{}' does not have a replication token configured",
            remote.endpoint
        );
    }

    CapnpReplicationClient::connect(&endpoint, &remote.token)
        .await
        .map_err(|err| anyhow!("failed to connect to remote {}: {}", remote.endpoint, err))
}

#[derive(Debug, Clone)]
struct ReplicationFilter {
    aggregate_types: HashSet<String>,
    aggregate_ids: HashMap<String, HashSet<String>>,
}

impl ReplicationFilter {
    fn includes(&self, aggregate_type: &str, aggregate_id: &str) -> bool {
        if let Some(ids) = self.aggregate_ids.get(aggregate_type) {
            return ids.contains(aggregate_id);
        }
        if !self.aggregate_types.is_empty() {
            return self.aggregate_types.contains(aggregate_type);
        }
        if !self.aggregate_ids.is_empty() {
            return false;
        }
        true
    }
}

fn positions_to_map(
    entries: &[AggregatePositionEntry],
    filter: Option<&ReplicationFilter>,
) -> HashMap<(String, String), u64> {
    entries
        .iter()
        .filter(|entry| match filter {
            Some(filter) => filter.includes(&entry.aggregate_type, &entry.aggregate_id),
            None => true,
        })
        .map(|entry| {
            (
                (entry.aggregate_type.clone(), entry.aggregate_id.clone()),
                entry.version,
            )
        })
        .collect()
}

fn is_lock_error(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("lock file") || lower.contains("resource temporarily unavailable")
}

fn validate_fast_forward(
    remote_name: &str,
    local: &HashMap<(String, String), u64>,
    remote: &HashMap<(String, String), u64>,
) -> Result<()> {
    for (key, remote_version) in remote {
        if *remote_version == 0 {
            continue;
        }
        match local.get(key) {
            Some(local_version) => {
                if remote_version > local_version {
                    bail!(
                        "remote '{}' is ahead for aggregate {}::{} (remote version {}, local version {})",
                        remote_name,
                        key.0,
                        key.1,
                        remote_version,
                        local_version
                    );
                }
            }
            None => {
                bail!(
                    "remote '{}' contains aggregate {}::{} that does not exist locally; aborting push",
                    remote_name,
                    key.0,
                    key.1
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct SyncPlan {
    aggregate_type: String,
    aggregate_id: String,
    from_version: u64,
    to_version: u64,
    event_count: u64,
}

fn build_push_plans(
    local: &HashMap<(String, String), u64>,
    remote: &HashMap<(String, String), u64>,
) -> Vec<SyncPlan> {
    let mut plans = Vec::new();
    for (key, local_version) in local {
        let remote_version = remote.get(key).copied().unwrap_or(0);
        if remote_version > *local_version {
            continue;
        }
        if local_version > &remote_version {
            plans.push(SyncPlan {
                aggregate_type: key.0.clone(),
                aggregate_id: key.1.clone(),
                from_version: remote_version,
                to_version: *local_version,
                event_count: local_version - remote_version,
            });
        }
    }
    plans
}

fn build_pull_plans(
    local: &HashMap<(String, String), u64>,
    remote: &HashMap<(String, String), u64>,
) -> Vec<SyncPlan> {
    let mut plans = Vec::new();
    for (key, remote_version) in remote {
        let local_version = local.get(key).copied().unwrap_or(0);
        if remote_version > &local_version {
            plans.push(SyncPlan {
                aggregate_type: key.0.clone(),
                aggregate_id: key.1.clone(),
                from_version: local_version,
                to_version: *remote_version,
                event_count: remote_version - local_version,
            });
        }
    }
    plans
}

fn summarize_plans(plans: &[SyncPlan]) -> BTreeMap<String, u64> {
    let mut map = BTreeMap::new();
    for plan in plans {
        *map.entry(plan.aggregate_type.clone()).or_default() += plan.event_count;
    }
    map
}

#[derive(Debug)]
enum SchemaChange {
    Added {
        aggregate: String,
        event_count: usize,
    },
    Removed {
        aggregate: String,
        event_count: usize,
    },
    Updated {
        aggregate: String,
        diff: AggregateDiff,
    },
}

#[derive(Debug, Default)]
struct AggregateDiff {
    snapshot_threshold: Option<(Option<u64>, Option<u64>)>,
    locked: Option<(bool, bool)>,
    field_lock_added: Vec<String>,
    field_lock_removed: Vec<String>,
    events_added: Vec<String>,
    events_removed: Vec<String>,
    event_field_diffs: Vec<EventFieldDiff>,
}

impl AggregateDiff {
    fn is_empty(&self) -> bool {
        self.snapshot_threshold.is_none()
            && self.locked.is_none()
            && self.field_lock_added.is_empty()
            && self.field_lock_removed.is_empty()
            && self.events_added.is_empty()
            && self.events_removed.is_empty()
            && self.event_field_diffs.is_empty()
    }
}

#[derive(Debug)]
struct EventFieldDiff {
    event: String,
    added: Vec<String>,
    removed: Vec<String>,
}

fn diff_schemas(
    local: &BTreeMap<String, AggregateSchema>,
    remote: &BTreeMap<String, AggregateSchema>,
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    for (aggregate, remote_schema) in remote {
        match local.get(aggregate) {
            None => changes.push(SchemaChange::Added {
                aggregate: aggregate.clone(),
                event_count: remote_schema.events.len(),
            }),
            Some(local_schema) => {
                if let Some(diff) = diff_aggregate(local_schema, remote_schema) {
                    changes.push(SchemaChange::Updated {
                        aggregate: aggregate.clone(),
                        diff,
                    });
                }
            }
        }
    }

    for (aggregate, local_schema) in local {
        if !remote.contains_key(aggregate) {
            changes.push(SchemaChange::Removed {
                aggregate: aggregate.clone(),
                event_count: local_schema.events.len(),
            });
        }
    }

    changes
}

fn diff_aggregate(local: &AggregateSchema, remote: &AggregateSchema) -> Option<AggregateDiff> {
    let mut diff = AggregateDiff::default();

    if local.snapshot_threshold != remote.snapshot_threshold {
        diff.snapshot_threshold = Some((local.snapshot_threshold, remote.snapshot_threshold));
    }
    if local.locked != remote.locked {
        diff.locked = Some((local.locked, remote.locked));
    }

    let local_field_locks: BTreeSet<String> = local.field_locks.iter().cloned().collect();
    let remote_field_locks: BTreeSet<String> = remote.field_locks.iter().cloned().collect();
    diff.field_lock_added = set_difference(&remote_field_locks, &local_field_locks);
    diff.field_lock_removed = set_difference(&local_field_locks, &remote_field_locks);

    let local_events: BTreeSet<String> = local.events.keys().cloned().collect();
    let remote_events: BTreeSet<String> = remote.events.keys().cloned().collect();
    diff.events_added = set_difference(&remote_events, &local_events);
    diff.events_removed = set_difference(&local_events, &remote_events);

    for event in remote.events.keys() {
        if let Some(local_event) = local.events.get(event) {
            let local_fields: BTreeSet<String> = local_event.fields.iter().cloned().collect();
            let remote_event = remote
                .events
                .get(event)
                .expect("checked membership via keys iterator");
            let remote_fields: BTreeSet<String> = remote_event.fields.iter().cloned().collect();

            let added = set_difference(&remote_fields, &local_fields);
            let removed = set_difference(&local_fields, &remote_fields);
            if !added.is_empty() || !removed.is_empty() {
                diff.event_field_diffs.push(EventFieldDiff {
                    event: event.clone(),
                    added,
                    removed,
                });
            }
        }
    }

    if diff.is_empty() { None } else { Some(diff) }
}

fn set_difference(source: &BTreeSet<String>, other: &BTreeSet<String>) -> Vec<String> {
    source.difference(other).cloned().collect::<Vec<_>>()
}

fn print_schema_changes(header: &str, changes: &[SchemaChange]) {
    println!("{}", header);
    if changes.is_empty() {
        println!("  no schema changes detected");
        return;
    }

    println!("  {} aggregate change(s)", changes.len());
    for change in changes {
        match change {
            SchemaChange::Added {
                aggregate,
                event_count,
            } => {
                println!("    + {} ({} event(s))", aggregate, event_count);
            }
            SchemaChange::Removed {
                aggregate,
                event_count,
            } => {
                println!("    - {} ({} event(s))", aggregate, event_count);
            }
            SchemaChange::Updated { aggregate, diff } => {
                println!("    ~ {}", aggregate);
                if let Some((from, to)) = diff.snapshot_threshold {
                    println!("      snapshot_threshold: {:?} -> {:?}", from, to);
                }
                if let Some((from, to)) = diff.locked {
                    println!("      locked: {} -> {}", from, to);
                }
                if !diff.field_lock_added.is_empty() {
                    println!(
                        "      field locks added: {}",
                        diff.field_lock_added.join(", ")
                    );
                }
                if !diff.field_lock_removed.is_empty() {
                    println!(
                        "      field locks removed: {}",
                        diff.field_lock_removed.join(", ")
                    );
                }
                for event in &diff.events_added {
                    println!("      + event {}", event);
                }
                for event in &diff.events_removed {
                    println!("      - event {}", event);
                }
                for field_diff in &diff.event_field_diffs {
                    if !field_diff.added.is_empty() {
                        println!(
                            "      event {} fields added: {}",
                            field_diff.event,
                            field_diff.added.join(", ")
                        );
                    }
                    if !field_diff.removed.is_empty() {
                        println!(
                            "      event {} fields removed: {}",
                            field_diff.event,
                            field_diff.removed.join(", ")
                        );
                    }
                }
            }
        }
    }
}

fn print_summary(header: &str, stats: &BTreeMap<String, u64>) {
    let total_events: u64 = stats.values().copied().sum();
    let aggregate_count = stats.len();
    println!("{}", header);
    if stats.is_empty() {
        println!("  no changes detected");
    } else {
        println!(
            "  {} event(s) across {} aggregate(s)",
            total_events, aggregate_count
        );
        for (aggregate_type, count) in stats {
            println!("    {}: {} event(s)", aggregate_type, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use eventdbx::schema::EventSchema;

    fn make_schema(
        aggregate: &str,
        snapshot_threshold: Option<u64>,
        locked: bool,
        field_locks: &[&str],
        events: &[(&str, &[&str])],
    ) -> AggregateSchema {
        let mut schema = AggregateSchema {
            aggregate: aggregate.to_string(),
            snapshot_threshold,
            locked,
            field_locks: field_locks.iter().map(|value| value.to_string()).collect(),
            hidden: false,
            hidden_fields: Vec::new(),
            column_types: BTreeMap::new(),
            events: events
                .iter()
                .map(|(event, fields)| {
                    (
                        event.to_string(),
                        EventSchema {
                            fields: fields.iter().map(|value| value.to_string()).collect(),
                            notes: None,
                        },
                    )
                })
                .collect(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        schema.field_locks.sort();
        schema.field_locks.dedup();
        for event_schema in schema.events.values_mut() {
            event_schema.fields.sort();
            event_schema.fields.dedup();
        }

        schema
    }

    #[test]
    fn diff_detects_added_schema() {
        let local = BTreeMap::new();
        let mut remote = BTreeMap::new();
        remote.insert(
            "patient".into(),
            make_schema(
                "patient",
                Some(10),
                false,
                &[],
                &[("patient-created", &["id"])],
            ),
        );

        let changes = diff_schemas(&local, &remote);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            SchemaChange::Added {
                aggregate,
                event_count,
            } => {
                assert_eq!(aggregate, "patient");
                assert_eq!(*event_count, 1);
            }
            other => panic!("expected Added change, got {:?}", other),
        }
    }

    #[test]
    fn diff_detects_field_updates() {
        let mut local = BTreeMap::new();
        local.insert(
            "order".into(),
            make_schema(
                "order",
                Some(5),
                false,
                &["region"],
                &[("order-created", &["id"])],
            ),
        );

        let mut remote = BTreeMap::new();
        remote.insert(
            "order".into(),
            make_schema(
                "order",
                Some(12),
                true,
                &["region", "status"],
                &[("order-created", &["id", "status"])],
            ),
        );

        let changes = diff_schemas(&local, &remote);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            SchemaChange::Updated { aggregate, diff } => {
                assert_eq!(aggregate, "order");
                assert!(diff.snapshot_threshold.is_some());
                assert!(diff.locked.is_some());
                assert_eq!(diff.field_lock_added, vec!["status".to_string()]);
                assert!(diff.field_lock_removed.is_empty());
                assert!(diff.events_added.is_empty());
                assert!(diff.events_removed.is_empty());
                assert_eq!(diff.event_field_diffs.len(), 1);
                let field_diff = &diff.event_field_diffs[0];
                assert_eq!(field_diff.event, "order-created");
                assert_eq!(field_diff.added, vec!["status".to_string()]);
                assert!(field_diff.removed.is_empty());
            }
            other => panic!("expected Updated change, got {:?}", other),
        }
    }
}
#[derive(Args, Default)]
pub struct RemoteListArgs {
    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}
