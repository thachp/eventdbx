use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use chrono::Utc;
use clap::{Args, Subcommand};
use tokio::runtime::Runtime;

use eventdbx::{
    config::{RemoteConfig, load_or_default},
    replication::{
        convert_event, decode_event, normalize_endpoint,
        proto::{
            AggregatePosition, EventBatch, ListPositionsRequest, PullEventsRequest,
            replication_client::ReplicationClient,
        },
    },
    store::{AggregatePositionEntry, EventStore},
};
use tonic::{Request, transport::Channel};

#[derive(Subcommand)]
pub enum RemoteCommands {
    /// Add a replication remote
    Add(RemoteAddArgs),
    /// Remove a replication remote
    #[command(name = "rm")]
    Remove(RemoteRemoveArgs),
    /// List configured remotes
    #[command(name = "ls")]
    List,
    /// Show details for a remote
    Show(RemoteShowArgs),
    /// Display this node's replication public key
    Key(RemoteKeyArgs),
    /// Push events to a remote standby
    Push(RemotePushArgs),
    /// Pull events from a remote primary
    Pull(RemotePullArgs),
}

#[derive(Args)]
pub struct RemoteAddArgs {
    /// Remote alias (e.g. standby1)
    pub name: String,
    /// Remote gRPC endpoint (e.g. grpc://10.0.0.1:7443)
    pub endpoint: String,
    /// Base64-encoded Ed25519 public key for the remote
    #[arg(long = "public-key")]
    pub public_key: String,
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

#[derive(Args, Default)]
pub struct RemoteKeyArgs {
    /// Also print the path to the public key file
    #[arg(long, default_value_t = false)]
    pub show_path: bool,
}

#[derive(Args)]
pub struct RemotePushArgs {
    /// Remote alias to push to
    pub name: String,
    /// Perform a dry run without sending data
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    /// Number of events per batch when pushing
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,
    /// Limit the push to specific aggregate types
    #[arg(long = "aggregate", value_name = "TYPE")]
    pub aggregates: Vec<String>,
    /// Limit the push to specific aggregate identifiers (TYPE:ID)
    #[arg(long = "aggregate-id", value_name = "TYPE:ID")]
    pub aggregate_ids: Vec<String>,
}

#[derive(Args)]
pub struct RemotePullArgs {
    /// Remote alias to pull from
    pub name: String,
    /// Perform a dry run without writing data
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    /// Maximum events to request per gRPC call
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,
    /// Limit the pull to specific aggregate types
    #[arg(long = "aggregate", value_name = "TYPE")]
    pub aggregates: Vec<String>,
    /// Limit the pull to specific aggregate identifiers (TYPE:ID)
    #[arg(long = "aggregate-id", value_name = "TYPE:ID")]
    pub aggregate_ids: Vec<String>,
}

pub fn execute(config_path: Option<PathBuf>, command: RemoteCommands) -> Result<()> {
    match command {
        RemoteCommands::Add(args) => add_remote(config_path, args),
        RemoteCommands::Remove(args) => remove_remote(config_path, args),
        RemoteCommands::List => list_remotes(config_path),
        RemoteCommands::Show(args) => show_remote(config_path, args),
        RemoteCommands::Key(args) => show_local_key(config_path, args),
        RemoteCommands::Push(args) => push_remote(config_path, args),
        RemoteCommands::Pull(args) => pull_remote(config_path, args),
    }
}

fn add_remote(config_path: Option<PathBuf>, args: RemoteAddArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    let name = args.name.trim();
    if name.is_empty() {
        bail!("remote name cannot be empty");
    }

    let endpoint = args.endpoint.trim();
    if endpoint.is_empty() {
        bail!("remote endpoint cannot be empty");
    }

    let public_key = normalize_public_key(&args.public_key)?;

    if config.remotes.contains_key(name) && !args.replace {
        bail!(
            "remote '{}' already exists (use --replace to overwrite)",
            name
        );
    }

    config.remotes.insert(
        name.to_string(),
        RemoteConfig {
            endpoint: endpoint.to_string(),
            public_key: public_key.clone(),
        },
    );
    config.updated_at = Utc::now();
    config.save(&path)?;

    println!(
        "Remote '{}' set to endpoint {} with pinned key {}",
        name, endpoint, public_key
    );

    Ok(())
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

fn list_remotes(config_path: Option<PathBuf>) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    if config.remotes.is_empty() {
        println!("(no remotes configured)");
        return Ok(());
    }

    for (name, remote) in &config.remotes {
        println!("{} {}", name, remote.endpoint);
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
    println!("public_key: {}", remote.public_key);
    Ok(())
}

fn show_local_key(config_path: Option<PathBuf>, args: RemoteKeyArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let key = config
        .load_public_key()
        .context("failed to load local replication public key")?;
    println!("{}", key);
    if args.show_path {
        println!("path: {}", config.public_key_path().display());
    }
    Ok(())
}

fn push_remote(config_path: Option<PathBuf>, args: RemotePushArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let remote = config
        .remotes
        .get(&args.name)
        .cloned()
        .ok_or_else(|| anyhow!("no remote named '{}' is configured", args.name))?;

    let store = Arc::new(EventStore::open_read_only(config.event_store_path())?);
    let local_positions = store.aggregate_positions()?;

    let filter = normalize_filter(&args.aggregates, &args.aggregate_ids)?;

    let runtime = Runtime::new()?;
    runtime.block_on(push_remote_async(
        store,
        local_positions,
        remote,
        args.name,
        args.dry_run,
        args.batch_size.max(1),
        filter,
    ))
}

fn pull_remote(config_path: Option<PathBuf>, args: RemotePullArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let remote = config
        .remotes
        .get(&args.name)
        .cloned()
        .ok_or_else(|| anyhow!("no remote named '{}' is configured", args.name))?;

    let store = Arc::new(EventStore::open(config.event_store_path())?);
    let local_positions = store.aggregate_positions()?;

    let filter = normalize_filter(&args.aggregates, &args.aggregate_ids)?;

    let runtime = Runtime::new()?;
    runtime.block_on(pull_remote_async(
        store,
        local_positions,
        remote,
        args.name,
        args.dry_run,
        args.batch_size.max(1),
        filter,
    ))
}

fn normalize_public_key(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("public key cannot be empty");
    }

    let decoded = decode_public_key(trimmed)?;
    if decoded.len() != 32 {
        bail!("public key must decode to 32 bytes");
    }
    Ok(trimmed.to_string())
}

fn decode_public_key(input: &str) -> Result<Vec<u8>> {
    STANDARD_NO_PAD
        .decode(input)
        .or_else(|_| STANDARD.decode(input))
        .map_err(|err| anyhow!("invalid base64 public key: {}", err))
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

async fn push_remote_async(
    store: Arc<EventStore>,
    local_positions: Vec<AggregatePositionEntry>,
    remote: RemoteConfig,
    remote_name: String,
    dry_run: bool,
    batch_size: usize,
    filter: Option<ReplicationFilter>,
) -> Result<()> {
    let mut client = connect_client(&remote).await?;
    let response = client
        .list_positions(Request::new(ListPositionsRequest {}))
        .await
        .map_err(|err| anyhow!("remote {} list_positions failed: {}", remote_name, err))?
        .into_inner();

    let filter_ref = filter.as_ref();
    let remote_map = proto_positions_to_map(&response.positions, filter_ref);
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
            let proto_events = events
                .iter()
                .map(|event| convert_event(event).map_err(|err| anyhow!(err.to_string())))
                .collect::<Result<Vec<_>>>()?;

            let batch = EventBatch {
                sequence,
                events: proto_events,
            };

            client
                .apply_events(Request::new(tokio_stream::iter(vec![batch])))
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

async fn pull_remote_async(
    store: Arc<EventStore>,
    local_positions: Vec<AggregatePositionEntry>,
    remote: RemoteConfig,
    remote_name: String,
    dry_run: bool,
    batch_size: usize,
    filter: Option<ReplicationFilter>,
) -> Result<()> {
    let mut client = connect_client(&remote).await?;
    let response = client
        .list_positions(Request::new(ListPositionsRequest {}))
        .await
        .map_err(|err| anyhow!("remote {} list_positions failed: {}", remote_name, err))?
        .into_inner();

    let filter_ref = filter.as_ref();
    let remote_map = proto_positions_to_map(&response.positions, filter_ref);
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
            let request = PullEventsRequest {
                aggregate_type: plan.aggregate_type.clone(),
                aggregate_id: plan.aggregate_id.clone(),
                from_version: current,
                limit: batch_size as u64,
            };

            let response = client
                .pull_events(Request::new(request))
                .await
                .map_err(|err| {
                    anyhow!("failed to pull events from remote {}: {}", remote_name, err)
                })?
                .into_inner();

            if response.events.is_empty() {
                break;
            }

            for proto_event in response.events {
                let record = decode_event(&proto_event)
                    .map_err(|err| anyhow!("failed to decode event: {}", err))?;
                store.append_replica(record)?;
                current = proto_event.version;
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

async fn connect_client(remote: &RemoteConfig) -> Result<ReplicationClient<Channel>> {
    let endpoint = normalize_endpoint(&remote.endpoint)
        .map_err(|err| anyhow!("invalid endpoint {}: {}", remote.endpoint, err))?;
    let channel = tonic::transport::Endpoint::try_from(endpoint)
        .map_err(|err| anyhow!("failed to parse endpoint {}: {}", remote.endpoint, err))?
        .connect()
        .await
        .map_err(|err| anyhow!("failed to connect to remote {}: {}", remote.endpoint, err))?;
    Ok(ReplicationClient::new(channel))
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

fn proto_positions_to_map(
    positions: &[AggregatePosition],
    filter: Option<&ReplicationFilter>,
) -> HashMap<(String, String), u64> {
    positions
        .iter()
        .filter(|pos| match filter {
            Some(filter) => filter.includes(&pos.aggregate_type, &pos.aggregate_id),
            None => true,
        })
        .map(|pos| {
            (
                (pos.aggregate_type.clone(), pos.aggregate_id.clone()),
                pos.version,
            )
        })
        .collect()
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
