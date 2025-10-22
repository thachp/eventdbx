use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
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

use eventdbx::{
    config::{Config, RemoteConfig, load_or_default},
    error::EventError,
    replication_capnp_client::{
        CapnpReplicationClient, decode_public_key_bytes, decode_schemas, normalize_capnp_endpoint,
    },
    schema::{AggregateSchema, SchemaManager},
    store::{AggregatePositionEntry, EventStore},
};
use serde_json;

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
    /// Remote replication endpoint (e.g. tcp://10.0.0.1:7443)
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
        RemoteCommands::Key(args) => show_local_key(config_path, args),
        RemoteCommands::Push(args) => push_remote(config_path, args).await,
        RemoteCommands::Pull(args) => pull_remote(config_path, args).await,
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
            println!("{} {}", name, remote.endpoint);
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

async fn push_remote(config_path: Option<PathBuf>, mut args: RemotePushArgs) -> Result<()> {
    if args.schema_only {
        args.schema = true;
    }

    if args.schema_only && (!args.aggregates.is_empty() || !args.aggregate_ids.is_empty()) {
        bail!("--schema-only cannot be combined with aggregate filters");
    }

    let (config, _) = load_or_default(config_path)?;
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

async fn pull_remote(config_path: Option<PathBuf>, mut args: RemotePullArgs) -> Result<()> {
    if args.schema_only {
        args.schema = true;
    }

    if args.schema_only && (!args.aggregates.is_empty() || !args.aggregate_ids.is_empty()) {
        bail!("--schema-only cannot be combined with aggregate filters");
    }

    let (config, _) = load_or_default(config_path)?;
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
            match EventStore::open(config.event_store_path(), config.encryption_key()?) {
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
    let remote_positions = client
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
    let public_key = config
        .load_public_key()
        .context("failed to load local replication public key")?;
    let expected_key = decode_public_key_bytes(&public_key)?;
    CapnpReplicationClient::connect(&endpoint, &expected_key)
        .await
        .map_err(|err| anyhow!("failed to connect to local replication endpoint: {}", err))
}

async fn connect_client(remote: &RemoteConfig) -> Result<CapnpReplicationClient> {
    let endpoint = normalize_capnp_endpoint(&remote.endpoint)?;
    let expected_key = decode_public_key_bytes(&remote.public_key)?;
    CapnpReplicationClient::connect(&endpoint, &expected_key)
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
