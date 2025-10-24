use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Subcommand, ValueEnum};
use csv::WriterBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value};
use tempfile::tempdir;
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

use eventdbx::{
    config::{Config, load_or_default},
    error::EventError,
    merkle::compute_merkle_root,
    plugin::PluginManager,
    schema::{MAX_EVENT_NOTE_LENGTH, SchemaManager},
    store::{self, ActorClaims, AppendEvent, EventRecord, EventStore, payload_to_map},
    token::{IssueTokenInput, JwtLimits, TokenManager},
    validation::{
        ensure_aggregate_id, ensure_first_event_rule, ensure_metadata_extensions,
        ensure_payload_size, ensure_schema_declared, ensure_snake_case,
    },
};

#[cfg(test)]
use eventdbx::restrict::{self, RestrictMode};

use crate::commands::{cli_token, client::ServerClient};
use tracing::warn;

#[derive(Subcommand)]
pub enum AggregateCommands {
    /// Apply an event to an aggregate instance
    Apply(AggregateApplyArgs),
    /// List aggregates in the store
    List(AggregateListArgs),
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
    /// Remove an aggregate that has no events
    Remove(AggregateRemoveArgs),
    /// Commit events previously staged with `aggregate apply --stage`
    Commit,
    /// Export aggregate state to CSV or JSON
    Export(AggregateExportArgs),
}

#[derive(Args)]
pub struct AggregateApplyArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Event type to append
    pub event: String,

    /// Event fields expressed as KEY=VALUE pairs
    #[arg(long = "field", value_parser = parse_key_value, value_name = "KEY=VALUE")]
    pub fields: Vec<KeyValue>,

    /// Stage the event for a later commit instead of writing immediately
    #[arg(long, default_value_t = false)]
    pub stage: bool,

    /// Authorization token used when proxying through a running server
    #[arg(long, value_name = "TOKEN")]
    pub token: Option<String>,

    /// Raw JSON payload to use instead of key-value fields
    #[arg(long, value_name = "JSON")]
    pub payload: Option<String>,

    /// JSON metadata with plugin-specific keys prefixed by '@'
    #[arg(long)]
    pub metadata: Option<String>,

    /// Optional note associated with the event (up to 128 characters)
    #[arg(long, value_name = "NOTE")]
    pub note: Option<String>,

    /// JSON Patch (RFC 6902) document to apply server-side
    #[arg(long)]
    pub patch: Option<String>,
}

#[derive(Args)]
pub struct AggregateGetArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Optional version to compute state at
    #[arg(long)]
    pub version: Option<u64>,

    /// Include event history in the output
    #[arg(long, default_value_t = false)]
    pub include_events: bool,
}

#[derive(Args)]
pub struct AggregateReplayArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Number of events to skip
    #[arg(long, default_value_t = 0)]
    pub skip: usize,

    /// Number of events to return
    #[arg(long)]
    pub take: Option<usize>,

    /// Emit results as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args)]
pub struct AggregateVerifyArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Emit results as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args)]
pub struct AggregateSnapshotArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Optional comment to record with the snapshot
    #[arg(long)]
    pub comment: Option<String>,
}

#[derive(Args)]
pub struct AggregateArchiveArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,

    /// Optional comment recorded with the action
    #[arg(long)]
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Args)]
pub struct AggregateRemoveArgs {
    /// Aggregate type
    pub aggregate: String,

    /// Aggregate identifier
    pub aggregate_id: String,
}

#[derive(Args)]
pub struct AggregateListArgs {
    /// Number of aggregates to skip
    #[arg(long, default_value_t = 0)]
    pub skip: usize,

    /// Maximum number of aggregates to return
    #[arg(long)]
    pub take: Option<usize>,

    /// Show staged events instead of persisted aggregates
    #[arg(long, default_value_t = false)]
    pub stage: bool,

    /// Emit results as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum AggregateExportFormat {
    Csv,
    Json,
}

#[derive(Args)]
pub struct AggregateExportArgs {
    /// Aggregate type to export (omit when using --all)
    pub aggregate: Option<String>,

    /// Export every aggregate type
    #[arg(long)]
    pub all: bool,

    /// Output format
    #[arg(long, value_enum, default_value_t = AggregateExportFormat::Csv)]
    pub format: AggregateExportFormat,

    /// Output path (file or directory depending on context)
    #[arg(long)]
    pub output: PathBuf,

    /// Compress the export into a ZIP archive
    #[arg(long, default_value_t = false)]
    pub zip: bool,

    /// Pretty-print JSON output (no effect for CSV)
    #[arg(long, default_value_t = false)]
    pub pretty: bool,
}

#[derive(Clone)]
struct ExportRecord {
    aggregate_id: String,
    state: BTreeMap<String, String>,
}

const EXPORT_ID_KEY: &str = "__aggregate_id";

pub fn execute(config_path: Option<PathBuf>, command: AggregateCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    match command {
        AggregateCommands::List(args) => {
            if args.stage {
                let staging_path = config.staging_path();
                let staged_events = load_staged_events(staging_path.as_path())?;
                if staged_events.is_empty() {
                    println!("no staged events");
                } else {
                    for event in staged_events {
                        println!("{}", serde_json::to_string_pretty(&event)?);
                    }
                }
                return Ok(());
            }

            let store =
                EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;
            let take = args.take.or(Some(config.list_page_size));
            let aggregates = store.aggregates_paginated(args.skip, take);
            if args.json {
                println!("{}", serde_json::to_string_pretty(&aggregates)?);
            } else {
                for aggregate in aggregates {
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
        }
        AggregateCommands::Remove(args) => {
            let store = EventStore::open(config.event_store_path(), config.encryption_key()?)?;
            store.remove_aggregate(&args.aggregate, &args.aggregate_id)?;
            println!(
                "aggregate_type={} aggregate_id={} removed",
                args.aggregate, args.aggregate_id
            );
        }
        AggregateCommands::Get(args) => {
            let store =
                EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;
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

            let mut output = serde_json::json!({
                "aggregate_type": state.aggregate_type,
                "aggregate_id": state.aggregate_id,
                "version": state.version,
                "state": state.state,
                "merkle_root": state.merkle_root,
                "archived": state.archived,
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
            let AggregateApplyArgs {
                aggregate,
                aggregate_id,
                event,
                fields,
                stage,
                token,
                payload: payload_arg,
                metadata,
                note,
                patch,
            } = args;
            if payload_arg.is_some() && !fields.is_empty() {
                bail!("--payload cannot be used together with --field");
            }
            if patch.is_some() && (payload_arg.is_some() || !fields.is_empty()) {
                bail!("--patch cannot be combined with --payload or --field");
            }
            let payload = match payload_arg {
                Some(raw) => serde_json::from_str(&raw)
                    .with_context(|| "failed to parse JSON payload provided via --payload")?,
                None => collect_payload(fields),
            };
            let patch_ops = match patch {
                Some(raw) => Some(
                    serde_json::from_str::<serde_json::Value>(&raw)
                        .with_context(|| "failed to parse JSON patch provided via --patch")?,
                ),
                None => None,
            };
            if let Some(ref note_value) = note {
                if note_value.chars().count() > MAX_EVENT_NOTE_LENGTH {
                    bail!("note cannot exceed {} characters", MAX_EVENT_NOTE_LENGTH);
                }
            }

            let metadata_value = match metadata {
                Some(raw) => Some(
                    serde_json::from_str::<Value>(&raw)
                        .with_context(|| "failed to parse JSON metadata provided via --metadata")?,
                ),
                None => None,
            };
            if let Some(ref metadata) = metadata_value {
                ensure_metadata_extensions(metadata)?;
            }

            ensure_snake_case("aggregate_type", &aggregate)?;
            ensure_snake_case("event_type", &event)?;
            ensure_aggregate_id(&aggregate_id)?;

            let schema_manager = SchemaManager::load(config.schema_store_path())?;
            ensure_schema_declared(&schema_manager, &aggregate)?;
            if patch_ops.is_none() {
                ensure_payload_size(&payload)?;
                schema_manager.validate_event(&aggregate, &event, &payload)?;
            }

            if stage {
                match EventStore::open(config.event_store_path(), config.encryption_key()?) {
                    Ok(store) => {
                        let effective_payload = if let Some(ref ops) = patch_ops {
                            store.prepare_payload_from_patch(&aggregate, &aggregate_id, ops)?
                        } else {
                            payload.clone()
                        };
                        if patch_ops.is_some() {
                            ensure_payload_size(&effective_payload)?;
                            schema_manager.validate_event(
                                &aggregate,
                                &event,
                                &effective_payload,
                            )?;
                        }
                        let is_new = match store.aggregate_version(&aggregate, &aggregate_id)? {
                            Some(version) if version > 0 => false,
                            Some(_) | None => true,
                        };
                        ensure_first_event_rule(is_new, &event)?;
                        {
                            let mut tx = store.transaction()?;
                            tx.append(AppendEvent {
                                aggregate_type: aggregate.clone(),
                                aggregate_id: aggregate_id.clone(),
                                event_type: event.clone(),
                                payload: effective_payload.clone(),
                                metadata: metadata_value.clone(),
                                issued_by: None,
                                note: note.clone(),
                            })?;
                        }

                        let staged_event = StagedEvent {
                            aggregate,
                            aggregate_id,
                            event,
                            payload: effective_payload,
                            metadata: metadata_value.clone(),
                            issued_by: None,
                            note,
                        };
                        let staging_path = config.staging_path();
                        append_staged_event(staging_path.as_path(), staged_event)?;
                        println!("event staged for later commit");
                        return Ok(());
                    }
                    Err(EventError::Storage(message)) if is_lock_error(&message) => {
                        bail!(
                            "event store is locked by a running server.\nStop the server or omit --stage."
                        );
                    }
                    Err(err) => return Err(err.into()),
                }
            }

            let encryption = config.encryption_key()?;
            match EventStore::open(config.event_store_path(), encryption) {
                Ok(store) => {
                    let plugins = PluginManager::from_config(&config)?;
                    let effective_payload = if let Some(ref ops) = patch_ops {
                        store.prepare_payload_from_patch(&aggregate, &aggregate_id, ops)?
                    } else {
                        payload.clone()
                    };
                    if patch_ops.is_some() {
                        ensure_payload_size(&effective_payload)?;
                        schema_manager.validate_event(&aggregate, &event, &effective_payload)?;
                    }
                    let is_new = match store.aggregate_version(&aggregate, &aggregate_id)? {
                        Some(version) if version > 0 => false,
                        Some(_) | None => true,
                    };
                    ensure_first_event_rule(is_new, &event)?;
                    let record = store.append(AppendEvent {
                        aggregate_type: aggregate.clone(),
                        aggregate_id: aggregate_id.clone(),
                        event_type: event.clone(),
                        payload: effective_payload.clone(),
                        metadata: metadata_value.clone(),
                        issued_by: None,
                        note: note.clone(),
                    })?;

                    maybe_auto_snapshot(&store, &schema_manager, &record);
                    println!("{}", serde_json::to_string_pretty(&record)?);

                    if !plugins.is_empty() {
                        let schema = schema_manager.get(&record.aggregate_type).ok();
                        match store
                            .get_aggregate_state(&record.aggregate_type, &record.aggregate_id)
                        {
                            Ok(current_state) => {
                                if let Err(err) =
                                    plugins.notify_event(&record, &current_state, schema.as_ref())
                                {
                                    eprintln!("plugin notification failed: {}", err);
                                }
                            }
                            Err(err) => {
                                eprintln!(
                                    "plugin notification skipped (failed to load state): {}",
                                    err
                                );
                            }
                        }
                    }
                    return Ok(());
                }
                Err(EventError::Storage(message)) if is_lock_error(&message) => {
                    if !config.api.rest_enabled() {
                        bail!(
                            "event store is locked by a running server, and the REST API is disabled.\nEnable REST (e.g. `eventdbx config --api rest`) or stop the server to continue with CLI writes."
                        );
                    }
                    let record = proxy_append_via_http(
                        &config,
                        token.clone(),
                        &aggregate,
                        &aggregate_id,
                        &event,
                        if patch_ops.is_some() {
                            None
                        } else {
                            Some(&payload)
                        },
                        patch_ops.as_ref(),
                        metadata_value.as_ref(),
                        note.as_deref(),
                    )?;
                    println!("{}", serde_json::to_string_pretty(&record)?);
                    return Ok(());
                }
                Err(err) => return Err(err.into()),
            }
        }
        AggregateCommands::Replay(args) => {
            let store =
                EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;
            let events = store.list_events(&args.aggregate, &args.aggregate_id)?;
            let iter = events.into_iter().skip(args.skip);
            let events: Vec<_> = if let Some(limit) = args.take {
                iter.take(limit).collect()
            } else {
                iter.collect()
            };

            if args.json {
                println!("{}", serde_json::to_string_pretty(&events)?);
            } else {
                for event in events {
                    println!("{}", serde_json::to_string_pretty(&event)?);
                }
            }
        }
        AggregateCommands::Verify(args) => {
            let store =
                EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;
            let merkle_root = store.verify(&args.aggregate, &args.aggregate_id)?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "aggregate_type": args.aggregate,
                        "aggregate_id": args.aggregate_id,
                        "merkle_root": merkle_root,
                    }))?
                );
            } else {
                println!(
                    "aggregate_type={} aggregate_id={} merkle_root={}",
                    args.aggregate, args.aggregate_id, merkle_root
                );
            }
        }
        AggregateCommands::Snapshot(args) => {
            let store = EventStore::open(config.event_store_path(), config.encryption_key()?)?;
            let snapshot =
                store.create_snapshot(&args.aggregate, &args.aggregate_id, args.comment.clone())?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        AggregateCommands::Archive(args) => {
            let store = EventStore::open(config.event_store_path(), config.encryption_key()?)?;
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
            let store = EventStore::open(config.event_store_path(), config.encryption_key()?)?;
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
            let staging_path = config.staging_path();
            let staged_events = load_staged_events(staging_path.as_path())?;
            if staged_events.is_empty() {
                println!("no staged events to commit");
                return Ok(());
            }

            let schema_manager = SchemaManager::load(config.schema_store_path())?;
            let store = EventStore::open(config.event_store_path(), config.encryption_key()?)?;
            let plugins = PluginManager::from_config(&config)?;
            let mut tx = store.transaction()?;

            for staged_event in &staged_events {
                ensure_snake_case("aggregate_type", &staged_event.aggregate)?;
                ensure_snake_case("event_type", &staged_event.event)?;
                ensure_aggregate_id(&staged_event.aggregate_id)?;
                ensure_payload_size(&staged_event.payload)?;
                if let Some(ref metadata) = staged_event.metadata {
                    ensure_metadata_extensions(metadata)?;
                }

                let is_new = match store
                    .aggregate_version(&staged_event.aggregate, &staged_event.aggregate_id)?
                {
                    Some(version) if version > 0 => false,
                    Some(_) | None => true,
                };
                ensure_first_event_rule(is_new, &staged_event.event)?;

                ensure_schema_declared(&schema_manager, &staged_event.aggregate)?;
                schema_manager.validate_event(
                    &staged_event.aggregate,
                    &staged_event.event,
                    &staged_event.payload,
                )?;

                tx.append(staged_event.to_append_event())?;
            }

            let records = tx.commit()?;
            for record in &records {
                println!("{}", serde_json::to_string_pretty(record)?);
                maybe_auto_snapshot(&store, &schema_manager, record);
            }

            if !plugins.is_empty() {
                for record in &records {
                    let schema = schema_manager.get(&record.aggregate_type).ok();
                    match store.get_aggregate_state(&record.aggregate_type, &record.aggregate_id) {
                        Ok(current_state) => {
                            if let Err(err) =
                                plugins.notify_event(record, &current_state, schema.as_ref())
                            {
                                eprintln!("plugin notification failed: {}", err);
                            }
                        }
                        Err(err) => {
                            eprintln!(
                                "plugin notification skipped (failed to load state): {}",
                                err
                            );
                        }
                    }
                }
            }

            clear_staged_events(staging_path.as_path())?;
            println!("committed {} event(s)", records.len());
        }
        AggregateCommands::Export(args) => {
            export_aggregates(&config, args)?;
        }
    }

    Ok(())
}

fn maybe_auto_snapshot(store: &EventStore, schemas: &SchemaManager, record: &EventRecord) {
    if !schemas.should_snapshot(&record.aggregate_type, record.version) {
        return;
    }

    match store.create_snapshot(
        &record.aggregate_type,
        &record.aggregate_id,
        Some(format!("auto snapshot v{}", record.version)),
    ) {
        Ok(snapshot) => eprintln!(
            "auto snapshot created: aggregate={} aggregate_id={} version={}",
            snapshot.aggregate_type, snapshot.aggregate_id, snapshot.version
        ),
        Err(err) => eprintln!(
            "failed to create auto snapshot for {}::{} v{}: {}",
            record.aggregate_type, record.aggregate_id, record.version, err
        ),
    }
}

#[cfg(test)]
fn ensure_schema_for_mode(
    mode: RestrictMode,
    schema_manager: &SchemaManager,
    aggregate: &str,
) -> Result<()> {
    if mode.requires_declared_schema() {
        schema_manager.get(aggregate).map(|_| ()).map_err(|_| {
            EventError::SchemaViolation(restrict::strict_mode_missing_schema_message(aggregate))
        })?;
    }
    Ok(())
}

fn proxy_append_via_http(
    config: &Config,
    token: Option<String>,
    aggregate: &str,
    aggregate_id: &str,
    event: &str,
    payload: Option<&Value>,
    patch: Option<&Value>,
    metadata: Option<&Value>,
    note: Option<&str>,
) -> Result<EventRecord> {
    let token = ensure_proxy_token(config, token)?;
    let client = ServerClient::new(config)?;
    client
        .append_event(
            &token,
            aggregate,
            aggregate_id,
            event,
            payload,
            patch,
            metadata,
            note,
        )
        .with_context(|| {
            format!(
                "failed to append event via running server on port {}",
                config.port
            )
        })
}

fn ensure_proxy_token(config: &Config, token: Option<String>) -> Result<String> {
    if let Some(token) = token.and_then(normalize_token) {
        return Ok(token);
    }
    if let Some(token) = env::var("EVENTDBX_TOKEN").ok().and_then(normalize_token) {
        return Ok(token);
    }
    match cli_token::ensure_bootstrap_token(config) {
        Ok(token) => return Ok(token),
        Err(err) => {
            warn!(
                "failed to load CLI bootstrap token ({}); falling back to ephemeral token",
                err
            );
        }
    }
    issue_ephemeral_token(config)
}

fn issue_ephemeral_token(config: &Config) -> Result<String> {
    let encryptor = config.encryption_key()?;
    let jwt_config = config.jwt_manager_config()?;
    let manager = TokenManager::load(
        jwt_config,
        config.tokens_path(),
        config.jwt_revocations_path(),
        encryptor,
    )?;
    let user = proxy_user_identity();
    let subject = format!("cli:{}", user);
    let record = manager.issue(IssueTokenInput {
        subject,
        group: "cli".to_string(),
        user,
        root: true,
        actions: Vec::new(),
        resources: Vec::new(),
        ttl_secs: Some(120),
        not_before: None,
        issued_by: "cli".to_string(),
        limits: JwtLimits {
            write_events: None,
            keep_alive: false,
        },
    })?;
    Ok(record.token)
}

fn proxy_user_identity() -> String {
    let env_value = env::var("USER")
        .or_else(|_| env::var("USERNAME"))
        .ok()
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

    env_value.unwrap_or_else(|| "local".to_string())
}

fn normalize_token(token: String) -> Option<String> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn is_lock_error(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("lock file") || lower.contains("resource temporarily unavailable")
}

fn export_aggregates(config: &Config, args: AggregateExportArgs) -> Result<()> {
    let AggregateExportArgs {
        aggregate,
        all,
        format,
        output,
        zip,
        pretty,
    } = args;

    if all && aggregate.is_some() {
        bail!("aggregate name cannot be provided when using --all");
    }
    if !all && aggregate.is_none() {
        bail!("aggregate name must be provided unless --all is specified");
    }

    let target_name = if all { None } else { aggregate };
    let store = EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;
    let exports = collect_export_records(&store, target_name.as_deref(), all)?;

    if exports.is_empty() {
        if let Some(name) = target_name {
            println!("no aggregates found for '{}'", name);
        } else {
            println!("no aggregates found");
        }
        return Ok(());
    }

    if zip {
        let zip_path = export_as_zip(&exports, format, pretty, output)?;
        println!(
            "Exported {} aggregate type(s) to {} (zip archive)",
            exports.len(),
            zip_path.display()
        );
    } else {
        let files = export_to_files(&exports, format, pretty, output)?;
        for path in &files {
            println!("wrote {}", path.display());
        }
        println!(
            "Exported {} aggregate type(s) in {} format",
            exports.len(),
            export_suffix(format).to_uppercase()
        );
    }

    Ok(())
}

fn collect_export_records(
    store: &EventStore,
    target: Option<&str>,
    include_all: bool,
) -> Result<BTreeMap<String, Vec<ExportRecord>>> {
    let mut map: BTreeMap<String, Vec<ExportRecord>> = BTreeMap::new();
    let filter = if include_all {
        None
    } else {
        Some(
            target
                .ok_or_else(|| anyhow!("aggregate name must be provided unless --all is set"))?
                .to_string(),
        )
    };

    for aggregate in store.aggregates_paginated(0, None) {
        let store::AggregateState {
            aggregate_type,
            aggregate_id,
            state,
            ..
        } = aggregate;

        if let Some(ref filter_type) = filter {
            if &aggregate_type != filter_type {
                continue;
            }
        }

        map.entry(aggregate_type).or_default().push(ExportRecord {
            aggregate_id,
            state,
        });
    }

    for rows in map.values_mut() {
        rows.sort_by(|a, b| a.aggregate_id.cmp(&b.aggregate_id));
    }

    Ok(map)
}

fn export_to_files(
    exports: &BTreeMap<String, Vec<ExportRecord>>,
    format: AggregateExportFormat,
    pretty: bool,
    output: PathBuf,
) -> Result<Vec<PathBuf>> {
    let multiple = exports.len() > 1;

    if multiple {
        if output.exists() && output.is_file() {
            bail!("output path must be a directory when exporting multiple aggregate types");
        }
        let files = export_into_directory(output.as_path(), exports, format, pretty)?;
        return Ok(files.into_iter().map(|(path, _)| path).collect());
    }

    let (aggregate_type, rows) = exports.iter().next().expect("exports is not empty");
    let target = output;
    let as_file = should_treat_as_file(&target, format);

    if as_file {
        if let Some(parent) = target.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        match format {
            AggregateExportFormat::Csv => write_csv_file(&target, rows)?,
            AggregateExportFormat::Json => write_json_file(&target, rows, pretty)?,
        }
        return Ok(vec![target]);
    }

    fs::create_dir_all(&target)?;
    let file_name = export_file_name(aggregate_type, format);
    let file_path = target.join(file_name);
    match format {
        AggregateExportFormat::Csv => write_csv_file(&file_path, rows)?,
        AggregateExportFormat::Json => write_json_file(&file_path, rows, pretty)?,
    }
    Ok(vec![file_path])
}

fn should_treat_as_file(path: &Path, format: AggregateExportFormat) -> bool {
    if path.exists() {
        return path.is_file();
    }

    let expected_extension = export_suffix(format);
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case(expected_extension))
        .unwrap_or(false)
}

fn export_as_zip(
    exports: &BTreeMap<String, Vec<ExportRecord>>,
    format: AggregateExportFormat,
    pretty: bool,
    output: PathBuf,
) -> Result<PathBuf> {
    let temp = tempdir()?;
    let generated = export_into_directory(temp.path(), exports, format, pretty)?;
    let zip_path = normalize_zip_output(output, exports, format)?;
    create_zip_archive(&generated, &zip_path)?;
    Ok(zip_path)
}

fn export_into_directory(
    base_dir: &Path,
    exports: &BTreeMap<String, Vec<ExportRecord>>,
    format: AggregateExportFormat,
    pretty: bool,
) -> Result<Vec<(PathBuf, String)>> {
    fs::create_dir_all(base_dir)?;
    let mut files = Vec::new();

    for (aggregate_type, rows) in exports {
        let file_name = export_file_name(aggregate_type, format);
        let file_path = base_dir.join(&file_name);
        match format {
            AggregateExportFormat::Csv => write_csv_file(&file_path, rows)?,
            AggregateExportFormat::Json => write_json_file(&file_path, rows, pretty)?,
        }
        files.push((file_path, file_name));
    }

    Ok(files)
}

fn write_csv_file(path: &Path, rows: &[ExportRecord]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let mut dynamic_columns = BTreeSet::new();
    for row in rows {
        for key in row.state.keys() {
            dynamic_columns.insert(key.clone());
        }
    }

    let mut headers = Vec::with_capacity(dynamic_columns.len() + 1);
    headers.push(EXPORT_ID_KEY.to_string());
    headers.extend(dynamic_columns.into_iter());

    let mut writer = WriterBuilder::new().from_path(path)?;
    writer.write_record(&headers)?;

    for row in rows {
        let mut record = Vec::with_capacity(headers.len());
        for (idx, header) in headers.iter().enumerate() {
            if idx == 0 {
                record.push(row.aggregate_id.clone());
            } else {
                record.push(row.state.get(header).cloned().unwrap_or_default());
            }
        }
        writer.write_record(record)?;
    }

    writer.flush()?;
    Ok(())
}

fn write_json_file(path: &Path, rows: &[ExportRecord], pretty: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let mut file = File::create(path)?;
    if pretty {
        let entries: Vec<_> = rows.iter().map(record_to_json).collect();
        serde_json::to_writer_pretty(&mut file, &entries)?;
    } else {
        file.write_all(b"[")?;
        for (idx, row) in rows.iter().enumerate() {
            if idx > 0 {
                file.write_all(b",")?;
            }
            let value = record_to_json(row);
            let payload = serde_json::to_vec(&value)?;
            file.write_all(&payload)?;
        }
        file.write_all(b"]")?;
    }
    file.write_all(b"\n")?;
    Ok(())
}

fn record_to_json(record: &ExportRecord) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(
        EXPORT_ID_KEY.to_string(),
        Value::String(record.aggregate_id.clone()),
    );
    for (key, value) in &record.state {
        map.insert(key.clone(), Value::String(value.clone()));
    }
    Value::Object(map)
}

fn normalize_zip_output(
    output: PathBuf,
    exports: &BTreeMap<String, Vec<ExportRecord>>,
    format: AggregateExportFormat,
) -> Result<PathBuf> {
    if output.exists() && output.is_dir() {
        let file_name = default_zip_name(exports, format);
        let path = output.join(file_name);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        return Ok(path);
    }

    let mut path = output;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let has_zip_extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("zip"))
        .unwrap_or(false);

    if !has_zip_extension {
        if path.file_name().is_none() || path.file_name().unwrap().is_empty() {
            path = PathBuf::from(default_zip_name(exports, format));
        } else {
            path.set_extension("zip");
        }
    }

    Ok(path)
}

fn default_zip_name(
    exports: &BTreeMap<String, Vec<ExportRecord>>,
    format: AggregateExportFormat,
) -> String {
    let suffix = export_suffix(format);
    let stem = if exports.len() == 1 {
        let name = exports.keys().next().expect("exports not empty");
        format!("{}_{}", sanitize_component(name), suffix)
    } else {
        format!("aggregates_{}", suffix)
    };
    format!("{stem}.zip")
}

fn export_file_name(aggregate_type: &str, format: AggregateExportFormat) -> String {
    let stem = sanitize_component(aggregate_type);
    let suffix = export_suffix(format);
    format!("{stem}.{suffix}")
}

fn export_suffix(format: AggregateExportFormat) -> &'static str {
    match format {
        AggregateExportFormat::Csv => "csv",
        AggregateExportFormat::Json => "json",
    }
}

fn sanitize_component(input: &str) -> String {
    let mut sanitized = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        sanitized.push('_');
    }
    sanitized
}

fn create_zip_archive(files: &[(PathBuf, String)], output: &Path) -> Result<()> {
    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let file = File::create(output)?;
    let mut zip = ZipWriter::new(file);
    let options = FileOptions::default().compression_method(CompressionMethod::Deflated);

    for (path, name) in files {
        zip.start_file(name, options)?;
        let mut reader = File::open(path)?;
        std::io::copy(&mut reader, &mut zip)?;
    }

    zip.finish()?;
    Ok(())
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

fn collect_payload(fields: Vec<KeyValue>) -> Value {
    let mut map = JsonMap::new();
    for kv in fields {
        map.insert(kv.key, Value::String(kv.value));
    }
    Value::Object(map)
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
        for (key, value) in payload_to_map(&event.payload) {
            state.insert(key, value);
        }
    }

    let merkle_root = compute_merkle_root(&hashes);
    (last_version, state, merkle_root)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedEvent {
    aggregate: String,
    aggregate_id: String,
    event: String,
    #[serde(default = "default_event_payload")]
    payload: Value,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    issued_by: Option<StagedIssuedBy>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedIssuedBy {
    group: String,
    user: String,
}

impl StagedEvent {
    fn to_append_event(&self) -> AppendEvent {
        AppendEvent {
            aggregate_type: self.aggregate.clone(),
            aggregate_id: self.aggregate_id.clone(),
            event_type: self.event.clone(),
            payload: self.payload.clone(),
            metadata: self.metadata.clone(),
            issued_by: self.issued_by.clone().map(Into::into),
            note: self.note.clone(),
        }
    }
}

impl From<StagedIssuedBy> for ActorClaims {
    fn from(value: StagedIssuedBy) -> Self {
        ActorClaims {
            group: value.group,
            user: value.user,
        }
    }
}

fn default_event_payload() -> Value {
    Value::Null
}

fn load_staged_events(path: &Path) -> Result<Vec<StagedEvent>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path)?;
    if contents.trim().is_empty() {
        return Ok(Vec::new());
    }

    let events = serde_json::from_str::<Vec<StagedEvent>>(&contents)?;
    Ok(events)
}

fn append_staged_event(path: &Path, event: StagedEvent) -> Result<()> {
    let mut events = load_staged_events(path)?;
    events.push(event);
    save_staged_events(path, &events)
}

fn clear_staged_events(path: &Path) -> Result<()> {
    if path.exists() {
        fs::write(path, "[]")?;
    }
    Ok(())
}

fn save_staged_events(path: &Path, events: &[StagedEvent]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_string_pretty(events)?;
    fs::write(path, payload)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventdbx::schema::CreateSchemaInput;

    #[test]
    fn ensure_schema_for_mode_respects_modes() {
        let dir = tempdir().expect("tempdir should be created");
        let manager = SchemaManager::load(dir.path().join("schemas.json"))
            .expect("schema manager should load");

        ensure_schema_for_mode(RestrictMode::Off, &manager, "account")
            .expect("off mode bypasses schema checks");
        ensure_schema_for_mode(RestrictMode::Default, &manager, "account")
            .expect("default mode allows missing schema");

        let err = ensure_schema_for_mode(RestrictMode::Strict, &manager, "account")
            .expect_err("strict mode should reject missing schemas");
        assert!(err.to_string().contains("restrict=strict"));

        manager
            .create(CreateSchemaInput {
                aggregate: "account".into(),
                events: vec!["opened".into()],
                snapshot_threshold: None,
            })
            .expect("schema creation should succeed");

        ensure_schema_for_mode(RestrictMode::Strict, &manager, "account")
            .expect("strict mode accepts existing schema");
    }
}
