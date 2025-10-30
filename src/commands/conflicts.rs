use std::{env, fs, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use clap::{ArgGroup, Args, Subcommand, ValueEnum};
use eventdbx::{
    config::{Config, load_or_default},
    conflict::ConflictKind,
    conflict_store::{
        ConflictEntry, ConflictResolution, ConflictResolutionMode, ConflictStatus, ConflictStore,
    },
    schema::SchemaManager,
    service::{AppendEventInput, CoreContext},
    store::EventStore,
    token::{IssueTokenInput, JwtLimits, ROOT_ACTION, ROOT_RESOURCE, TokenManager},
};
use serde_json::Value;

#[derive(Subcommand)]
pub enum ConflictCommands {
    /// List recorded replication conflicts
    List(ConflictListArgs),
    /// Show details for a specific conflict
    Show(ConflictShowArgs),
    /// Resolve a pending replication conflict
    Resolve(ConflictResolveArgs),
}

#[derive(Args)]
pub struct ConflictListArgs {
    /// Filter conflicts by status
    #[arg(long, value_enum)]
    pub status: Option<ConflictStatusFilter>,

    /// Maximum number of entries to display
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ConflictStatusFilter {
    Pending,
    Resolved,
}

impl From<ConflictStatusFilter> for ConflictStatus {
    fn from(value: ConflictStatusFilter) -> ConflictStatus {
        match value {
            ConflictStatusFilter::Pending => ConflictStatus::Pending,
            ConflictStatusFilter::Resolved => ConflictStatus::Resolved,
        }
    }
}

#[derive(Args)]
pub struct ConflictShowArgs {
    /// Conflict identifier
    pub id: u64,
}

#[derive(Args)]
#[command(group(ArgGroup::new("resolution").required(true).multiple(false).args(&["apply_incoming", "append_event", "patch_event", "dismiss"])))]
pub struct ConflictResolveArgs {
    /// Conflict identifier
    pub id: u64,

    /// Append the incoming conflicting payload as a new event
    #[arg(long, default_value_t = false)]
    pub apply_incoming: bool,

    /// Append a custom event type to resolve the conflict
    #[arg(long, value_name = "EVENT")]
    pub append_event: Option<String>,

    /// JSON payload for --append-event (pass inline JSON)
    #[arg(long, value_name = "JSON")]
    pub payload: Option<String>,

    /// Read payload JSON from file for --append-event
    #[arg(long, value_name = "PATH")]
    pub payload_file: Option<PathBuf>,

    /// Append an event by applying a JSON Patch document
    #[arg(long, value_name = "EVENT")]
    pub patch_event: Option<String>,

    /// JSON Patch payload for --patch-event
    #[arg(long, value_name = "JSON")]
    pub patch: Option<String>,

    /// Read patch JSON from file for --patch-event
    #[arg(long, value_name = "PATH")]
    pub patch_file: Option<PathBuf>,

    /// Mark the conflict as resolved without appending a new event
    #[arg(long, default_value_t = false)]
    pub dismiss: bool,

    /// Optional note stored alongside the resolution
    #[arg(long, value_name = "TEXT")]
    pub note: Option<String>,

    /// Optional metadata JSON when appending a custom event
    #[arg(long, value_name = "JSON")]
    pub metadata: Option<String>,
}

pub fn execute(config_path: Option<PathBuf>, command: ConflictCommands) -> Result<()> {
    match command {
        ConflictCommands::List(args) => list_conflicts(config_path, args),
        ConflictCommands::Show(args) => show_conflict(config_path, args),
        ConflictCommands::Resolve(args) => resolve_conflict(config_path, args),
    }
}

fn list_conflicts(config_path: Option<PathBuf>, args: ConflictListArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = ConflictStore::open(config.conflict_store_db_path().as_path())
        .with_context(|| "failed to open conflict store")?;
    let status = args.status.map(Into::into);
    let entries = store.list(status, Some(args.limit))?;
    if entries.is_empty() {
        println!("no conflicts recorded");
        return Ok(());
    }

    println!(
        "{:<20} {:<10} {:<20} {:<8} {:<18} {:<20}",
        "ID", "STATUS", "AGGREGATE", "VERS", "KIND", "CREATED_AT"
    );
    for entry in entries {
        let aggregate = format!("{}::{}", entry.aggregate_type, entry.aggregate_id);
        let kind = summarize_kind(&entry.kind);
        let created = entry.created_at.to_rfc3339();
        println!(
            "{:<20} {:<10} {:<20} {:<8} {:<18} {:<20}",
            entry.id,
            format_status(entry.status),
            aggregate,
            entry.incoming.version,
            kind,
            created
        );
    }
    Ok(())
}

fn show_conflict(config_path: Option<PathBuf>, args: ConflictShowArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = ConflictStore::open(config.conflict_store_db_path().as_path())
        .with_context(|| "failed to open conflict store")?;
    let Some(entry) = store.get(args.id)? else {
        bail!("conflict {} not found", args.id);
    };

    print_entry(&entry);
    Ok(())
}

fn resolve_conflict(config_path: Option<PathBuf>, args: ConflictResolveArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path.clone())?;
    let conflicts = ConflictStore::open(config.conflict_store_db_path().as_path())
        .with_context(|| "failed to open conflict store")?;
    let Some(entry) = conflicts.get(args.id)? else {
        bail!("conflict {} not found", args.id);
    };
    if entry.status == ConflictStatus::Resolved {
        bail!("conflict {} is already resolved", args.id);
    }

    if args.dismiss
        && (args.metadata.is_some()
            || args.payload.is_some()
            || args.payload_file.is_some()
            || args.patch.is_some()
            || args.patch_file.is_some())
    {
        bail!("--dismiss cannot be combined with payload, patch, or metadata options");
    }

    if args.apply_incoming
        && (args.payload.is_some()
            || args.payload_file.is_some()
            || args.patch.is_some()
            || args.patch_file.is_some())
    {
        bail!("--apply-incoming cannot be combined with custom payload or patch options");
    }

    let resolution_note = args.note.clone();

    let resolution = if args.dismiss {
        ConflictResolution {
            mode: ConflictResolutionMode::Dismissed,
            event_id: None,
            event_type: None,
            comment: resolution_note,
            resolved_at: Utc::now(),
        }
    } else if args.apply_incoming {
        let payload = entry.incoming.payload.clone();
        let note = entry
            .incoming
            .metadata
            .note
            .as_ref()
            .map(|value| value.to_string());
        let record = append_resolution_event(
            &config,
            &entry.aggregate_type,
            &entry.aggregate_id,
            &entry.incoming.event_type,
            Some(payload),
            None,
            args.metadata.as_deref(),
            note.as_deref(),
        )?;
        ConflictResolution {
            mode: ConflictResolutionMode::AppliedIncoming,
            event_id: Some(record.metadata.event_id),
            event_type: Some(record.event_type.clone()),
            comment: resolution_note,
            resolved_at: Utc::now(),
        }
    } else if let Some(event_type) = args.append_event.as_ref() {
        if args.payload.is_some() && args.payload_file.is_some() {
            bail!("--payload and --payload-file cannot be used together");
        }
        let payload = load_json_input(args.payload.as_deref(), args.payload_file.as_ref())
            .with_context(|| "payload must be provided via --payload or --payload-file")?;
        let record = append_resolution_event(
            &config,
            &entry.aggregate_type,
            &entry.aggregate_id,
            event_type,
            Some(payload),
            None,
            args.metadata.as_deref(),
            None,
        )?;
        ConflictResolution {
            mode: ConflictResolutionMode::AppendedEvent,
            event_id: Some(record.metadata.event_id),
            event_type: Some(record.event_type.clone()),
            comment: resolution_note,
            resolved_at: Utc::now(),
        }
    } else if let Some(event_type) = args.patch_event.as_ref() {
        if args.patch.is_some() && args.patch_file.is_some() {
            bail!("--patch and --patch-file cannot be used together");
        }
        let patch = load_json_input(args.patch.as_deref(), args.patch_file.as_ref()).with_context(
            || "patch must be provided via --patch or --patch-file for --patch-event",
        )?;
        let record = append_resolution_event(
            &config,
            &entry.aggregate_type,
            &entry.aggregate_id,
            event_type,
            None,
            Some(patch),
            args.metadata.as_deref(),
            None,
        )?;
        ConflictResolution {
            mode: ConflictResolutionMode::PatchedEvent,
            event_id: Some(record.metadata.event_id),
            event_type: Some(record.event_type.clone()),
            comment: resolution_note,
            resolved_at: Utc::now(),
        }
    } else {
        bail!("no resolution strategy selected");
    };

    let updated = conflicts.resolve(args.id, resolution)?;
    println!(
        "conflict {} resolved (status: {})",
        updated.id,
        format_status(updated.status)
    );
    Ok(())
}

fn append_resolution_event(
    config: &Config,
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    payload: Option<Value>,
    patch: Option<Value>,
    metadata: Option<&str>,
    note: Option<&str>,
) -> Result<eventdbx::store::EventRecord> {
    let encryptor = config.encryption_key()?;
    let store = Arc::new(EventStore::open(
        config.event_store_path(),
        encryptor.clone(),
        config.snowflake_worker_id,
    )?);
    let schemas = Arc::new(SchemaManager::load(config.schema_store_path())?);
    let tokens = Arc::new(TokenManager::load(
        config.jwt_manager_config()?,
        config.tokens_path(),
        config.jwt_revocations_path(),
        encryptor,
    )?);

    let core = CoreContext::new(
        Arc::clone(&tokens),
        schemas,
        Arc::clone(&store),
        config.restrict,
        config.list_page_size,
        config.page_limit,
    );

    let token_value = issue_resolution_token(tokens.as_ref())?;
    let metadata_value = match metadata {
        Some(raw) => Some(
            serde_json::from_str::<Value>(raw)
                .with_context(|| "failed to parse metadata JSON provided via --metadata")?,
        ),
        None => None,
    };

    let input = AppendEventInput {
        token: token_value,
        aggregate_type: aggregate_type.to_string(),
        aggregate_id: aggregate_id.to_string(),
        event_type: event_type.to_string(),
        payload,
        patch,
        metadata: metadata_value,
        note: note.map(|value| value.to_string()),
    };

    core.append_event(input)
        .with_context(|| "failed to append resolution event")
}

fn issue_resolution_token(tokens: &TokenManager) -> Result<String> {
    let subject = format!("conflict-resolver:{}", resolve_user_identity());
    let record = tokens.issue(IssueTokenInput {
        subject,
        group: "system".into(),
        user: resolve_user_identity(),
        actions: vec![ROOT_ACTION.to_string()],
        resources: vec![ROOT_RESOURCE.to_string()],
        ttl_secs: Some(300),
        not_before: None,
        issued_by: "conflict-resolver".into(),
        limits: JwtLimits::default(),
    })?;
    record
        .token
        .ok_or_else(|| anyhow!("issued conflict resolution token missing value"))
}

fn resolve_user_identity() -> String {
    env::var("USER")
        .or_else(|_| env::var("USERNAME"))
        .ok()
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(|| "local".to_string())
}

fn print_entry(entry: &ConflictEntry) {
    println!("id: {}", entry.id);
    println!(
        "aggregate: {}::{}",
        entry.aggregate_type, entry.aggregate_id
    );
    println!("status: {}", format_status(entry.status));
    println!("version: {}", entry.incoming.version);
    println!("kind: {}", summarize_kind(&entry.kind));
    println!("created_at: {}", entry.created_at.to_rfc3339());
    if let Some(remote) = entry.remote.as_ref() {
        println!("remote: {}", remote);
    }
    if let Some(note) = entry.note.as_ref() {
        println!("note: {}", note);
    }
    if let Some(existing) = entry.existing.as_ref() {
        println!("existing_event_id: {}", existing.metadata.event_id);
    }
    println!("incoming_event_id: {}", entry.incoming.metadata.event_id);
    if let Some(resolution) = entry.resolved.as_ref() {
        println!("resolved_at: {}", resolution.resolved_at.to_rfc3339());
        println!("resolution_mode: {}", summarize_mode(resolution.mode));
        if let Some(event_id) = resolution.event_id.as_ref() {
            println!("resolution_event_id: {}", event_id);
        }
        if let Some(event_type) = resolution.event_type.as_ref() {
            println!("resolution_event_type: {}", event_type);
        }
        if let Some(comment) = resolution.comment.as_ref() {
            println!("resolution_note: {}", comment);
        }
    }
}

fn format_status(status: ConflictStatus) -> &'static str {
    match status {
        ConflictStatus::Pending => "pending",
        ConflictStatus::Resolved => "resolved",
    }
}

fn summarize_kind(kind: &ConflictKind) -> String {
    match kind {
        ConflictKind::VersionMismatch { expected, found } => {
            format!("version {}!={}", expected, found)
        }
        ConflictKind::HashMismatch { expected, found } => {
            format!("hash {}!={}", expected, found)
        }
        ConflictKind::MerkleMismatch { expected, found } => {
            format!("merkle {}!={}", expected, found)
        }
    }
}

fn summarize_mode(mode: ConflictResolutionMode) -> &'static str {
    match mode {
        ConflictResolutionMode::AppliedIncoming => "applied_incoming",
        ConflictResolutionMode::AppendedEvent => "appended_event",
        ConflictResolutionMode::PatchedEvent => "patched_event",
        ConflictResolutionMode::Dismissed => "dismissed",
    }
}

fn load_json_input(inline: Option<&str>, file: Option<&PathBuf>) -> Result<Value> {
    if let Some(path) = file {
        let data = fs::read_to_string(path)
            .with_context(|| format!("failed to read payload from {}", path.display()))?;
        Ok(serde_json::from_str(&data)
            .with_context(|| format!("failed to parse JSON from {}", path.display()))?)
    } else if let Some(raw) = inline {
        Ok(serde_json::from_str(raw).with_context(|| "failed to parse JSON provided inline")?)
    } else {
        Err(anyhow!("JSON input must be provided inline or via file"))
    }
}
