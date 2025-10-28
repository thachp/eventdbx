use std::{path::PathBuf, str::FromStr};

use anyhow::{Context, Result, anyhow};
use clap::Args;
use eventdbx::{
    config::load_or_default,
    filter,
    snowflake::SnowflakeId,
    store::{EventArchiveScope, EventQueryScope, EventSort, EventSortField, EventStore},
};

#[derive(Args)]
pub struct EventsArgs {
    /// Optional aggregate type to scope results
    #[arg(value_name = "AGGREGATE")]
    pub aggregate: Option<String>,

    /// Optional aggregate identifier to scope results
    #[arg(value_name = "AGGREGATE_ID")]
    pub aggregate_id: Option<String>,

    /// Number of events to skip
    #[arg(long, default_value_t = 0)]
    pub skip: usize,

    /// Maximum number of events to return
    #[arg(long)]
    pub take: Option<usize>,

    /// Filter events using a SQL-like expression (e.g. `event_type = "order_created"`)
    #[arg(long)]
    pub filter: Option<String>,

    /// Sort events by comma-separated fields (e.g. `created_at:desc,aggregate_id`)
    #[arg(long, value_name = "FIELD[:ORDER][,...]")]
    pub sort: Option<String>,

    /// Emit results as JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,

    /// Include archived events alongside active ones
    #[arg(long, default_value_t = false, conflicts_with = "archived_only")]
    pub include_archived: bool,

    /// Show only archived events
    #[arg(long, default_value_t = false)]
    pub archived_only: bool,
}

#[derive(Args)]
pub struct EventArgs {
    /// Snowflake event identifier
    pub event_id: String,

    /// Emit the event as pretty-printed JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

pub fn list(config_path: Option<PathBuf>, args: EventsArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;

    let filter_expr = if let Some(raw) = args.filter.as_ref() {
        Some(
            filter::parse_shorthand(raw)
                .with_context(|| format!("invalid filter expression: {raw}"))?,
        )
    } else {
        None
    };

    let sort_directives = if let Some(spec) = args.sort.as_deref() {
        Some(
            parse_event_sort_directives(spec)
                .map_err(|err| anyhow!("invalid sort specification: {err}"))?,
        )
    } else {
        None
    };

    let archive_scope = if args.archived_only {
        EventArchiveScope::ArchivedOnly
    } else if args.include_archived {
        EventArchiveScope::IncludeArchived
    } else {
        EventArchiveScope::ActiveOnly
    };

    let take = args.take.or(Some(config.list_page_size));

    let scope = match (&args.aggregate, &args.aggregate_id) {
        (Some(aggregate), Some(aggregate_id)) => EventQueryScope::Aggregate {
            aggregate_type: aggregate.as_str(),
            aggregate_id: aggregate_id.as_str(),
        },
        (Some(aggregate), None) => EventQueryScope::AggregateType(aggregate.as_str()),
        (None, _) => EventQueryScope::All,
    };

    let events = store.events_paginated(
        scope,
        archive_scope,
        args.skip,
        take,
        sort_directives
            .as_ref()
            .map(|directives| directives.as_slice()),
        filter_expr.as_ref(),
    )?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&events)?);
        return Ok(());
    }

    if events.is_empty() {
        println!("no events");
        return Ok(());
    }

    for record in events {
        let issued = record
            .metadata
            .issued_by
            .as_ref()
            .map(|claims| format!("{}:{}", claims.group, claims.user))
            .unwrap_or_else(|| "-".to_string());
        let note = record.metadata.note.as_deref().unwrap_or("-");
        println!(
            "aggregate_type={} aggregate_id={} version={} event_type={} event_id={} created_at={} issued_by={} note={}",
            record.aggregate_type,
            record.aggregate_id,
            record.version,
            record.event_type,
            record.metadata.event_id,
            record.metadata.created_at.to_rfc3339(),
            issued,
            note,
        );
    }

    Ok(())
}

pub fn show(config_path: Option<PathBuf>, args: EventArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let store = EventStore::open_read_only(config.event_store_path(), config.encryption_key()?)?;

    let event_id = args
        .event_id
        .parse::<SnowflakeId>()
        .with_context(|| format!("invalid snowflake id '{}'", args.event_id))?;

    let Some(event) = store.find_event_by_id(event_id)? else {
        anyhow::bail!("event {} not found", args.event_id);
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&event)?);
        return Ok(());
    }

    println!("aggregate_type={}", event.aggregate_type);
    println!("aggregate_id={}", event.aggregate_id);
    println!("version={}", event.version);
    println!("event_type={}", event.event_type);
    println!("event_id={}", event.metadata.event_id);
    println!("created_at={}", event.metadata.created_at.to_rfc3339());
    if let Some(claims) = event.metadata.issued_by.as_ref() {
        println!("issued_by={}:{}", claims.group, claims.user);
    }
    if let Some(note) = event.metadata.note.as_ref() {
        println!("note={}", note);
    }
    println!("hash={}", event.hash);
    println!("merkle_root={}", event.merkle_root);

    println!("payload={}", serde_json::to_string_pretty(&event.payload)?);

    if let Some(extensions) = event.extensions.as_ref() {
        println!("extensions={}", serde_json::to_string_pretty(extensions)?);
    }

    Ok(())
}

fn parse_event_sort_directives(raw: &str) -> Result<Vec<EventSort>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("sort specification cannot be empty".to_string());
    }

    let mut directives = Vec::new();
    for segment in trimmed.split(',') {
        let spec = segment.trim();
        if spec.is_empty() {
            return Err("sort segments cannot be empty".to_string());
        }
        directives.push(parse_event_sort(spec)?);
    }

    Ok(directives)
}

fn parse_event_sort(spec: &str) -> Result<EventSort, String> {
    let mut parts = spec.split(':');
    let field_str = parts
        .next()
        .ok_or_else(|| "missing sort field".to_string())?
        .trim();

    if field_str.is_empty() {
        return Err("sort field cannot be empty".to_string());
    }

    let field = EventSortField::from_str(field_str)?;
    let descending = match parts.next() {
        Some(order) => match order.trim().to_ascii_lowercase().as_str() {
            "asc" => false,
            "desc" => true,
            other => {
                return Err(format!(
                    "invalid sort order '{other}' (expected 'asc' or 'desc')"
                ));
            }
        },
        None => false,
    };

    if parts.next().is_some() {
        return Err("sort specification contains too many ':' separators".to_string());
    }

    Ok(EventSort { field, descending })
}
