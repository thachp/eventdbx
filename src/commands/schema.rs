use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};

use eventdbx::{
    config::load_or_default,
    schema::{CreateSchemaInput, MAX_EVENT_NOTE_LENGTH, SchemaManager, SchemaUpdate},
};
use serde_json;

#[derive(Subcommand)]
pub enum SchemaCommands {
    /// Create a new schema definition
    Create(SchemaCreateArgs),
    /// Add events to an existing schema definition
    Add(SchemaAddEventArgs),
    /// Remove an event definition from an aggregate
    Remove(SchemaRemoveEventArgs),
    /// Set or clear the default note for an event
    Annotate(SchemaAnnotateArgs),
    /// List available schemas
    List(SchemaListArgs),
    /// Hide a field from aggregate detail responses
    Hide(SchemaHideArgs),
    /// Validate payload against a schema definition
    Validate(SchemaValidateArgs),
    /// Fallback handler for positional aggregate commands
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[derive(Args)]
pub struct SchemaCreateArgs {
    /// Aggregate name to initialize
    #[arg(value_name = "AGGREGATE")]
    pub aggregate: String,

    /// Comma-delimited list of events to seed the schema
    #[arg(short, long, required = true, value_delimiter = ',')]
    pub events: Vec<String>,

    /// Optional override for the snapshot threshold
    #[arg(short, long)]
    pub snapshot_threshold: Option<u64>,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Default)]
pub struct SchemaListArgs {
    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args)]
pub struct SchemaAddEventArgs {
    /// Aggregate name to modify
    pub aggregate: String,

    /// Event names to add (comma-delimited or repeated)
    #[arg(required = true, value_delimiter = ',')]
    pub events: Vec<String>,
}

#[derive(Args)]
pub struct SchemaRemoveEventArgs {
    /// Aggregate name
    pub aggregate: String,

    /// Event name to remove
    pub event: String,
}

#[derive(Args)]
pub struct SchemaAnnotateArgs {
    /// Aggregate name to modify
    pub aggregate: String,

    /// Event name to annotate
    pub event: String,

    /// Note text to set (omit to use --clear)
    #[arg(long, value_name = "NOTE")]
    pub note: Option<String>,

    /// Clear the existing note for the event
    #[arg(long, default_value_t = false)]
    pub clear: bool,
}

#[derive(Args)]
pub struct SchemaHideArgs {
    /// Aggregate name
    #[arg(long)]
    pub aggregate: String,

    /// Field/property name to hide
    #[arg(long)]
    pub field: String,
}

#[derive(Args)]
pub struct SchemaValidateArgs {
    /// Aggregate name to validate against
    #[arg(long)]
    pub aggregate: String,

    /// Event name to validate against
    #[arg(long)]
    pub event: String,

    /// JSON payload to validate
    #[arg(long)]
    pub payload: String,
}

pub fn execute(config_path: Option<PathBuf>, command: SchemaCommands) -> Result<()> {
    match command {
        SchemaCommands::Hide(args) => hide_field(config_path, args),
        SchemaCommands::Create(args) => {
            let (config, _) = load_or_default(config_path)?;
            let manager = SchemaManager::load(config.schema_store_path())?;
            let schema = manager.create(CreateSchemaInput {
                aggregate: args.aggregate,
                events: args.events,
                snapshot_threshold: args.snapshot_threshold.or(config.snapshot_threshold),
            })?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&schema)?);
            } else {
                println!(
                    "schema={} events={} snapshot_threshold={:?}",
                    schema.aggregate,
                    schema.events.len(),
                    schema.snapshot_threshold
                );
            }
            Ok(())
        }
        SchemaCommands::Add(args) => {
            let (config, _) = load_or_default(config_path)?;
            let manager = SchemaManager::load(config.schema_store_path())?;
            manager.get(&args.aggregate)?;
            let mut update = SchemaUpdate::default();
            for event in &args.events {
                update.event_add_fields.entry(event.clone()).or_default();
            }
            let schema = manager.update(&args.aggregate, update)?;
            println!(
                "schema={} added_events={} total_events={}",
                schema.aggregate,
                args.events.join(","),
                schema.events.len()
            );
            Ok(())
        }
        SchemaCommands::Remove(args) => {
            let (config, _) = load_or_default(config_path)?;
            let manager = SchemaManager::load(config.schema_store_path())?;
            let schema = manager.remove_event(&args.aggregate, &args.event)?;
            println!(
                "schema={} removed_event={} remaining_events={}",
                schema.aggregate,
                args.event,
                schema.events.len()
            );
            Ok(())
        }
        SchemaCommands::Annotate(args) => annotate_event(config_path, args),
        SchemaCommands::List(args) => {
            let (config, _) = load_or_default(config_path)?;
            let manager = SchemaManager::load(config.schema_store_path())?;
            let schemas = manager.list();
            if args.json {
                println!("{}", serde_json::to_string_pretty(&schemas)?);
            } else {
                for schema in schemas {
                    println!(
                        "schema={} events={} locked={} snapshot_threshold={:?}",
                        schema.aggregate,
                        schema.events.len(),
                        schema.locked,
                        schema.snapshot_threshold
                    );
                }
            }
            Ok(())
        }
        SchemaCommands::Validate(args) => {
            let (config, _) = load_or_default(config_path)?;
            let manager = SchemaManager::load(config.schema_store_path())?;
            let payload: serde_json::Value = serde_json::from_str(&args.payload)?;
            manager.validate_event(&args.aggregate, &args.event, &payload)?;
            println!(
                "aggregate={} event={} validation=ok",
                args.aggregate, args.event
            );
            Ok(())
        }
        SchemaCommands::External(args) => {
            let (config, _) = load_or_default(config_path)?;
            let manager = SchemaManager::load(config.schema_store_path())?;
            match args.as_slice() {
                [] => Err(anyhow!(
                    "missing aggregate name; try `eventdbx schema <aggregate>`"
                )),
                [aggregate] => {
                    let schema = manager.get(aggregate)?;
                    println!("{}", serde_json::to_string_pretty(&schema)?);
                    Ok(())
                }
                [command, aggregate] if command.eq_ignore_ascii_case("show") => {
                    let schema = manager.get(aggregate)?;
                    println!("{}", serde_json::to_string_pretty(&schema)?);
                    Ok(())
                }
                _ => Err(anyhow!(
                    "unsupported schema command; available subcommands are create, add, remove, list"
                )),
            }
        }
    }
}

fn annotate_event(config_path: Option<PathBuf>, args: SchemaAnnotateArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = SchemaManager::load(config.schema_store_path())?;

    let aggregate = args.aggregate.trim();
    if aggregate.is_empty() {
        return Err(anyhow!("aggregate name cannot be empty"));
    }

    let event = args.event.trim();
    if event.is_empty() {
        return Err(anyhow!("event name cannot be empty"));
    }

    if args.clear && args.note.is_some() {
        return Err(anyhow!("--note cannot be combined with --clear"));
    }

    let desired_note = if args.clear {
        None
    } else if let Some(note) = args.note {
        if note.chars().count() > MAX_EVENT_NOTE_LENGTH {
            return Err(anyhow!(
                "note cannot exceed {} characters",
                MAX_EVENT_NOTE_LENGTH
            ));
        }
        Some(note)
    } else {
        return Err(anyhow!("either --note must be provided or use --clear"));
    };

    let schema = manager.get(aggregate)?;
    if !schema.events.contains_key(event) {
        return Err(anyhow!(
            "event {} is not defined for aggregate {}",
            event,
            aggregate
        ));
    }

    let mut update = SchemaUpdate::default();
    update
        .event_notes
        .insert(event.to_string(), desired_note.clone());
    manager.update(aggregate, update)?;

    match desired_note {
        Some(note) => println!(
            "aggregate={} event={} note_set=\"{}\"",
            aggregate, event, note
        ),
        None => println!("aggregate={} event={} note_cleared", aggregate, event),
    }

    Ok(())
}

fn hide_field(config_path: Option<PathBuf>, args: SchemaHideArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = SchemaManager::load(config.schema_store_path())?;

    let aggregate = args.aggregate.trim();
    if aggregate.is_empty() {
        return Err(anyhow!("aggregate name cannot be empty"));
    }
    let field = args.field.trim();
    if field.is_empty() {
        return Err(anyhow!("field name cannot be empty"));
    }

    // Ensure aggregate exists (best-effort)
    if manager.get(aggregate).is_err() {
        tracing::warn!(
            "hiding field `{}` for unknown aggregate `{}`; field visibility will still be enforced",
            field,
            aggregate
        );
    }

    let mut update = SchemaUpdate::default();
    update.hidden_field = Some((field.to_string(), true));
    manager.update(aggregate, update)?;

    println!("aggregate={} field={} hidden", aggregate, field);
    Ok(())
}
