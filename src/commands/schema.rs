use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};

use eventdbx::{
    config::{ConfigUpdate, load_or_default},
    schema::{CreateSchemaInput, SchemaManager, SchemaUpdate},
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
    /// List available schemas
    List(SchemaListArgs),
    /// Hide a field from aggregate detail responses
    Hide(SchemaHideArgs),
    /// Fallback handler for positional aggregate commands
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[derive(Args)]
pub struct SchemaCreateArgs {
    #[arg(long)]
    pub aggregate: String,

    #[arg(long, value_delimiter = ',')]
    pub events: Vec<String>,

    #[arg(long)]
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
pub struct SchemaHideArgs {
    /// Aggregate name
    #[arg(long)]
    pub aggregate: String,

    /// Field/property name to hide
    #[arg(long)]
    pub field: String,
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

fn hide_field(config_path: Option<PathBuf>, args: SchemaHideArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
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

    let mut hidden_fields = config.hidden_fields.clone();
    let entry = hidden_fields.entry(aggregate.to_string()).or_default();
    if !entry.iter().any(|existing| existing == field) {
        entry.push(field.to_string());
        entry.sort();
        entry.dedup();
    }

    config.apply_update(ConfigUpdate {
        hidden_fields: Some(hidden_fields),
        ..ConfigUpdate::default()
    });
    config.save(&path)?;

    println!("aggregate={} field={} hidden", aggregate, field);
    Ok(())
}
