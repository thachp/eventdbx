use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Subcommand};

use eventdbx::{
    config::load_or_default,
    schema::{CreateSchemaInput, SchemaManager, SchemaUpdate},
};

#[derive(Subcommand)]
pub enum SchemaCommands {
    /// Create a new schema definition
    Create(SchemaCreateArgs),
    /// Add events to an existing schema definition
    Add(SchemaAddEventArgs),
    /// Remove an event definition from an aggregate
    Remove(SchemaRemoveEventArgs),
    /// List available schemas
    List,
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

pub fn execute(config_path: Option<PathBuf>, command: SchemaCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = SchemaManager::load(config.schema_store_path())?;

    match command {
        SchemaCommands::Create(args) => {
            let schema = manager.create(CreateSchemaInput {
                aggregate: args.aggregate,
                events: args.events,
                snapshot_threshold: args.snapshot_threshold,
            })?;
            println!(
                "schema={} events={} snapshot_threshold={:?}",
                schema.aggregate,
                schema.events.len(),
                schema.snapshot_threshold
            );
        }
        SchemaCommands::Add(args) => {
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
        }
        SchemaCommands::Remove(args) => {
            let schema = manager.remove_event(&args.aggregate, &args.event)?;
            println!(
                "schema={} removed_event={} remaining_events={}",
                schema.aggregate,
                args.event,
                schema.events.len()
            );
        }
        SchemaCommands::List => {
            for schema in manager.list() {
                println!(
                    "schema={} events={} locked={} snapshot_threshold={:?}",
                    schema.aggregate,
                    schema.events.len(),
                    schema.locked,
                    schema.snapshot_threshold
                );
            }
        }
        SchemaCommands::External(args) => match args.as_slice() {
            [] => {
                return Err(anyhow::anyhow!(
                    "missing aggregate name; try `eventdbx schema <aggregate>`"
                ));
            }
            [aggregate] => {
                let schema = manager.get(aggregate)?;
                println!("{}", serde_json::to_string_pretty(&schema)?);
            }
            [command, aggregate] if command.eq_ignore_ascii_case("show") => {
                let schema = manager.get(aggregate)?;
                println!("{}", serde_json::to_string_pretty(&schema)?);
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "unsupported schema command; available subcommands are create, add, remove, list"
                ));
            }
        },
    }

    Ok(())
}
