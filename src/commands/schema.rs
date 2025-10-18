use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser as ClapParser, Subcommand};

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
    /// Alter an existing schema definition
    Alter(SchemaAlterArgs),
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
pub struct SchemaAlterArgs {
    /// Aggregate name
    pub aggregate: String,

    /// Optional event name to alter
    pub event: Option<String>,

    /// Set the snapshot threshold
    #[arg(long)]
    pub snapshot_threshold: Option<u64>,

    /// Lock or unlock the aggregate (or field when --field is specified)
    #[arg(long)]
    pub lock: Option<bool>,

    /// Field name to lock/unlock
    #[arg(long)]
    pub field: Option<String>,

    /// Add fields to an event definition
    #[arg(long, short = 'a', value_delimiter = ',')]
    pub add: Vec<String>,

    /// Remove fields from an event definition
    #[arg(long, short = 'r', value_delimiter = ',')]
    pub remove: Vec<String>,
}

#[derive(Args)]
pub struct SchemaRemoveEventArgs {
    /// Aggregate name
    pub aggregate: String,

    /// Event name to remove
    pub event: String,
}

#[derive(ClapParser, Default, Debug)]
struct SchemaInlineArgs {
    /// Event name to target when adding/removing fields
    #[arg(long)]
    event: Option<String>,

    /// Add fields to the event definition (comma separated)
    #[arg(long, value_delimiter = ',', default_value = "")]
    add: Vec<String>,

    /// Remove fields from the event definition (comma separated)
    #[arg(long, value_delimiter = ',', default_value = "")]
    remove: Vec<String>,

    /// Lock or unlock the aggregate (or specific field when --field is set)
    #[arg(long)]
    lock: Option<bool>,

    /// Field name to lock/unlock
    #[arg(long)]
    field: Option<String>,

    /// Snapshot threshold to set
    #[arg(long)]
    snapshot_threshold: Option<u64>,
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
        SchemaCommands::Alter(args) => {
            apply_schema_update(&manager, args)?;
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
            [aggregate, rest @ ..] => {
                let mut argv = vec!["schema-inline".to_string()];
                argv.extend(rest.iter().cloned());
                let inline = SchemaInlineArgs::try_parse_from(argv.iter().map(String::as_str))?;
                let alter_args = SchemaAlterArgs {
                    aggregate: aggregate.clone(),
                    event: inline.event,
                    snapshot_threshold: inline.snapshot_threshold,
                    lock: inline.lock,
                    field: inline.field,
                    add: inline.add,
                    remove: inline.remove,
                };
                apply_schema_update(&manager, alter_args)?;
            }
        },
    }

    Ok(())
}

fn apply_schema_update(manager: &SchemaManager, args: SchemaAlterArgs) -> Result<()> {
    let mut update = SchemaUpdate::default();

    if let Some(value) = args.snapshot_threshold {
        update.snapshot_threshold = Some(Some(value));
    }

    if args.event.is_none() && args.field.is_none() {
        if let Some(lock) = args.lock {
            update.locked = Some(lock);
        }
    }

    if let Some(field) = args.field.clone() {
        if let Some(lock) = args.lock {
            update.field_lock = Some((field, lock));
        } else {
            return Err(anyhow::anyhow!(
                "--lock must be provided when using --field"
            ));
        }
    }

    if let Some(event) = args.event.clone() {
        if !args.add.is_empty() {
            update
                .event_add_fields
                .entry(event.clone())
                .or_default()
                .extend(args.add.clone());
        }
        if !args.remove.is_empty() {
            update
                .event_remove_fields
                .insert(event.clone(), args.remove.clone());
        }
        if args.add.is_empty() && args.remove.is_empty() {
            return Err(anyhow::anyhow!(
                "provide --add or --remove when specifying an event"
            ));
        }
    } else if (!args.add.is_empty() || !args.remove.is_empty()) && args.event.is_none() {
        return Err(anyhow::anyhow!(
            "--event must be provided when adding or removing fields"
        ));
    }

    let schema = manager.update(&args.aggregate, update)?;
    println!(
        "schema={} updated_at={} version_events={}",
        schema.aggregate,
        schema.updated_at.to_rfc3339(),
        schema.events.len()
    );
    Ok(())
}
