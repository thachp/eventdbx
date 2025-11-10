use std::{fs, path::PathBuf, str::FromStr};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand, ValueEnum};

use crate::commands::schema_version::{
    SchemaActivateArgs, SchemaDiffArgs, SchemaHistoryArgs, SchemaPublishArgs, SchemaReloadArgs,
    SchemaRollbackArgs, SchemaShowArgs, schema_activate, schema_diff, schema_history,
    schema_publish, schema_reload, schema_rollback, schema_show,
};
use eventdbx::{
    config::load_or_default,
    schema::{
        ColumnType, CreateSchemaInput, FieldFormat, FieldRules, MAX_EVENT_NOTE_LENGTH,
        SchemaManager, SchemaUpdate,
    },
    schema_history::SchemaAuditAction,
};
use serde::de::DeserializeOwned;
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
    /// Modify column types and validation rules for a field
    Field(SchemaFieldArgs),
    /// Validate payload against a schema definition
    Validate(SchemaValidateArgs),
    /// Publish a tenant-level schema snapshot
    Publish(SchemaPublishArgs),
    /// Show schema history for a tenant
    History(SchemaHistoryArgs),
    /// Show a specific schema version
    Show(SchemaShowArgs),
    /// Diff two schema versions
    Diff(SchemaDiffArgs),
    /// Activate a schema version
    Activate(SchemaActivateArgs),
    /// Roll back to a previous schema version
    Rollback(SchemaRollbackArgs),
    /// Reload the tenant schema cache
    Reload(SchemaReloadArgs),
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
pub struct SchemaFieldArgs {
    /// Aggregate name to modify
    #[arg(value_name = "AGGREGATE")]
    pub aggregate: String,

    /// Field/property name to alter
    #[arg(value_name = "FIELD")]
    pub field: String,

    /// Column data type to enforce
    #[arg(long = "type", value_name = "TYPE", conflicts_with = "clear_type")]
    pub column_type: Option<String>,

    /// Remove the current column type and validation rules
    #[arg(long = "clear-type", default_value_t = false)]
    pub clear_type: bool,

    /// Replace the entire rules block with JSON
    #[arg(
        long = "rules",
        value_name = "JSON",
        conflicts_with_all = [
            "required",
            "optional",
            "format",
            "clear_format",
            "contains",
            "clear_contains",
            "does_not_contain",
            "clear_does_not_contain",
            "regex",
            "clear_regex",
            "length_min",
            "length_max",
            "clear_length",
            "range_min",
            "range_max",
            "clear_range",
            "properties",
            "clear_properties"
        ],
        conflicts_with = "clear_rules"
    )]
    pub rules: Option<String>,

    /// Clear all validation rules
    #[arg(long = "clear-rules", default_value_t = false)]
    pub clear_rules: bool,

    /// Require the field to be present in payloads
    #[arg(long, conflicts_with = "optional")]
    pub required: bool,

    /// Mark the field as optional
    #[arg(long = "not-required", conflicts_with = "required")]
    pub optional: bool,

    /// Built-in format validation to apply
    #[arg(long = "format", value_enum, value_name = "FORMAT")]
    pub format: Option<FieldFormatArg>,

    /// Remove any existing format rule
    #[arg(long = "clear-format", default_value_t = false)]
    pub clear_format: bool,

    /// Replace the allowed substring list
    #[arg(long = "contains", value_name = "TEXT", value_delimiter = ',')]
    pub contains: Option<Vec<String>>,

    /// Clear all `contains` checks
    #[arg(long = "clear-contains", default_value_t = false)]
    pub clear_contains: bool,

    /// Replace the disallowed substring list
    #[arg(long = "does-not-contain", value_name = "TEXT", value_delimiter = ',')]
    pub does_not_contain: Option<Vec<String>>,

    /// Clear all `does_not_contain` checks
    #[arg(long = "clear-does-not-contain", default_value_t = false)]
    pub clear_does_not_contain: bool,

    /// Replace the enforced regex patterns
    #[arg(long = "regex", value_name = "PATTERN", value_delimiter = ',')]
    pub regex: Option<Vec<String>>,

    /// Remove all regex validations
    #[arg(long = "clear-regex", default_value_t = false)]
    pub clear_regex: bool,

    /// Minimum accepted length for text/binary fields
    #[arg(long = "length-min", value_name = "N")]
    pub length_min: Option<usize>,

    /// Maximum accepted length for text/binary fields
    #[arg(long = "length-max", value_name = "N")]
    pub length_max: Option<usize>,

    /// Remove any length constraint
    #[arg(long = "clear-length", default_value_t = false)]
    pub clear_length: bool,

    /// Minimum accepted value for numeric/date/timestamp fields
    #[arg(long = "range-min", value_name = "VALUE")]
    pub range_min: Option<String>,

    /// Maximum accepted value for numeric/date/timestamp fields
    #[arg(long = "range-max", value_name = "VALUE")]
    pub range_max: Option<String>,

    /// Remove range constraints
    #[arg(long = "clear-range", default_value_t = false)]
    pub clear_range: bool,

    /// Replace nested property rules for object columns
    #[arg(long = "properties", value_name = "JSON")]
    pub properties: Option<String>,

    /// Clear nested object property rules
    #[arg(long = "clear-properties", default_value_t = false)]
    pub clear_properties: bool,
}

impl SchemaFieldArgs {
    fn has_type_mutation(&self) -> bool {
        self.column_type.is_some() || self.clear_type
    }

    fn has_rule_flag_updates(&self) -> bool {
        self.required
            || self.optional
            || self.format.is_some()
            || self.clear_format
            || self.contains.is_some()
            || self.clear_contains
            || self.does_not_contain.is_some()
            || self.clear_does_not_contain
            || self.regex.is_some()
            || self.clear_regex
            || self.length_min.is_some()
            || self.length_max.is_some()
            || self.clear_length
            || self.range_min.is_some()
            || self.range_max.is_some()
            || self.clear_range
            || self.properties.is_some()
            || self.clear_properties
    }

    fn has_any_changes(&self) -> bool {
        self.has_type_mutation()
            || self.rules.is_some()
            || self.clear_rules
            || self.has_rule_flag_updates()
    }

    fn apply_rule_overrides(&self, rules: &mut FieldRules) -> Result<()> {
        if self.required {
            rules.required = true;
        }
        if self.optional {
            rules.required = false;
        }
        if let Some(format) = self.format {
            rules.format = Some(FieldFormat::from(format));
        } else if self.clear_format {
            rules.format = None;
        }
        if let Some(values) = &self.contains {
            rules.contains = clone_non_empty(values, "contains")?;
        } else if self.clear_contains {
            rules.contains.clear();
        }
        if let Some(values) = &self.does_not_contain {
            rules.does_not_contain = clone_non_empty(values, "does-not-contain")?;
        } else if self.clear_does_not_contain {
            rules.does_not_contain.clear();
        }
        if let Some(values) = &self.regex {
            rules.regex = clone_non_empty(values, "regex")?;
        } else if self.clear_regex {
            rules.regex.clear();
        }
        if self.clear_length {
            rules.length = None;
        } else if self.length_min.is_some() || self.length_max.is_some() {
            let mut length = rules.length.clone().unwrap_or_default();
            if let Some(min) = self.length_min {
                length.min = Some(min);
            }
            if let Some(max) = self.length_max {
                length.max = Some(max);
            }
            rules.length = Some(length);
        }
        if self.clear_range {
            rules.range = None;
        } else if self.range_min.is_some() || self.range_max.is_some() {
            let mut range = rules.range.clone().unwrap_or_default();
            if let Some(min) = &self.range_min {
                range.min = Some(min.clone());
            }
            if let Some(max) = &self.range_max {
                range.max = Some(max.clone());
            }
            rules.range = Some(range);
        }
        if self.clear_properties {
            rules.properties.clear();
        } else if let Some(json) = &self.properties {
            rules.properties = parse_json_input(json, "properties")?;
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum FieldFormatArg {
    #[value(name = "email")]
    Email,
    #[value(name = "url")]
    Url,
    #[value(name = "credit_card")]
    CreditCard,
    #[value(name = "country_code")]
    CountryCode,
    #[value(name = "iso_8601")]
    Iso8601,
    #[value(name = "wgs_84")]
    Wgs84,
    #[value(name = "camel_case")]
    CamelCase,
    #[value(name = "snake_case")]
    SnakeCase,
    #[value(name = "kebab_case")]
    KebabCase,
    #[value(name = "pascal_case")]
    PascalCase,
    #[value(name = "upper_case_snake_case")]
    UpperCaseSnakeCase,
}

impl From<FieldFormatArg> for FieldFormat {
    fn from(value: FieldFormatArg) -> Self {
        match value {
            FieldFormatArg::Email => FieldFormat::Email,
            FieldFormatArg::Url => FieldFormat::Url,
            FieldFormatArg::CreditCard => FieldFormat::CreditCard,
            FieldFormatArg::CountryCode => FieldFormat::CountryCode,
            FieldFormatArg::Iso8601 => FieldFormat::Iso8601,
            FieldFormatArg::Wgs84 => FieldFormat::Wgs84,
            FieldFormatArg::CamelCase => FieldFormat::CamelCase,
            FieldFormatArg::SnakeCase => FieldFormat::SnakeCase,
            FieldFormatArg::KebabCase => FieldFormat::KebabCase,
            FieldFormatArg::PascalCase => FieldFormat::PascalCase,
            FieldFormatArg::UpperCaseSnakeCase => FieldFormat::UpperCaseSnakeCase,
        }
    }
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
        SchemaCommands::Field(args) => schema_field(config_path, args),
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
        SchemaCommands::Publish(args) => schema_publish(config_path, args),
        SchemaCommands::History(args) => schema_history(config_path, args),
        SchemaCommands::Show(args) => schema_show(config_path, args),
        SchemaCommands::Diff(args) => schema_diff(config_path, args),
        SchemaCommands::Activate(args) => {
            schema_activate(config_path, args, SchemaAuditAction::Activate, "activated")
        }
        SchemaCommands::Rollback(args) => schema_rollback(config_path, args),
        SchemaCommands::Reload(args) => schema_reload(config_path, args),
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

fn schema_field(config_path: Option<PathBuf>, args: SchemaFieldArgs) -> Result<()> {
    let aggregate = args.aggregate.trim();
    if aggregate.is_empty() {
        return Err(anyhow!("aggregate name cannot be empty"));
    }

    let field = args.field.trim();
    if field.is_empty() {
        return Err(anyhow!("field name cannot be empty"));
    }

    if !args.has_any_changes() {
        return Err(anyhow!(
            "no changes requested; pass --type, --rules, or at least one rule flag"
        ));
    }

    if args.clear_type && (args.rules.is_some() || args.clear_rules || args.has_rule_flag_updates())
    {
        return Err(anyhow!(
            "--clear-type cannot be combined with rule changes; removing the type deletes all rules automatically"
        ));
    }

    let (config, _) = load_or_default(config_path)?;
    let manager = SchemaManager::load(config.schema_store_path())?;
    let schema = manager.get(aggregate)?;

    let mut update = SchemaUpdate::default();
    let mut actions = Vec::new();

    if let Some(ref value) = args.column_type {
        let column_type =
            ColumnType::from_str(value).map_err(|err| anyhow!("invalid column type: {err}"))?;
        update.column_type = Some((field.to_string(), Some(column_type.clone())));
        actions.push(format!("type={column_type}"));
    } else if args.clear_type {
        update.column_type = Some((field.to_string(), None));
        actions.push("type=cleared".to_string());
    }

    if let Some(ref rules_json) = args.rules {
        let rules: FieldRules = parse_json_input(rules_json, "rules")?;
        update.column_rules = Some((field.to_string(), Some(rules)));
        actions.push("rules=updated".to_string());
    } else if args.clear_rules {
        if schema.column_types.get(field).is_none() && args.column_type.is_none() {
            return Err(anyhow!("field {} does not have rules to clear", field));
        }
        update.column_rules = Some((field.to_string(), None));
        actions.push("rules=cleared".to_string());
    } else if args.has_rule_flag_updates() {
        let mut rules = if let Some(settings) = schema.column_types.get(field) {
            settings.rules.clone()
        } else if args.column_type.is_some() {
            FieldRules::default()
        } else {
            return Err(anyhow!(
                "field {} has no column type; set --type before editing rules",
                field
            ));
        };
        args.apply_rule_overrides(&mut rules)?;
        update.column_rules = Some((field.to_string(), Some(rules)));
        actions.push("rules=updated".to_string());
    }

    if actions.is_empty() {
        // Should be unreachable thanks to earlier guard, but double-check to avoid silent no-ops.
        return Err(anyhow!(
            "no valid updates were derived from the provided options"
        ));
    }

    manager.update(aggregate, update)?;

    println!(
        "aggregate={} field={} {}",
        aggregate,
        field,
        actions.join(" ")
    );
    Ok(())
}

fn parse_json_input<T>(input: &str, label: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let source = if let Some(path) = input.strip_prefix('@') {
        fs::read_to_string(path).with_context(|| format!("failed to read {label} from {path}"))?
    } else {
        input.to_string()
    };
    serde_json::from_str(&source)
        .with_context(|| format!("failed to parse {label} JSON: {}", input))
}

fn clone_non_empty(values: &[String], label: &str) -> Result<Vec<String>> {
    let mut items = Vec::with_capacity(values.len());
    for value in values {
        if value.trim().is_empty() {
            return Err(anyhow!("{label} entries cannot be empty"));
        }
        items.push(value.clone());
    }
    Ok(items)
}
