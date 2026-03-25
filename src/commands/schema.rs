use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use chrono::{TimeZone, Utc};
use clap::{Args, Subcommand};
use serde_json::{self, json};

use crate::commands::{cli_token, client::ServerClient};
use eventdbx::{
    config::{Config, DEFAULT_DOMAIN_NAME, load_or_default},
    schema::{AggregateSchema, SchemaManager},
    schema_source::{CompiledSchema, compile_schema_source, load_schema_file},
};

#[derive(Subcommand)]
pub enum SchemaCommands {
    /// Validate the local schema.dbx file
    Validate(SchemaValidateArgs),
    /// Show compiled runtime JSON from the local schema.dbx file
    Show(SchemaShowArgs),
    /// Apply the local schema.dbx file to the active runtime schema store
    Apply(SchemaApplyArgs),
}

#[derive(Args, Clone, Default)]
pub struct SchemaValidateArgs {
    /// Explicit schema source path; defaults to nearest schema.dbx in the current directory tree
    #[arg(long = "file", value_name = "PATH")]
    pub file: Option<PathBuf>,

    /// Emit machine-readable JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Clone, Default)]
pub struct SchemaShowArgs {
    /// Optional aggregate name to render instead of the full compiled schema set
    #[arg(value_name = "AGGREGATE")]
    pub aggregate: Option<String>,

    /// Explicit schema source path; defaults to nearest schema.dbx in the current directory tree
    #[arg(long = "file", value_name = "PATH")]
    pub file: Option<PathBuf>,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Clone, Default)]
pub struct SchemaApplyArgs {
    /// Explicit schema source path; defaults to nearest schema.dbx in the current directory tree
    #[arg(long = "file", value_name = "PATH")]
    pub file: Option<PathBuf>,

    /// Skip daemon schema cache reload after applying
    #[arg(long = "no-reload", default_value_t = false)]
    pub no_reload: bool,

    /// Emit machine-readable JSON
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

pub fn execute(config_path: Option<PathBuf>, command: SchemaCommands) -> Result<()> {
    match command {
        SchemaCommands::Validate(args) => schema_validate(args),
        SchemaCommands::Show(args) => schema_show(args),
        SchemaCommands::Apply(args) => schema_apply(config_path, args),
    }
}

fn schema_validate(args: SchemaValidateArgs) -> Result<()> {
    match load_compiled_schema(args.file.as_deref(), None) {
        Ok((path, compiled)) => {
            let aggregates = compiled.aggregate_names();
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "valid": true,
                        "file": path,
                        "aggregate_count": aggregates.len(),
                        "aggregates": aggregates,
                    }))?
                );
            } else {
                println!(
                    "schema={} aggregate_count={} validation=ok",
                    path.display(),
                    aggregates.len()
                );
            }
            Ok(())
        }
        Err(err) => {
            if args.json {
                let file = args.file.as_ref().map(|path| path.display().to_string());
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "valid": false,
                        "file": file,
                        "error": err.to_string(),
                    }))?
                );
            }
            Err(err)
        }
    }
}

fn schema_show(args: SchemaShowArgs) -> Result<()> {
    let (_, compiled) = load_compiled_schema(args.file.as_deref(), None)?;
    let compiled = normalize_preview_schema(compiled);
    if let Some(aggregate) = args.aggregate.as_deref() {
        let schema = compiled.schemas.get(aggregate).ok_or_else(|| {
            anyhow!(
                "aggregate {} is not defined in the schema source",
                aggregate
            )
        })?;
        println!("{}", serde_json::to_string_pretty(schema)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&compiled.schemas)?);
    }
    Ok(())
}

fn schema_apply(config_path: Option<PathBuf>, args: SchemaApplyArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let tenant = DEFAULT_DOMAIN_NAME.to_string();
    let manager = SchemaManager::load(config.schema_store_path_for(&tenant))?;
    let existing = manager.snapshot();

    let (path, compiled) = load_compiled_schema(args.file.as_deref(), Some(&existing))?;
    let aggregates = compiled.aggregate_names();
    let aggregate_count = aggregates.len();
    manager.replace_all(compiled.schemas)?;

    let reloaded = if args.no_reload {
        false
    } else {
        schema_reload_online(&config, &tenant)?
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "tenant": tenant,
                "file": path,
                "aggregate_count": aggregate_count,
                "aggregates": aggregates,
                "reloaded": reloaded,
                "reload_skipped": args.no_reload,
            }))?
        );
        return Ok(());
    }

    println!(
        "tenant={} schema={} applied aggregates={}",
        tenant,
        path.display(),
        aggregate_count
    );
    if args.no_reload {
        println!("tenant={} daemon reload skipped (--no-reload)", tenant);
    } else {
        report_schema_reload(&tenant, reloaded);
    }
    Ok(())
}

fn load_compiled_schema(
    explicit_file: Option<&Path>,
    existing: Option<&std::collections::BTreeMap<String, eventdbx::schema::AggregateSchema>>,
) -> Result<(PathBuf, CompiledSchema)> {
    let (path, contents) = load_schema_file(explicit_file)?;
    let compiled = compile_schema_source(&contents, existing)?;
    Ok((path, compiled))
}

fn normalize_preview_schema(mut compiled: CompiledSchema) -> CompiledSchema {
    let preview_timestamp = Utc.timestamp_opt(0, 0).single().expect("unix epoch");
    for schema in compiled.schemas.values_mut() {
        normalize_preview_timestamps(schema, preview_timestamp);
    }
    compiled
}

fn normalize_preview_timestamps(schema: &mut AggregateSchema, timestamp: chrono::DateTime<Utc>) {
    schema.created_at = timestamp;
    schema.updated_at = timestamp;
}

fn schema_reload_online(config: &Config, tenant: &str) -> Result<bool> {
    if let Ok(token) = cli_token::ensure_bootstrap_token(config, None) {
        if let Ok(client) = ServerClient::new(config) {
            let client = client.with_tenant(Some(tenant.to_string()));
            if let Ok(reloaded) = client.reload_tenant(&token, tenant) {
                return Ok(reloaded);
            }
        }
    }
    Ok(false)
}

fn report_schema_reload(tenant: &str, reloaded: bool) {
    if reloaded {
        println!("tenant={} schema cache reloaded", tenant);
    } else {
        println!("tenant={} schema cache reload skipped", tenant);
    }
}
