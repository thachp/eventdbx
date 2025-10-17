use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Args, Subcommand, ValueEnum};

use eventful::config::{
    CsvPluginConfig, PluginConfig, PluginDefinition, PluginKind, PostgresColumnConfig,
    PostgresPluginConfig, SqlitePluginConfig, load_or_default,
};

#[derive(Subcommand)]
pub enum PluginCommands {
    /// Configure the Postgres plugin
    #[command(name = "postgres")]
    PostgresConfigure(PluginPostgresConfigureArgs),
    /// Configure the SQLite plugin
    #[command(name = "sqlite")]
    SqliteConfigure(PluginSqliteConfigureArgs),
    /// Configure the CSV plugin
    #[command(name = "csv")]
    CsvConfigure(PluginCsvConfigureArgs),
    /// Configure per-plugin field mappings
    #[command(name = "map")]
    Map(PluginMapArgs),
}

#[derive(Args)]
pub struct PluginPostgresConfigureArgs {
    /// Connection string used to reach the Postgres database
    #[arg(long = "connection")]
    pub connection: String,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,
}

#[derive(Args)]
pub struct PluginSqliteConfigureArgs {
    /// Path to the SQLite database file
    #[arg(long)]
    pub path: PathBuf,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,
}

#[derive(Args)]
pub struct PluginCsvConfigureArgs {
    /// Output directory for CSV files
    #[arg(long)]
    pub output_dir: PathBuf,

    /// Disable the plugin after configuring
    #[arg(long, default_value_t = false)]
    pub disable: bool,
}

#[derive(Args)]
pub struct PluginMapArgs {
    /// Plugin identifier
    #[arg(long, value_enum)]
    pub plugin: PluginTarget,

    /// Aggregate name to configure
    #[arg(long)]
    pub aggregate: String,

    /// Field name to configure
    #[arg(long)]
    pub field: String,

    /// Data type to use for the field (e.g., VARCHAR(255))
    #[arg(long = "datatype")]
    pub data_type: String,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum PluginTarget {
    Postgres,
    Sqlite,
    Csv,
}

impl From<PluginTarget> for PluginKind {
    fn from(value: PluginTarget) -> Self {
        match value {
            PluginTarget::Postgres => PluginKind::Postgres,
            PluginTarget::Sqlite => PluginKind::Sqlite,
            PluginTarget::Csv => PluginKind::Csv,
        }
    }
}

pub fn execute(config_path: Option<PathBuf>, command: PluginCommands) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    match command {
        PluginCommands::PostgresConfigure(args) => {
            let existing_mapping = config
                .plugins
                .iter()
                .find_map(|def| match &def.config {
                    PluginConfig::Postgres(settings) => Some(settings.field_mappings.clone()),
                    _ => None,
                })
                .unwrap_or_default();

            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Postgres(PostgresPluginConfig {
                    connection_string: args.connection,
                    field_mappings: existing_mapping,
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("Postgres plugin disabled");
            } else {
                println!("Postgres plugin configured");
            }
        }
        PluginCommands::SqliteConfigure(args) => {
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Sqlite(SqlitePluginConfig {
                    path: args.path.clone(),
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("SQLite plugin disabled");
            } else {
                println!("SQLite plugin configured");
            }
        }
        PluginCommands::CsvConfigure(args) => {
            let definition = PluginDefinition {
                enabled: !args.disable,
                config: PluginConfig::Csv(CsvPluginConfig {
                    output_dir: args.output_dir.clone(),
                }),
            };
            config.set_plugin(definition);
            config.ensure_data_dir()?;
            config.save(&path)?;
            if args.disable {
                println!("CSV plugin disabled");
            } else {
                println!("CSV plugin configured");
            }
        }
        PluginCommands::Map(args) => match PluginKind::from(args.plugin) {
            PluginKind::Postgres => {
                let definition = config
                    .plugins
                    .iter_mut()
                    .find(|def| matches!(def.config, PluginConfig::Postgres(_)))
                    .ok_or_else(|| {
                        anyhow!(
                            "configure postgres plugin before mapping fields with `eventful plugin postgres --connection=...`"
                        )
                    })?;

                match &mut definition.config {
                    PluginConfig::Postgres(settings) => {
                        let mut field_config = PostgresColumnConfig::default();
                        field_config.data_type = Some(args.data_type.clone());

                        settings
                            .field_mappings
                            .entry(args.aggregate.clone())
                            .or_default()
                            .insert(args.field.clone(), field_config);

                        config.ensure_data_dir()?;
                        config.save(&path)?;

                        println!(
                            "Mapped {}.{} as {}",
                            args.aggregate, args.field, args.data_type
                        );
                    }
                    _ => {
                        return Err(anyhow!(
                            "unexpected plugin configuration variant; reconfigure the postgres plugin and try again"
                        ));
                    }
                }
            }
            PluginKind::Sqlite => {
                return Err(anyhow!(
                    "field mapping is not supported for the SQLite plugin"
                ));
            }
            PluginKind::Csv => {
                return Err(anyhow!("field mapping is not supported for the CSV plugin"));
            }
        },
    }

    Ok(())
}
