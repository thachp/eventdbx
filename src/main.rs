mod commands;
mod logging;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use eventdbx::observability;

use crate::commands::{
    aggregate::AggregateCommands,
    config::ConfigArgs,
    schema::SchemaCommands,
    start::{DestroyArgs, StartArgs},
    token::TokenCommands,
};

#[derive(Parser)]
#[command(
    author,
    version,
    about = "EventDBX server CLI",
    long_about = None,
    disable_version_flag = true
)]
struct Cli {
    /// Path to the configuration file. Defaults to ~/.eventdbx/config.toml
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the EventDBX server
    Start(StartArgs),
    /// Stop the EventDBX server
    Stop,
    /// Display EventDBX server status
    Status,
    /// Restart the EventDBX server
    Restart(StartArgs),
    /// Destroy all EventDBX data and configuration
    Destroy(DestroyArgs),
    /// Update system configuration
    Config(ConfigArgs),
    /// Manage access tokens
    Token {
        #[command(subcommand)]
        command: TokenCommands,
    },
    /// Manage schemas
    Schema {
        #[command(subcommand)]
        command: SchemaCommands,
    },
    /// List events in the store or inspect a specific event
    Events(commands::events::EventsArgs),
    /// Manage aggregates
    Aggregate {
        #[command(subcommand)]
        command: AggregateCommands,
    },
    /// Internal command used for daemonized server execution
    #[command(name = "__internal:server", hide = true)]
    InternalServer,
    #[command(external_subcommand)]
    External(Vec<String>),
    /// Print version information
    #[command(name = "--version", hide = true)]
    Version,
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::init()?;
    observability::init()?;

    let Cli { config, command } = Cli::parse();

    match command {
        Commands::Start(args) => commands::start::execute(config, args).await?,
        Commands::Stop => commands::start::stop(config)?,
        Commands::Status => commands::start::status(config)?,
        Commands::Restart(args) => restart(config, args).await?,
        Commands::Destroy(args) => commands::start::destroy(config, args)?,
        Commands::Config(args) => commands::config::execute(config, args)?,
        Commands::Token { command } => commands::token::execute(config, command)?,
        Commands::Schema { command } => commands::schema::execute(config, command)?,
        Commands::Events(args) => commands::events::list(config, args)?,
        Commands::Aggregate { command } => commands::aggregate::execute(config, command)?,
        Commands::InternalServer => commands::start::run_internal(config).await?,
        Commands::External(argv) => {
            if let Some(name) = argv.first() {
                anyhow::bail!("unknown command '{}'", name);
            } else {
                anyhow::bail!("unknown command");
            }
        }
        Commands::Version => {
            println!("eventdbx version {}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}

async fn restart(config: Option<PathBuf>, args: StartArgs) -> Result<()> {
    if let Err(err) = commands::start::stop(config.clone()) {
        tracing::warn!("failed to stop EventDBX server before restart: {err}");
    }
    commands::start::execute(config, args).await
}
