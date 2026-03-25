mod commands;
mod logging;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use eventdbx::observability;

use crate::commands::{
    aggregate::AggregateCommands, config::ConfigArgs, init::InitArgs, schema::SchemaCommands,
    serve::ServeCommands, token::TokenCommands,
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
    /// Path to the configuration file. Defaults to the nearest .dbx/config.toml in the current directory tree
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a local EventDBX workspace
    Init(InitArgs),
    /// Manage EventDBX server lifecycle
    Serve {
        #[command(subcommand)]
        command: ServeCommands,
    },
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
        Commands::Init(args) => commands::init::execute(config, args)?,
        Commands::Serve { command } => commands::serve::execute(config, command).await?,
        Commands::Config(args) => commands::config::execute(config, args)?,
        Commands::Token { command } => commands::token::execute(config, command)?,
        Commands::Schema { command } => commands::schema::execute(config, command)?,
        Commands::Events(args) => commands::events::list(config, args)?,
        Commands::Aggregate { command } => commands::aggregate::execute(config, command)?,
        Commands::InternalServer => commands::serve::run_internal(config).await?,
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
