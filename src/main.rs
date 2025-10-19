mod commands;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::commands::{
    aggregate::AggregateCommands,
    config::ConfigArgs,
    plugin::PluginCommands,
    remote::RemoteCommands,
    schema::SchemaCommands,
    start::{DestroyArgs, StartArgs},
    system::{BackupArgs, RestoreArgs},
    token::TokenCommands,
};

#[derive(Parser)]
#[command(author, version, about = "EventDBX server CLI")]
struct Cli {
    /// Path to the configuration file. Defaults to ~/.config/eventdbx/config.toml
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
    /// Manage plugins
    Plugin {
        #[command(subcommand)]
        command: PluginCommands,
    },
    /// Manage aggregates
    Aggregate {
        #[command(subcommand)]
        command: AggregateCommands,
    },
    /// Manage replication remotes
    Remote {
        #[command(subcommand)]
        command: RemoteCommands,
    },
    /// Create a backup archive containing all EventDBX data
    Backup(BackupArgs),
    /// Restore EventDBX data from a backup archive
    Restore(RestoreArgs),
    /// Internal command used for daemonized server execution
    #[command(name = "__internal:server", hide = true)]
    InternalServer,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

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
        Commands::Plugin { command } => commands::plugin::execute(config, command)?,
        Commands::Aggregate { command } => commands::aggregate::execute(config, command)?,
        Commands::Remote { command } => commands::remote::execute(config, command).await?,
        Commands::Backup(args) => commands::system::backup(config, args)?,
        Commands::Restore(args) => commands::system::restore(config, args)?,
        Commands::InternalServer => commands::start::run_internal(config).await?,
    }

    Ok(())
}

async fn restart(config: Option<PathBuf>, args: StartArgs) -> Result<()> {
    if let Err(err) = commands::start::stop(config.clone()) {
        tracing::warn!("failed to stop EventDBX server before restart: {err}");
    }
    commands::start::execute(config, args).await
}
