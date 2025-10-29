mod commands;
mod logging;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use eventdbx::observability;

use crate::commands::{
    aggregate::AggregateCommands,
    config::ConfigArgs,
    domain::{DomainCheckoutArgs, DomainMergeArgs},
    list::ListArgs,
    plugin::{PluginCommands, PluginWorkerArgs},
    queue::QueueArgs,
    remote::RemoteCommands,
    schema::SchemaCommands,
    start::{DestroyArgs, StartArgs},
    system::{BackupArgs, RestoreArgs},
    token::TokenCommands,
    upgrade::UpgradeArgs,
};

#[derive(Parser)]
#[command(author, version, about = "EventDBX server CLI")]
struct Cli {
    /// Path to the configuration file. Defaults to ~/.eventdbx/config.toml
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List domains with aggregate and event counts
    List(ListArgs),
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
    /// Switch the active domain context
    Checkout(DomainCheckoutArgs),
    /// Merge data from one domain into another
    Merge(DomainMergeArgs),
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
    /// List events in the store or inspect a specific event
    Events(commands::events::EventsArgs),
    /// Show or manage the plugin retry queue
    Queue(QueueArgs),
    /// Manage aggregates
    Aggregate {
        #[command(subcommand)]
        command: AggregateCommands,
    },
    /// Push events to a remote standby
    Push(commands::remote::RemotePushArgs),
    /// Pull events from a remote primary
    Pull(commands::remote::RemotePullArgs),
    /// Upgrade or switch the EventDBX CLI binary
    Upgrade(UpgradeArgs),
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
    #[command(name = "__internal:plugin-worker", hide = true)]
    InternalPluginWorker(PluginWorkerArgs),
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::init()?;
    observability::init()?;

    let Cli { config, command } = Cli::parse();

    if !matches!(&command, Commands::Upgrade(_)) {
        if let Err(err) = commands::upgrade::maybe_print_upgrade_notice().await {
            tracing::debug!("upgrade notice check failed: {err:?}");
        }
    }

    match command {
        Commands::List(args) => commands::list::execute(config, args)?,
        Commands::Start(args) => commands::start::execute(config, args).await?,
        Commands::Stop => commands::start::stop(config)?,
        Commands::Status => commands::start::status(config)?,
        Commands::Restart(args) => restart(config, args).await?,
        Commands::Destroy(args) => commands::start::destroy(config, args)?,
        Commands::Checkout(args) => commands::domain::checkout(config, args)?,
        Commands::Merge(args) => commands::domain::merge(config, args)?,
        Commands::Config(args) => commands::config::execute(config, args)?,
        Commands::Token { command } => commands::token::execute(config, command)?,
        Commands::Schema { command } => commands::schema::execute(config, command)?,
        Commands::Plugin { command } => commands::plugin::execute(config, command)?,
        Commands::Events(args) => commands::events::list(config, args)?,
        Commands::Queue(args) => commands::queue::execute(config, args)?,
        Commands::Aggregate { command } => commands::aggregate::execute(config, command)?,
        Commands::Push(args) => commands::remote::push(config, args).await?,
        Commands::Pull(args) => commands::remote::pull(config, args).await?,
        Commands::Upgrade(args) => commands::upgrade::execute(args).await?,
        Commands::Remote { command } => commands::remote::execute(config, command).await?,
        Commands::Backup(args) => commands::system::backup(config, args)?,
        Commands::Restore(args) => commands::system::restore(config, args)?,
        Commands::InternalServer => commands::start::run_internal(config).await?,
        Commands::InternalPluginWorker(args) => {
            commands::plugin::run_plugin_worker(config, args).await?
        }
        Commands::External(argv) => {
            if !commands::upgrade::try_handle_shortcut(&argv).await? {
                if let Some(name) = argv.first() {
                    anyhow::bail!("unknown command '{}'", name);
                } else {
                    anyhow::bail!("unknown command");
                }
            }
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
