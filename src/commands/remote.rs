use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use chrono::Utc;
use clap::{Args, Subcommand};

use eventdbx::config::{RemoteConfig, load_or_default};

#[derive(Subcommand)]
pub enum RemoteCommands {
    /// Add a replication remote
    Add(RemoteAddArgs),
    /// Remove a replication remote
    #[command(name = "rm")]
    Remove(RemoteRemoveArgs),
    /// List configured remotes
    #[command(name = "ls")]
    List,
    /// Show details for a remote
    Show(RemoteShowArgs),
    /// Display this node's replication public key
    Key(RemoteKeyArgs),
}

#[derive(Args)]
pub struct RemoteAddArgs {
    /// Remote alias (e.g. standby1)
    pub name: String,
    /// Remote gRPC endpoint (e.g. grpc://10.0.0.1:7443)
    pub endpoint: String,
    /// Base64-encoded Ed25519 public key for the remote
    #[arg(long = "public-key")]
    pub public_key: String,
    /// Replace existing remote with the same name
    #[arg(long, default_value_t = false)]
    pub replace: bool,
}

#[derive(Args)]
pub struct RemoteRemoveArgs {
    /// Remote alias to remove
    pub name: String,
}

#[derive(Args)]
pub struct RemoteShowArgs {
    /// Remote alias to display
    pub name: String,
}

#[derive(Args, Default)]
pub struct RemoteKeyArgs {
    /// Also print the path to the public key file
    #[arg(long, default_value_t = false)]
    pub show_path: bool,
}

pub fn execute(config_path: Option<PathBuf>, command: RemoteCommands) -> Result<()> {
    match command {
        RemoteCommands::Add(args) => add_remote(config_path, args),
        RemoteCommands::Remove(args) => remove_remote(config_path, args),
        RemoteCommands::List => list_remotes(config_path),
        RemoteCommands::Show(args) => show_remote(config_path, args),
        RemoteCommands::Key(args) => show_local_key(config_path, args),
    }
}

fn add_remote(config_path: Option<PathBuf>, args: RemoteAddArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;

    let name = args.name.trim();
    if name.is_empty() {
        bail!("remote name cannot be empty");
    }

    let endpoint = args.endpoint.trim();
    if endpoint.is_empty() {
        bail!("remote endpoint cannot be empty");
    }

    let public_key = normalize_public_key(&args.public_key)?;

    if config.remotes.contains_key(name) && !args.replace {
        bail!(
            "remote '{}' already exists (use --replace to overwrite)",
            name
        );
    }

    config.remotes.insert(
        name.to_string(),
        RemoteConfig {
            endpoint: endpoint.to_string(),
            public_key: public_key.clone(),
        },
    );
    config.updated_at = Utc::now();
    config.save(&path)?;

    println!(
        "Remote '{}' set to endpoint {} with pinned key {}",
        name, endpoint, public_key
    );

    Ok(())
}

fn remove_remote(config_path: Option<PathBuf>, args: RemoteRemoveArgs) -> Result<()> {
    let (mut config, path) = load_or_default(config_path)?;
    let name = args.name.trim();
    if name.is_empty() {
        bail!("remote name cannot be empty");
    }

    if config.remotes.remove(name).is_some() {
        config.updated_at = Utc::now();
        config.save(&path)?;
        println!("Removed remote '{}'", name);
    } else {
        bail!("no remote named '{}' is configured", name);
    }
    Ok(())
}

fn list_remotes(config_path: Option<PathBuf>) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    if config.remotes.is_empty() {
        println!("(no remotes configured)");
        return Ok(());
    }

    for (name, remote) in &config.remotes {
        println!("{} {}", name, remote.endpoint);
    }
    Ok(())
}

fn show_remote(config_path: Option<PathBuf>, args: RemoteShowArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let name = args.name.trim();
    if name.is_empty() {
        bail!("remote name cannot be empty");
    }

    let remote = config
        .remotes
        .get(name)
        .ok_or_else(|| anyhow!("no remote named '{}' is configured", name))?;

    println!("name: {}", name);
    println!("endpoint: {}", remote.endpoint);
    println!("public_key: {}", remote.public_key);
    Ok(())
}

fn show_local_key(config_path: Option<PathBuf>, args: RemoteKeyArgs) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let key = config
        .load_public_key()
        .context("failed to load local replication public key")?;
    println!("{}", key);
    if args.show_path {
        println!("path: {}", config.public_key_path().display());
    }
    Ok(())
}

fn normalize_public_key(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("public key cannot be empty");
    }

    let decoded = decode_public_key(trimmed)?;
    if decoded.len() != 32 {
        bail!("public key must decode to 32 bytes");
    }
    Ok(trimmed.to_string())
}

fn decode_public_key(input: &str) -> Result<Vec<u8>> {
    STANDARD_NO_PAD
        .decode(input)
        .or_else(|_| STANDARD.decode(input))
        .map_err(|err| anyhow!("invalid base64 public key: {}", err))
}
