use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Subcommand};

use crate::commands::config::ensure_secrets_configured;
use eventdb::{
    config::load_or_default,
    token::{IssueTokenInput, TokenManager},
};

#[derive(Subcommand)]
pub enum TokenCommands {
    /// Generate a new token
    Generate(TokenGenerateArgs),
    /// List configured tokens
    List,
    /// Revoke an active token
    Revoke {
        /// Token value to revoke
        token: String,
    },
    /// Refresh an existing token
    Refresh(TokenRefreshArgs),
}

#[derive(Args)]
pub struct TokenGenerateArgs {
    #[arg(long)]
    pub identifier_type: String,

    #[arg(long)]
    pub identifier_id: String,

    #[arg(long)]
    pub expiration: Option<u64>,

    #[arg(long)]
    pub limit: Option<u64>,

    #[arg(long, default_value_t = false)]
    pub keep_alive: bool,
}

#[derive(Args)]
pub struct TokenRefreshArgs {
    #[arg(long)]
    pub token: String,

    #[arg(long)]
    pub expiration: Option<u64>,

    #[arg(long)]
    pub limit: Option<u64>,
}

pub fn execute(config_path: Option<PathBuf>, command: TokenCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let manager = TokenManager::load(config.tokens_path())?;

    match command {
        TokenCommands::Generate(args) => {
            ensure_secrets_configured(&config)?;
            let record = manager.issue(IssueTokenInput {
                identifier_type: args.identifier_type,
                identifier_id: args.identifier_id,
                expiration_secs: args.expiration,
                limit: args.limit,
                keep_alive: args.keep_alive,
            })?;
            println!(
                "token={} identifier_type={} identifier_id={} expires_at={} remaining_writes={}",
                record.token,
                record.identifier_type,
                record.identifier_id,
                record
                    .expires_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "never".into()),
                record
                    .remaining_writes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unlimited".into())
            );
        }
        TokenCommands::List => {
            for record in manager.list() {
                println!(
                    "token={} status={:?} issued_at={} expires_at={} remaining_writes={}",
                    record.token,
                    record.status,
                    record.issued_at.to_rfc3339(),
                    record
                        .expires_at
                        .map(|ts| ts.to_rfc3339())
                        .unwrap_or_else(|| "never".into()),
                    record
                        .remaining_writes
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unlimited".into())
                );
            }
        }
        TokenCommands::Revoke { token } => {
            manager.revoke(&token)?;
            println!("token {} revoked", token);
        }
        TokenCommands::Refresh(args) => {
            let record = manager.refresh(&args.token, args.expiration, args.limit)?;
            println!(
                "token={} expires_at={} remaining_writes={}",
                record.token,
                record
                    .expires_at
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_else(|| "never".into()),
                record
                    .remaining_writes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unlimited".into())
            );
        }
    }

    Ok(())
}
