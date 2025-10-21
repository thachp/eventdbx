use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Subcommand};

use crate::commands::config::ensure_secrets_configured;
use eventdbx::{
    config::load_or_default,
    token::{IssueTokenInput, TokenManager},
};
use serde_json;

#[derive(Subcommand)]
pub enum TokenCommands {
    /// Generate a new token
    Generate(TokenGenerateArgs),
    /// List configured tokens
    List(TokenListArgs),
    /// Revoke an active token
    Revoke(TokenRevokeArgs),
    /// Refresh an existing token
    Refresh(TokenRefreshArgs),
}

#[derive(Args)]
pub struct TokenGenerateArgs {
    #[arg(long)]
    pub group: String,

    #[arg(long)]
    pub user: String,

    #[arg(long)]
    pub expiration: Option<u64>,

    #[arg(long)]
    pub limit: Option<u64>,

    #[arg(long, default_value_t = false)]
    pub keep_alive: bool,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Default)]
pub struct TokenListArgs {
    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args)]
pub struct TokenRefreshArgs {
    #[arg(long)]
    pub token: String,

    #[arg(long)]
    pub expiration: Option<u64>,

    #[arg(long)]
    pub limit: Option<u64>,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args)]
pub struct TokenRevokeArgs {
    /// Token value to revoke
    pub token: String,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

pub fn execute(config_path: Option<PathBuf>, command: TokenCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let encryptor = config.encryption_key()?;
    let manager = TokenManager::load(config.tokens_path(), encryptor)?;

    match command {
        TokenCommands::Generate(args) => {
            ensure_secrets_configured(&config)?;
            let record = manager.issue(IssueTokenInput {
                group: args.group,
                user: args.user,
                expiration_secs: args.expiration,
                limit: args.limit,
                keep_alive: args.keep_alive,
            })?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&record)?);
            } else {
                println!(
                    "token={} group={} user={} expires_at={} remaining_writes={}",
                    record.token,
                    record.group,
                    record.user,
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
        TokenCommands::List(args) => {
            let records = manager.list();
            if args.json {
                println!("{}", serde_json::to_string_pretty(&records)?);
            } else {
                for record in records {
                    println!(
                        "token={} group={} user={} status={:?} issued_at={} expires_at={} remaining_writes={}",
                        record.token,
                        record.group,
                        record.user,
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
        }
        TokenCommands::Revoke(args) => {
            manager.revoke(&args.token)?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "token": args.token,
                        "revoked": true
                    }))?
                );
            } else {
                println!("token {} revoked", args.token);
            }
        }
        TokenCommands::Refresh(args) => {
            let record = manager.refresh(&args.token, args.expiration, args.limit)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&record)?);
            } else {
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
    }

    Ok(())
}
