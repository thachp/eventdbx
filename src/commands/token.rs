use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Args, Subcommand};

use super::cli_token;
use crate::commands::config::ensure_secrets_configured;
use eventdbx::{
    config::{Config, load_or_default},
    tenant::normalize_tenant_list,
    token::{IssueTokenInput, JwtLimits, RevokeTokenInput, TokenManager, TokenRecord},
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
    /// Refresh an existing token (revokes the prior token)
    Refresh(TokenRefreshArgs),
    /// Issue a CLI bootstrap token
    Bootstrap(TokenBootstrapArgs),
}

#[derive(Args)]
pub struct TokenGenerateArgs {
    #[arg(short, long)]
    pub group: String,

    #[arg(short, long)]
    pub user: String,

    #[arg(long)]
    pub subject: Option<String>,

    #[arg(long = "action", value_name = "ACTION")]
    pub actions: Vec<String>,

    #[arg(long = "resource", value_name = "RESOURCE")]
    pub resources: Vec<String>,

    #[arg(long = "ttl")]
    pub ttl: Option<u64>,

    #[arg(long = "issued-by")]
    pub issued_by: Option<String>,

    #[arg(long = "write-limit")]
    pub write_limit: Option<u64>,

    #[arg(long, default_value_t = false)]
    pub keep_alive: bool,

    /// Restrict the token to one or more tenants (repeat flag to allow multiple)
    #[arg(long = "tenant", value_name = "TENANT")]
    pub tenants: Vec<String>,

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

    #[arg(long = "ttl")]
    pub ttl: Option<u64>,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args)]
pub struct TokenRevokeArgs {
    /// Token value or JTI to revoke
    pub token: String,

    #[arg(long)]
    pub reason: Option<String>,

    /// Emit JSON output
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Default)]
pub struct TokenBootstrapArgs {
    /// Print the bootstrap token to stdout (skips writing cli.token unless --persist is set)
    #[arg(long, default_value_t = false)]
    pub stdout: bool,

    /// Persist the token to cli.token even when printing to stdout
    #[arg(long, requires = "stdout", default_value_t = false)]
    pub persist: bool,

    /// Override the bootstrap token TTL in seconds (use 0 to disable expiration)
    #[arg(long = "ttl")]
    pub ttl: Option<u64>,
}

pub fn execute(config_path: Option<PathBuf>, command: TokenCommands) -> Result<()> {
    let (config, _) = load_or_default(config_path)?;
    let encryptor = config.encryption_key()?;
    let jwt_config = config.jwt_manager_config()?;
    let manager = TokenManager::load(
        jwt_config,
        config.tokens_path(),
        config.jwt_revocations_path(),
        encryptor,
    )?;

    match command {
        TokenCommands::Generate(args) => {
            ensure_secrets_configured(&config)?;
            if args.actions.is_empty() {
                bail!("at least one --action must be provided");
            }
            if args.tenants.is_empty() {
                bail!("--tenant must be specified at least once to bind the token to a tenant");
            }

            let subject = args
                .subject
                .clone()
                .unwrap_or_else(|| format!("{}:{}", args.group, args.user));
            let issued_by = args.issued_by.clone().unwrap_or_else(|| "cli".to_string());
            let resources = if args.resources.is_empty() {
                vec!["*".to_string()]
            } else {
                args.resources.clone()
            };
            let record = manager.issue(IssueTokenInput {
                subject,
                group: args.group,
                user: args.user,
                actions: args.actions.clone(),
                resources,
                tenants: normalize_tenant_list(&args.tenants),
                ttl_secs: args.ttl,
                not_before: None,
                issued_by,
                limits: JwtLimits {
                    write_events: args.write_limit,
                    keep_alive: args.keep_alive,
                },
            })?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&record)?);
            } else {
                print_record(&record);
            }
        }
        TokenCommands::List(args) => {
            let records = manager.list()?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&records)?);
            } else if records.is_empty() {
                println!("no issued tokens");
            } else {
                for record in records {
                    print_record(&record);
                }
            }
        }
        TokenCommands::Revoke(args) => {
            let revoked = manager.revoke(RevokeTokenInput {
                token_or_id: args.token.clone(),
                reason: args.reason.clone(),
            })?;
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "token": args.token,
                        "revoked": true,
                        "reason": revoked.reason,
                        "revoked_at": revoked.revoked_at.to_rfc3339(),
                    }))?
                );
            } else {
                println!("revoked {}", revoked.jti);
            }
        }
        TokenCommands::Refresh(args) => {
            let record = manager.refresh(&args.token, args.ttl)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&record)?);
            } else {
                println!("issued replacement token:");
                print_record(&record);
            }
        }
        TokenCommands::Bootstrap(args) => handle_bootstrap_command(&config, args)?,
    }

    Ok(())
}

fn print_record(record: &TokenRecord) {
    let token_display = record.token.as_deref().unwrap_or("<hidden>");
    let expires_at = record
        .expires_at
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "never".to_string());
    let actions = if record.actions.is_empty() {
        "none".to_string()
    } else {
        record.actions.join(",")
    };
    let resources = if record.resources.is_empty() {
        "none".to_string()
    } else {
        record.resources.join(",")
    };
    let tenants = if record.tenants.is_empty() {
        "any".to_string()
    } else {
        record.tenants.join(",")
    };
    println!(
        "token={}\n  jti={}\n  subject={}\n  group={}\n  user={}\n  status={:?}\n  issued_by={}\n  issued_at={}\n  expires_at={}\n  actions=[{}]\n  resources=[{}]\n  tenants=[{}]",
        token_display,
        record.jti,
        record.subject,
        record.group,
        record.user,
        record.status,
        record.issued_by,
        record.issued_at.to_rfc3339(),
        expires_at,
        actions,
        resources,
        tenants
    );
}

fn handle_bootstrap_command(config: &Config, args: TokenBootstrapArgs) -> Result<()> {
    if args.stdout {
        let token = if args.persist {
            cli_token::ensure_bootstrap_token(config, args.ttl)?
        } else {
            cli_token::issue_bootstrap_token(config, args.ttl)?
        };
        println!("{token}");
    } else {
        cli_token::ensure_bootstrap_token(config, args.ttl)?;
        println!(
            "Bootstrap token stored at {}",
            config.cli_token_path().display()
        );
    }
    Ok(())
}
