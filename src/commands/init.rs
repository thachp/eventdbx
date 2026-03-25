use std::path::PathBuf;

use anyhow::Result;
use clap::Args;

use crate::commands::cli_token;
use eventdbx::config::init_workspace;

const INIT_BOOTSTRAP_TTL_SECS: u64 = 86_400;

#[derive(Args, Clone, Default)]
pub struct InitArgs {
    /// Bootstrap token TTL. Accepts raw seconds or suffixes like 10m, 24h, 10d, or 2w (use 0 to disable expiration)
    #[arg(long = "ttl", value_name = "TTL", value_parser = parse_init_ttl)]
    pub ttl: Option<u64>,
}

pub fn execute(config_path: Option<PathBuf>, args: InitArgs) -> Result<()> {
    let (config, path, created) = init_workspace(config_path)?;
    let workspace = config.data_dir.clone();
    let ttl_secs = args.ttl.unwrap_or(INIT_BOOTSTRAP_TTL_SECS);
    let token = cli_token::ensure_bootstrap_token(&config, Some(ttl_secs))?;

    if created {
        println!(
            "Initialized empty EventDBX workspace in {}",
            workspace.display()
        );
    } else {
        println!(
            "EventDBX workspace already initialized in {}",
            workspace.display()
        );
    }
    println!("Bootstrap token (ttl={ttl_secs}s): {token}");

    tracing::info!("workspace config path: {}", path.display());
    Ok(())
}

fn parse_init_ttl(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("ttl cannot be empty".to_string());
    }
    if trimmed == "0" {
        return Ok(0);
    }

    let digits_len = trimmed
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits_len == 0 {
        return Err(format!(
            "invalid ttl '{trimmed}'; expected a number like 86400 or a suffixed value like 10d"
        ));
    }

    let amount = trimmed[..digits_len]
        .parse::<u64>()
        .map_err(|_| format!("invalid ttl '{trimmed}'"))?;
    let suffix = trimmed[digits_len..].trim().to_ascii_lowercase();

    let multiplier = match suffix.as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 60 * 60,
        "d" | "day" | "days" => 60 * 60 * 24,
        "w" | "week" | "weeks" => 60 * 60 * 24 * 7,
        _ => {
            return Err(format!(
                "invalid ttl suffix '{suffix}'; supported suffixes are s, m, h, d, and w"
            ));
        }
    };

    amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("ttl '{trimmed}' is too large"))
}

#[cfg(test)]
mod tests {
    use super::parse_init_ttl;

    #[test]
    fn parse_init_ttl_supports_seconds_and_suffixes() {
        assert_eq!(parse_init_ttl("86400").unwrap(), 86_400);
        assert_eq!(parse_init_ttl("10m").unwrap(), 600);
        assert_eq!(parse_init_ttl("24h").unwrap(), 86_400);
        assert_eq!(parse_init_ttl("10d").unwrap(), 864_000);
        assert_eq!(parse_init_ttl("2weeks").unwrap(), 1_209_600);
        assert_eq!(parse_init_ttl("0").unwrap(), 0);
    }

    #[test]
    fn parse_init_ttl_rejects_invalid_values() {
        assert!(parse_init_ttl("").is_err());
        assert!(parse_init_ttl("d").is_err());
        assert!(parse_init_ttl("10x").is_err());
    }
}
