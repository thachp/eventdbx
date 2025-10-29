use std::path::PathBuf;

use anyhow::{Result, bail};
use chrono::Utc;
use clap::Args;

use eventdbx::config::{DEFAULT_DOMAIN_NAME, load_or_default};

#[derive(Args)]
pub struct DomainCheckoutArgs {
    /// Domain to activate
    #[arg(short = 'd', long = "domain", value_name = "NAME")]
    pub flag_domain: Option<String>,

    /// Domain to activate (positional alias for -d/--domain)
    #[arg(value_name = "NAME")]
    pub positional_domain: Option<String>,
}

pub fn checkout(config_path: Option<PathBuf>, args: DomainCheckoutArgs) -> Result<()> {
    let target = resolve_domain_input(&args)?;
    let (mut config, path) = load_or_default(config_path)?;

    if config.active_domain() == target {
        println!(
            "Domain '{}' is already active (data directory: {}).",
            target,
            config.domain_data_dir().display()
        );
        return Ok(());
    }

    config.domain = target.clone();
    config.ensure_data_dir()?;
    let domain_root = config.domain_data_dir();
    config.updated_at = Utc::now();
    config.save(&path)?;

    println!(
        "Switched to domain '{}' (data directory: {}).",
        target,
        domain_root.display()
    );

    Ok(())
}

fn normalize_domain_name(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("domain name cannot be empty");
    }

    if trimmed.eq_ignore_ascii_case(DEFAULT_DOMAIN_NAME) {
        return Ok(DEFAULT_DOMAIN_NAME.to_string());
    }

    let lower = trimmed.to_ascii_lowercase();
    if !matches!(lower.chars().next(), Some(ch) if ch.is_ascii_alphanumeric()) {
        bail!("domain name must begin with an ASCII letter or digit");
    }
    if !lower
        .chars()
        .skip(1)
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        bail!("domain name may only contain letters, numbers, '-' or '_'");
    }
    Ok(lower)
}

fn resolve_domain_input(args: &DomainCheckoutArgs) -> Result<String> {
    match (&args.flag_domain, &args.positional_domain) {
        (Some(flag), None) => normalize_domain_name(flag),
        (None, Some(positional)) => normalize_domain_name(positional),
        (Some(flag), Some(positional)) => {
            let normalized_flag = normalize_domain_name(flag)?;
            let normalized_positional = normalize_domain_name(positional)?;
            if normalized_flag != normalized_positional {
                bail!("conflicting domain inputs provided via -d/--domain and positional argument");
            }
            Ok(normalized_flag)
        }
        (None, None) => {
            bail!("domain name must be provided via -d/--domain or positional argument")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_default_domain_case_insensitively() {
        let result = normalize_domain_name("DEFAULT").expect("domain should normalize");
        assert_eq!(result, DEFAULT_DOMAIN_NAME);
    }

    #[test]
    fn normalizes_custom_domain() {
        let result = normalize_domain_name("Herds-01").expect("domain should normalize");
        assert_eq!(result, "herds-01");
    }

    #[test]
    fn resolve_domain_from_flag() {
        let args = DomainCheckoutArgs {
            flag_domain: Some("Herds".to_string()),
            positional_domain: None,
        };
        let result = resolve_domain_input(&args).expect("flag domain should resolve");
        assert_eq!(result, "herds");
    }

    #[test]
    fn resolve_domain_from_positional() {
        let args = DomainCheckoutArgs {
            flag_domain: None,
            positional_domain: Some("Herds_01".to_string()),
        };
        let result = resolve_domain_input(&args).expect("positional domain should resolve");
        assert_eq!(result, "herds_01");
    }

    #[test]
    fn resolve_conflicting_inputs_errors() {
        let args = DomainCheckoutArgs {
            flag_domain: Some("alpha".to_string()),
            positional_domain: Some("beta".to_string()),
        };
        assert!(resolve_domain_input(&args).is_err());
    }

    #[test]
    fn rejects_invalid_characters() {
        assert!(normalize_domain_name("bad/name").is_err());
        assert!(normalize_domain_name("  ").is_err());
        assert!(normalize_domain_name("-leading").is_err());
    }
}
