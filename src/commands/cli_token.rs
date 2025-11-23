use std::{fs, io, path::Path};

#[cfg(unix)]
use std::io::Write;

use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use eventdbx::{
    config::Config,
    token::{IssueTokenInput, JwtClaims, JwtLimits, ROOT_ACTION, ROOT_RESOURCE, TokenManager},
};
use ring::signature::UnparsedPublicKey;
use tracing::info;

const BOOTSTRAP_GROUP: &str = "cli";
const BOOTSTRAP_USER: &str = "root";
const BOOTSTRAP_SUBJECT: &str = "cli:bootstrap";
const BOOTSTRAP_ISSUER: &str = "cli-bootstrap";
const BOOTSTRAP_DEFAULT_TTL_SECS: u64 = 7_200;

pub fn ensure_bootstrap_token(config: &Config, ttl_override: Option<u64>) -> Result<String> {
    let manager = open_token_manager(config)?;
    if ttl_override.is_none() {
        if let Some(token) = load_existing_token(config, &manager)? {
            return Ok(token);
        }
    }

    let token_value = issue_new_bootstrap_token(&manager, ttl_override)?;
    let path = config.cli_token_path();
    write_token_file(path.as_path(), &token_value)?;
    info!(
        "CLI bootstrap token generated with root privileges at {}",
        path.display()
    );

    Ok(token_value)
}

pub fn issue_bootstrap_token(config: &Config, ttl_override: Option<u64>) -> Result<String> {
    let manager = open_token_manager(config)?;
    issue_new_bootstrap_token(&manager, ttl_override)
}

fn open_token_manager(config: &Config) -> Result<TokenManager> {
    let encryptor = config.encryption_key()?;
    let jwt_config = config.jwt_manager_config()?;
    Ok(TokenManager::load(
        jwt_config,
        config.tokens_path(),
        config.jwt_revocations_path(),
        encryptor,
    )?)
}

fn issue_new_bootstrap_token(manager: &TokenManager, ttl_override: Option<u64>) -> Result<String> {
    let ttl_secs = ttl_override.unwrap_or(BOOTSTRAP_DEFAULT_TTL_SECS);
    let record = manager.issue(IssueTokenInput {
        subject: BOOTSTRAP_SUBJECT.to_string(),
        group: BOOTSTRAP_GROUP.to_string(),
        user: BOOTSTRAP_USER.to_string(),
        actions: vec![ROOT_ACTION.to_string()],
        resources: vec![ROOT_RESOURCE.to_string()],
        tenants: Vec::new(),
        ttl_secs: Some(ttl_secs),
        not_before: None,
        issued_by: BOOTSTRAP_ISSUER.to_string(),
        limits: JwtLimits {
            write_events: None,
            keep_alive: false,
        },
    })?;

    let token_value = record
        .token
        .clone()
        .ok_or_else(|| anyhow!("bootstrap token missing value"))?;
    Ok(token_value)
}

fn load_existing_token(config: &Config, manager: &TokenManager) -> Result<Option<String>> {
    let path = config.cli_token_path();
    let contents = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).context("failed to read cli token file"),
    };

    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let segments: Vec<&str> = trimmed.split('.').collect();
    if segments.len() != 3 {
        return Ok(None);
    }
    let signing_input = format!("{}.{}", segments[0], segments[1]);
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(segments[2])
        .context("failed to decode cli token signature")?;
    UnparsedPublicKey::new(&ring::signature::ED25519, manager.public_key())
        .verify(signing_input.as_bytes(), &signature)
        .map_err(|err| anyhow!("invalid cli bootstrap token signature: {err}"))?;

    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(segments[1])
        .context("failed to decode cli token payload")?;
    let claims: JwtClaims =
        serde_json::from_slice(&payload).context("failed to parse cli token claims")?;

    if claims.iss != config.auth.issuer || claims.aud != config.auth.audience {
        return Ok(None);
    }

    let now = Utc::now();
    if let Some(exp) = claims.exp {
        let exp = DateTime::from_timestamp(exp, 0)
            .ok_or_else(|| anyhow!("cli token contains invalid expiration timestamp"))?;
        if exp < now {
            return Ok(None);
        }
    }
    if let Some(nbf) = claims.nbf {
        let nbf = DateTime::from_timestamp(nbf, 0)
            .ok_or_else(|| anyhow!("cli token contains invalid not-before timestamp"))?;
        if nbf > now {
            return Ok(None);
        }
    }
    let issued_at = DateTime::from_timestamp(claims.iat, 0)
        .ok_or_else(|| anyhow!("cli token contains invalid issued-at timestamp"))?;
    let skew = Duration::seconds(config.auth.clock_skew_secs as i64);
    if issued_at > now + skew {
        return Ok(None);
    }

    let has_root_action = claims.actions.iter().any(|action| action == ROOT_ACTION);
    let has_root_resource = claims
        .resources
        .iter()
        .any(|resource| resource == ROOT_RESOURCE);

    if has_root_action && has_root_resource {
        Ok(Some(trimmed.to_string()))
    } else {
        Ok(None)
    }
}

fn write_token_file(path: &Path, token: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create CLI token directory {}", parent.display())
        })?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("failed to open CLI token file {}", path.display()))?;
        file.write_all(token.as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .with_context(|| format!("failed to write CLI token to {}", path.display()))?;
    }

    #[cfg(not(unix))]
    {
        fs::write(path, format!("{token}\n"))
            .with_context(|| format!("failed to write CLI token to {}", path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventdbx::error::EventError;
    use std::{thread, time};
    use tempfile::tempdir;

    #[test]
    fn ensures_bootstrap_token_created_and_reused() -> Result<()> {
        let dir = tempdir().context("failed to create temp dir")?;
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ensure_data_dir()?;

        let first = ensure_bootstrap_token(&config, None)?;
        assert!(!first.is_empty());
        let path = config.cli_token_path();
        assert!(path.exists());
        let manager = TokenManager::load(
            config.jwt_manager_config()?,
            config.tokens_path(),
            config.jwt_revocations_path(),
            config.encryption_key()?,
        )?;
        let records = manager.list()?;
        assert!(
            records
                .iter()
                .any(|record| record.subject == BOOTSTRAP_SUBJECT)
        );
        let segments: Vec<&str> = first.split('.').collect();
        assert_eq!(segments.len(), 3, "token must contain three segments");
        let signing_input = format!("{}.{}", segments[0], segments[1]);
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(segments[2])
            .context("failed to decode token signature")?;
        let jwt_config = config.jwt_manager_config()?;
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, &jwt_config.public_key)
            .verify(signing_input.as_bytes(), &signature)
            .context("ring verification failed")?;
        let claims = manager.verify(&first)?;
        let exp = claims.exp.expect("bootstrap token should set exp");
        let ttl_secs = exp - claims.iat;
        assert!(
            (ttl_secs - BOOTSTRAP_DEFAULT_TTL_SECS as i64).abs() <= 1,
            "expected bootstrap TTL close to {BOOTSTRAP_DEFAULT_TTL_SECS}, got {ttl_secs}"
        );

        let second = ensure_bootstrap_token(&config, None)?;
        assert_eq!(first, second);

        Ok(())
    }

    #[test]
    fn bootstrap_token_respects_override_and_expires() -> Result<()> {
        let dir = tempdir().context("failed to create temp dir")?;
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.auth.clock_skew_secs = 0;
        config.ensure_data_dir()?;

        let token = issue_bootstrap_token(&config, Some(1))?;
        let manager = TokenManager::load(
            config.jwt_manager_config()?,
            config.tokens_path(),
            config.jwt_revocations_path(),
            config.encryption_key()?,
        )?;

        let claims = manager.verify(&token)?;
        assert_eq!(
            claims.exp.map(|exp| exp - claims.iat),
            Some(1),
            "override TTL should be applied"
        );

        thread::sleep(time::Duration::from_secs(2));
        let err = manager
            .verify(&token)
            .expect_err("expired token should be rejected");
        assert!(
            matches!(err, EventError::TokenExpired),
            "expected TokenExpired, got {err}"
        );

        Ok(())
    }
}
