use std::{fs, io, path::Path};

#[cfg(unix)]
use std::io::Write;

use anyhow::{Context, Result};
use base64::Engine as _;
use eventdbx::{
    config::Config,
    token::{IssueTokenInput, JwtLimits, ROOT_ACTION, ROOT_RESOURCE, TokenManager},
};
use ring::signature::UnparsedPublicKey;
use tracing::info;

const BOOTSTRAP_GROUP: &str = "cli";
const BOOTSTRAP_USER: &str = "root";
const BOOTSTRAP_SUBJECT: &str = "cli:bootstrap";
const BOOTSTRAP_ISSUER: &str = "cli-bootstrap";

pub fn ensure_bootstrap_token(config: &Config) -> Result<String> {
    let encryptor = config.encryption_key()?;
    let jwt_config = config.jwt_manager_config()?;
    let manager = TokenManager::load(
        jwt_config,
        config.tokens_path(),
        config.jwt_revocations_path(),
        encryptor,
    )?;

    if let Some(token) = load_existing_token(config, &manager)? {
        return Ok(token);
    }

    let record = manager.issue(IssueTokenInput {
        subject: BOOTSTRAP_SUBJECT.to_string(),
        group: BOOTSTRAP_GROUP.to_string(),
        user: BOOTSTRAP_USER.to_string(),
        actions: vec![ROOT_ACTION.to_string()],
        resources: vec![ROOT_RESOURCE.to_string()],
        ttl_secs: Some(0),
        not_before: None,
        issued_by: BOOTSTRAP_ISSUER.to_string(),
        limits: JwtLimits {
            write_events: None,
            keep_alive: false,
        },
    })?;

    let path = config.cli_token_path();
    write_token_file(path.as_path(), &record.token)?;
    info!(
        "CLI bootstrap token generated with root privileges at {}",
        path.display()
    );

    Ok(record.token)
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
        .map_err(|err| anyhow::anyhow!("invalid cli bootstrap token signature: {err}"))?;

    let record = manager
        .list()?
        .into_iter()
        .find(|record| record.token == trimmed);
    let Some(record) = record else {
        return Ok(None);
    };

    let has_root_action = record.actions.iter().any(|action| action == ROOT_ACTION);
    let has_root_resource = record
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
    use tempfile::tempdir;

    #[test]
    fn ensures_bootstrap_token_created_and_reused() -> Result<()> {
        let dir = tempdir().context("failed to create temp dir")?;
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ensure_data_dir()?;

        let first = ensure_bootstrap_token(&config)?;
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
        assert!(records.iter().any(|record| record.token == first));
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

        let second = ensure_bootstrap_token(&config)?;
        assert_eq!(first, second);

        Ok(())
    }
}
