use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use dirs::home_dir;
use serde::{Deserialize, Serialize};
use serde_json;
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCloudSession {
    pub token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub api_base: Option<String>,
}

impl StoredCloudSession {
    fn ensure_token_not_empty(&self) -> Result<()> {
        if self.token.trim().is_empty() {
            bail!("refusing to persist an empty token");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CloudSessionStore {
    token_path: PathBuf,
}

impl CloudSessionStore {
    pub fn new_default() -> Result<Self> {
        Ok(Self {
            token_path: Self::default_token_path()?,
        })
    }

    pub fn with_token_path<P: Into<PathBuf>>(token_path: P) -> Self {
        Self {
            token_path: token_path.into(),
        }
    }

    pub fn token_path(&self) -> &Path {
        &self.token_path
    }

    pub fn read(&self) -> Result<StoredCloudSession> {
        match self.read_optional()? {
            Some(session) => Ok(session),
            None => bail!(
                "No EventDBX Cloud session token found at {}. Run 'dbx cloud auth --email <email>' first.",
                self.token_path.display()
            ),
        }
    }

    pub fn read_optional(&self) -> Result<Option<StoredCloudSession>> {
        self.read_from_path(&self.token_path)
    }

    fn read_from_path(&self, token_path: &Path) -> Result<Option<StoredCloudSession>> {
        if !token_path.exists() {
            return Ok(None);
        }

        let contents = fs::read_to_string(token_path)
            .with_context(|| format!("failed to read {}", token_path.display()))?;
        let trimmed = contents.trim();
        if trimmed.is_empty() {
            bail!(
                "EventDBX Cloud token file at {} is empty. Re-authenticate with 'dbx cloud auth'.",
                token_path.display()
            );
        }

        let session = if trimmed.starts_with('{') {
            serde_json::from_str(trimmed).with_context(|| {
                format!(
                    "failed to parse EventDBX Cloud session in {}",
                    token_path.display()
                )
            })?
        } else {
            StoredCloudSession {
                token: trimmed.to_string(),
                refresh_token: None,
                api_base: None,
            }
        };

        Ok(Some(session))
    }

    pub fn persist(&self, session: &StoredCloudSession) -> Result<PathBuf> {
        session.ensure_token_not_empty()?;
        if let Some(parent) = self.token_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let serialized = serde_json::to_string_pretty(session)
            .context("failed to serialize EventDBX Cloud session")?;
        fs::write(&self.token_path, format!("{serialized}\n"))
            .with_context(|| format!("failed to write {}", self.token_path.display()))?;
        Ok(self.token_path.clone())
    }

    pub fn logout(&self) -> Result<()> {
        if self.token_path.exists() {
            fs::remove_file(&self.token_path)
                .with_context(|| format!("failed to remove {}", self.token_path.display()))?;
        }
        Ok(())
    }

    pub fn last_known_email(&self) -> Result<Option<String>> {
        let Some(session) = self.read_optional()? else {
            return Ok(None);
        };
        let claims = match decode_cloud_token(&session.token) {
            Ok(claims) => claims,
            Err(err) => {
                debug!("failed to decode stored EventDBX Cloud token when resolving email: {err}");
                return Ok(None);
            }
        };
        Ok(extract_email_from_claims(&claims)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string()))
    }

    fn default_token_path() -> Result<PathBuf> {
        let config_dir = home_dir()
            .map(|path| path.join(".eventdbx"))
            .context("failed to resolve home directory for ~/.eventdbx")?;
        Ok(config_dir.join("cloud.token"))
    }
}

#[derive(Debug, Deserialize)]
pub struct CloudTokenClaims {
    sub: String,
    #[serde(default)]
    exp: Option<i64>,
    #[serde(default)]
    email_address: Option<String>,
    #[serde(default)]
    email_addresses: Vec<CloudEmailAddressClaims>,
    #[serde(default)]
    primary_email_address_id: Option<String>,
}

impl CloudTokenClaims {
    pub fn expires_at_epoch(&self) -> Option<i64> {
        self.exp
    }

    pub fn subject(&self) -> Result<&str> {
        let subject = self.sub.trim();
        if subject.is_empty() {
            bail!("EventDBX Cloud token is missing a subject ('sub') claim");
        }
        Ok(subject)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct CloudEmailAddressClaims {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub email_address: Option<String>,
}

pub fn decode_cloud_token(token: &str) -> Result<CloudTokenClaims> {
    let payload_segment = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow!("EventDBX Cloud token is not a valid JWT"))?;
    let decoded = decode_jwt_segment(payload_segment)?;
    let claims: CloudTokenClaims = serde_json::from_slice(&decoded)
        .map_err(|err| anyhow!("failed to parse EventDBX Cloud token payload: {err}"))?;
    Ok(claims)
}

pub fn extract_email_from_claims<'a>(claims: &'a CloudTokenClaims) -> Option<&'a str> {
    if let Some(email) = claims
        .email_address
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return Some(email);
    }

    if let Some(primary_id) = claims
        .primary_email_address_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        if let Some(email) = claims
            .email_addresses
            .iter()
            .find(|address| address.id.as_deref() == Some(primary_id))
            .and_then(|address| address.email_address.as_deref())
            .filter(|value| !value.trim().is_empty())
        {
            return Some(email);
        }
    }

    claims.email_addresses.iter().find_map(|address| {
        address
            .email_address
            .as_deref()
            .filter(|value| !value.trim().is_empty())
    })
}

fn decode_jwt_segment(segment: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(segment)
        .or_else(|_| URL_SAFE.decode(segment))
        .map_err(|err| anyhow!("failed to decode EventDBX Cloud token: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_email_prefers_primary_id() {
        let claims = CloudTokenClaims {
            sub: "user_123".into(),
            exp: None,
            email_address: None,
            email_addresses: vec![
                CloudEmailAddressClaims {
                    id: Some("id_1".into()),
                    email_address: Some("first@example.com".into()),
                },
                CloudEmailAddressClaims {
                    id: Some("id_2".into()),
                    email_address: Some("second@example.com".into()),
                },
            ],
            primary_email_address_id: Some("id_2".into()),
        };
        assert_eq!(
            extract_email_from_claims(&claims),
            Some("second@example.com")
        );
    }

    #[test]
    fn extract_email_falls_back_to_first_address() {
        let claims = CloudTokenClaims {
            sub: "user_123".into(),
            exp: None,
            email_address: None,
            email_addresses: vec![CloudEmailAddressClaims {
                id: Some("id_1".into()),
                email_address: Some("fallback@example.com".into()),
            }],
            primary_email_address_id: None,
        };
        assert_eq!(
            extract_email_from_claims(&claims),
            Some("fallback@example.com")
        );
    }
}
