use std::{
    fs, io,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    encryption::{self, Encryptor},
    error::{EventError, Result},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TokenStatus {
    Active,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub token: String,
    #[serde(alias = "identifier_type")]
    pub group: String,
    #[serde(alias = "identifier_id")]
    pub user: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub issued_at: DateTime<Utc>,
    pub limit: Option<u64>,
    pub remaining_writes: Option<u64>,
    pub keep_alive: bool,
    pub status: TokenStatus,
}

impl TokenRecord {
    pub fn is_active(&self) -> bool {
        matches!(self.status, TokenStatus::Active)
    }
}

#[derive(Debug, Clone)]
pub struct TokenGrant {
    pub group: String,
    pub user: String,
}

pub enum AccessKind {
    Write,
}

#[derive(Debug, Clone)]
pub struct IssueTokenInput {
    pub group: String,
    pub user: String,
    pub expiration_secs: Option<u64>,
    pub limit: Option<u64>,
    pub keep_alive: bool,
}

#[derive(Debug)]
pub struct TokenManager {
    path: PathBuf,
    records: RwLock<Vec<TokenRecord>>,
    encryptor: Option<Encryptor>,
}

impl TokenManager {
    pub fn load(path: PathBuf, encryptor: Option<Encryptor>) -> Result<Self> {
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            if let Some(enc) = &encryptor {
                let payload = serde_json::to_vec(&Vec::<TokenRecord>::new())?;
                let encrypted = enc.encrypt_to_string(&payload)?;
                fs::write(&path, encrypted)?;
            } else {
                fs::write(&path, "[]")?;
            }
        }

        let (records, upgraded) = read_records(&path, encryptor.as_ref())?;
        if upgraded {
            write_records(&path, &records, encryptor.as_ref())?;
        }

        Ok(Self {
            path,
            records: RwLock::new(records),
            encryptor,
        })
    }

    pub fn authorize(&self, token: &str, access: AccessKind) -> Result<TokenGrant> {
        self.refresh_from_disk()?;
        let mut records = self.records.write();
        let now = Utc::now();

        let mut expired = false;
        let mut grant = None;

        {
            let record = records
                .iter_mut()
                .find(|record| record.token == token && record.is_active())
                .ok_or(EventError::InvalidToken)?;

            if let Some(exp) = record.expires_at {
                if exp < now {
                    record.status = TokenStatus::Revoked;
                    expired = true;
                }
            }

            if !expired {
                if matches!(access, AccessKind::Write) {
                    if let Some(remaining) = record.remaining_writes {
                        if remaining == 0 {
                            return Err(EventError::TokenLimitReached);
                        }
                        record.remaining_writes = Some(remaining - 1);
                    }
                }
                grant = Some(TokenGrant {
                    group: record.group.clone(),
                    user: record.user.clone(),
                });
            }
        }

        self.persist(&records)?;

        if expired {
            return Err(EventError::TokenExpired);
        }

        grant.ok_or(EventError::InvalidToken)
    }

    pub fn issue(&self, input: IssueTokenInput) -> Result<TokenRecord> {
        self.refresh_from_disk()?;
        let mut records = self.records.write();
        let token = format!("EVT-{}", Uuid::new_v4().simple());
        let issued_at = Utc::now();
        let expires_at = input
            .expiration_secs
            .map(|secs| issued_at + Duration::seconds(secs as i64));
        let record = TokenRecord {
            token,
            group: input.group,
            user: input.user,
            expires_at,
            issued_at,
            limit: input.limit,
            remaining_writes: input.limit,
            keep_alive: input.keep_alive,
            status: TokenStatus::Active,
        };
        records.push(record.clone());
        self.persist(&records)?;
        Ok(record)
    }

    pub fn list(&self) -> Vec<TokenRecord> {
        let _ = self.refresh_from_disk();
        self.records.read().clone()
    }

    pub fn revoke(&self, token_or_id: &str) -> Result<()> {
        self.refresh_from_disk()?;
        let mut records = self.records.write();
        let Some(record) = records
            .iter_mut()
            .find(|record| record.token == token_or_id && record.is_active())
        else {
            return Err(EventError::InvalidToken);
        };
        record.status = TokenStatus::Revoked;
        self.persist(&records)?;
        Ok(())
    }

    pub fn refresh(
        &self,
        token: &str,
        expiration_secs: Option<u64>,
        limit: Option<u64>,
    ) -> Result<TokenRecord> {
        self.refresh_from_disk()?;
        let mut records = self.records.write();
        let updated_record = {
            let record = records
                .iter_mut()
                .find(|record| record.token == token && record.is_active())
                .ok_or(EventError::InvalidToken)?;

            if let Some(secs) = expiration_secs {
                record.expires_at = Some(Utc::now() + Duration::seconds(secs as i64));
            }

            if let Some(limit) = limit {
                record.limit = Some(limit);
                record.remaining_writes = Some(limit);
            }

            record.clone()
        };

        self.persist(&records)?;

        Ok(updated_record)
    }

    fn persist(&self, records: &[TokenRecord]) -> Result<()> {
        write_records(&self.path, records, self.encryptor.as_ref())
    }

    fn refresh_from_disk(&self) -> Result<()> {
        let (parsed, upgraded) = read_records(&self.path, self.encryptor.as_ref())?;
        if upgraded {
            write_records(&self.path, &parsed, self.encryptor.as_ref())?;
        }
        let mut records = self.records.write();
        *records = parsed;
        Ok(())
    }
}

fn read_records(path: &Path, encryptor: Option<&Encryptor>) -> Result<(Vec<TokenRecord>, bool)> {
    let contents = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let empty: Vec<TokenRecord> = Vec::new();
            write_records(path, &empty, encryptor)?;
            return Ok((Vec::new(), false));
        }
        Err(err) => return Err(EventError::Io(err)),
    };

    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok((Vec::new(), false));
    }

    if let Some(enc) = encryptor {
        if encryption::is_encrypted_blob(trimmed) {
            let bytes = enc.decrypt_from_str(trimmed)?;
            if bytes.is_empty() {
                return Ok((Vec::new(), false));
            }
            let records = serde_json::from_slice(&bytes)?;
            Ok((records, false))
        } else {
            let records = serde_json::from_str(trimmed)?;
            Ok((records, true))
        }
    } else {
        if encryption::is_encrypted_blob(trimmed) {
            return Err(EventError::Config(
                "data encryption key must be configured to read encrypted tokens".to_string(),
            ));
        }
        let records = serde_json::from_str(trimmed)?;
        Ok((records, false))
    }
}

fn write_records(
    path: &Path,
    records: &[TokenRecord],
    encryptor: Option<&Encryptor>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec(records)?;
    if let Some(enc) = encryptor {
        let encrypted = enc.encrypt_to_string(&payload)?;
        fs::write(path, encrypted)?;
    } else {
        fs::write(path, payload)?;
    }
    Ok(())
}
