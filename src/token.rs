use std::{fs, io, path::PathBuf};

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{EventfulError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TokenStatus {
    Active,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub token: String,
    pub identifier_type: String,
    pub identifier_id: String,
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
    pub identifier_type: String,
    pub identifier_id: String,
}

pub enum AccessKind {
    Write,
}

#[derive(Debug, Clone)]
pub struct IssueTokenInput {
    pub identifier_type: String,
    pub identifier_id: String,
    pub expiration_secs: Option<u64>,
    pub limit: Option<u64>,
    pub keep_alive: bool,
}

#[derive(Debug)]
pub struct TokenManager {
    path: PathBuf,
    records: RwLock<Vec<TokenRecord>>,
}

impl TokenManager {
    pub fn load(path: PathBuf) -> Result<Self> {
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&path, "[]")?;
        }

        let contents = fs::read_to_string(&path)?;
        let records: Vec<TokenRecord> = if contents.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&contents)?
        };

        Ok(Self {
            path,
            records: RwLock::new(records),
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
                .ok_or(EventfulError::InvalidToken)?;

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
                            return Err(EventfulError::TokenLimitReached);
                        }
                        record.remaining_writes = Some(remaining - 1);
                    }
                }
                grant = Some(TokenGrant {
                    identifier_type: record.identifier_type.clone(),
                    identifier_id: record.identifier_id.clone(),
                });
            }
        }

        self.persist(&records)?;

        if expired {
            return Err(EventfulError::TokenExpired);
        }

        grant.ok_or(EventfulError::InvalidToken)
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
            identifier_type: input.identifier_type,
            identifier_id: input.identifier_id,
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
            return Err(EventfulError::InvalidToken);
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
                .ok_or(EventfulError::InvalidToken)?;

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
        let payload = serde_json::to_string_pretty(records)?;
        fs::write(&self.path, payload)?;
        Ok(())
    }

    fn refresh_from_disk(&self) -> Result<()> {
        let contents = match fs::read_to_string(&self.path) {
            Ok(data) => data,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = self.path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&self.path, "[]")?;
                String::new()
            }
            Err(err) => return Err(EventfulError::Io(err)),
        };

        let parsed: Vec<TokenRecord> = if contents.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&contents)?
        };

        let mut records = self.records.write();
        *records = parsed;
        Ok(())
    }
}
