use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    path::PathBuf,
};

use rocksdb::{DB, IteratorMode, Options};
use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::{EventError, Result};

pub struct TenantAssignmentStore {
    db: DB,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TenantRecord {
    #[serde(default)]
    pub shard: Option<String>,
    #[serde(default, rename = "storage_quota_mb", alias = "aggregate_quota")]
    pub storage_quota_mb: Option<u64>,
    #[serde(
        default,
        rename = "storage_usage_bytes",
        alias = "aggregate_count"
    )]
    pub storage_usage_bytes: Option<u64>,
}

impl TenantRecord {
    fn cleanup(&mut self) {
        if let Some(shard) = &self.shard {
            let trimmed = shard.trim();
            if trimmed.is_empty() {
                self.shard = None;
            } else if trimmed != shard {
                self.shard = Some(trimmed.to_string());
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.shard.is_none()
            && self.storage_quota_mb.is_none()
            && self.storage_usage_bytes.is_none()
    }
}

pub const BYTES_PER_MEGABYTE: u64 = 1024 * 1024;
pub const SHARD_PREFIX: &str = "shard-";

impl TenantAssignmentStore {
    pub fn open(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).map_err(map_db_error)?;
        Ok(Self { db })
    }

    pub fn open_read_only(path: PathBuf) -> Result<Self> {
        if path.exists() {
            let mut opts = Options::default();
            opts.create_if_missing(false);
            let db = DB::open_for_read_only(&opts, path, false).map_err(map_db_error)?;
            Ok(Self { db })
        } else {
            Self::open(path)
        }
    }

    fn key(tenant: &str) -> Vec<u8> {
        tenant.as_bytes().to_vec()
    }

    fn read_record(&self, tenant: &str) -> Result<Option<TenantRecord>> {
        let key = Self::key(tenant);
        let value = match self.db.get(&key).map_err(map_db_error)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let mut record = decode_record(value)?;
        record.cleanup();
        if record.is_empty() {
            Ok(None)
        } else {
            Ok(Some(record))
        }
    }

    fn write_record(&self, tenant: &str, record: TenantRecord) -> Result<()> {
        let key = Self::key(tenant);
        if record.is_empty() {
            self.db.delete(key).map_err(map_db_error)?;
        } else {
            self.db
                .put(
                    key,
                    serde_json::to_vec(&record).map_err(|err| {
                        EventError::Serialization(format!("invalid tenant record: {err}"))
                    })?,
                )
                .map_err(map_db_error)?;
        }
        Ok(())
    }

    pub fn assign(&self, tenant: &str, shard: &str) -> Result<bool> {
        let mut record = self.read_record(tenant)?.unwrap_or_default();
        let shard_string = Some(shard.to_string());
        let changed = record.shard != shard_string;
        record.shard = shard_string;
        self.write_record(tenant, record)?;
        Ok(changed)
    }

    pub fn unassign(&self, tenant: &str) -> Result<bool> {
        let Some(mut record) = self.read_record(tenant)? else {
            return Ok(false);
        };
        if record.shard.is_none() {
            return Ok(false);
        }
        record.shard = None;
        self.write_record(tenant, record)?;
        Ok(true)
    }

    pub fn shard_for(&self, tenant: &str) -> Result<Option<String>> {
        Ok(self.read_record(tenant)?.and_then(|record| record.shard))
    }

    pub fn quota_for(&self, tenant: &str) -> Result<Option<u64>> {
        Ok(self
            .read_record(tenant)?
            .and_then(|record| record.storage_quota_mb))
    }

    pub fn set_quota(&self, tenant: &str, quota: Option<u64>) -> Result<bool> {
        let mut record = self.read_record(tenant)?.unwrap_or_default();
        let changed = record.storage_quota_mb != quota;
        record.storage_quota_mb = quota;
        self.write_record(tenant, record)?;
        Ok(changed)
    }

    pub fn storage_usage_bytes(&self, tenant: &str) -> Result<Option<u64>> {
        Ok(self
            .read_record(tenant)?
            .and_then(|record| record.storage_usage_bytes))
    }

    pub fn ensure_storage_usage_bytes<F>(&self, tenant: &str, initializer: F) -> Result<u64>
    where
        F: FnOnce() -> Result<u64>,
    {
        let mut record = self.read_record(tenant)?.unwrap_or_default();
        if let Some(bytes) = record.storage_usage_bytes {
            return Ok(bytes);
        }
        let bytes = initializer()?;
        record.storage_usage_bytes = Some(bytes);
        self.write_record(tenant, record)?;
        Ok(bytes)
    }

    pub fn update_storage_usage_bytes(&self, tenant: &str, usage: u64) -> Result<u64> {
        let mut record = self.read_record(tenant)?.unwrap_or_default();
        record.storage_usage_bytes = Some(usage);
        self.write_record(tenant, record)?;
        Ok(usage)
    }

    pub fn record_for(&self, tenant: &str) -> Result<Option<TenantRecord>> {
        self.read_record(tenant)
    }

    pub fn list(&self) -> Result<Vec<(String, TenantRecord)>> {
        let mut entries = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(map_db_error)?;
            let tenant = String::from_utf8(key.to_vec())
                .map_err(|err| EventError::Serialization(format!("invalid tenant entry: {err}")))?;
            let mut record = decode_record(value.to_vec())?;
            record.cleanup();
            if record.is_empty() {
                continue;
            }
            entries.push((tenant, record));
        }
        Ok(entries)
    }
}

fn decode_record(bytes: Vec<u8>) -> Result<TenantRecord> {
    match serde_json::from_slice::<TenantRecord>(&bytes) {
        Ok(record) => Ok(record),
        Err(_) => {
            let shard = String::from_utf8(bytes)
                .map_err(|err| EventError::Serialization(format!("invalid tenant entry: {err}")))?;
            let shard = shard.trim();
            if shard.is_empty() {
                Ok(TenantRecord::default())
            } else {
                Ok(TenantRecord {
                    shard: Some(shard.to_string()),
                    storage_quota_mb: None,
                    storage_usage_bytes: None,
                })
            }
        }
    }
}

pub fn compute_default_shard(tenant: &str, shard_count: u16) -> String {
    let mut hasher = DefaultHasher::new();
    tenant.hash(&mut hasher);
    let max = shard_count.max(1) as u64;
    let index = (hasher.finish() % max) as u16;
    format_shard_id(index)
}

pub fn format_shard_id(index: u16) -> String {
    format!("{SHARD_PREFIX}{index:04}")
}

fn map_db_error(err: rocksdb::Error) -> EventError {
    EventError::Storage(format!("tenant metadata error: {err}"))
}
