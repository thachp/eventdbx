use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    path::PathBuf,
};

use rocksdb::{DB, IteratorMode, Options};

use crate::error::{EventError, Result};

pub struct TenantAssignmentStore {
    db: DB,
}

pub const SHARD_PREFIX: &str = "shard-";

impl TenantAssignmentStore {
    pub fn open(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&path)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).map_err(map_db_error)?;
        Ok(Self { db })
    }

    fn key(tenant: &str) -> Vec<u8> {
        tenant.as_bytes().to_vec()
    }

    pub fn assign(&self, tenant: &str, shard: &str) -> Result<bool> {
        let key = Self::key(tenant);
        let new_value = shard.as_bytes();
        if let Some(existing) = self.db.get(&key).map_err(map_db_error)? {
            if existing == new_value {
                return Ok(false);
            }
        }
        self.db.put(key, new_value).map_err(map_db_error)?;
        Ok(true)
    }

    pub fn unassign(&self, tenant: &str) -> Result<bool> {
        let key = Self::key(tenant);
        if self.db.get(&key).map_err(map_db_error)?.is_some() {
            self.db.delete(key).map_err(map_db_error)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn shard_for(&self, tenant: &str) -> Result<Option<String>> {
        let key = Self::key(tenant);
        match self.db.get(key).map_err(map_db_error)? {
            Some(bytes) => {
                let shard = String::from_utf8(bytes).map_err(|err| {
                    EventError::Serialization(format!("invalid shard entry: {err}"))
                })?;
                Ok(Some(shard))
            }
            None => Ok(None),
        }
    }

    pub fn list(&self) -> Result<Vec<(String, String)>> {
        let mut assignments = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(map_db_error)?;
            let tenant = String::from_utf8(key.to_vec())
                .map_err(|err| EventError::Serialization(format!("invalid tenant entry: {err}")))?;
            let shard = String::from_utf8(value.to_vec())
                .map_err(|err| EventError::Serialization(format!("invalid shard entry: {err}")))?;
            assignments.push((tenant, shard));
        }
        Ok(assignments)
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
