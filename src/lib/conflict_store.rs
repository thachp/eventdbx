use std::{path::Path, sync::Arc};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options};
use serde::{Deserialize, Serialize};

use crate::{
    conflict::{ConflictKind, ReplicationConflict},
    error::{EventError, Result},
    snowflake::{SnowflakeGenerator, SnowflakeId},
    store::EventRecord,
};

const SEP: u8 = 0x1F;
const PREFIX_CONFLICT: &[u8] = b"conflict";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictStatus {
    Pending,
    Resolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolutionMode {
    AppliedIncoming,
    AppendedEvent,
    PatchedEvent,
    Dismissed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictResolution {
    pub mode: ConflictResolutionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<SnowflakeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub resolved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictEntry {
    pub id: u64,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub kind: ConflictKind,
    pub incoming: EventRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing: Option<EventRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub status: ConflictStatus,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ConflictResolution>,
}

impl ConflictEntry {
    fn new(id: u64, conflict: ReplicationConflict, remote: Option<String>) -> Self {
        Self {
            id,
            aggregate_type: conflict.aggregate_type,
            aggregate_id: conflict.aggregate_id,
            kind: conflict.kind,
            incoming: conflict.incoming,
            existing: conflict.existing,
            remote,
            note: None,
            status: ConflictStatus::Pending,
            created_at: conflict.observed_at.unwrap_or_else(|| Utc::now()),
            resolved: None,
        }
    }
}

#[derive(Clone)]
pub struct ConflictStore {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    id_generator: Arc<Mutex<SnowflakeGenerator>>,
}

impl ConflictStore {
    pub fn open(path: &Path) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DBWithThreadMode::<MultiThreaded>::open(&options, path)
            .map_err(|err| EventError::Storage(err.to_string()))?;
        Ok(Self {
            db: Arc::new(db),
            id_generator: Arc::new(Mutex::new(SnowflakeGenerator::new(0))),
        })
    }

    pub fn record(
        &self,
        conflict: ReplicationConflict,
        remote: Option<String>,
    ) -> Result<ConflictEntry> {
        let id = self.next_id().as_u64();
        let entry = ConflictEntry::new(id, conflict, remote);
        let encoded = serde_json::to_vec(&entry)?;
        self.db
            .put(conflict_key(id), encoded)
            .map_err(|err| EventError::Storage(err.to_string()))?;
        Ok(entry)
    }

    pub fn update_note(&self, id: u64, note: Option<String>) -> Result<ConflictEntry> {
        self.update_entry(id, move |entry| {
            entry.note = note.clone();
        })
    }

    pub fn resolve(&self, id: u64, resolution: ConflictResolution) -> Result<ConflictEntry> {
        self.update_entry(id, move |entry| {
            entry.status = ConflictStatus::Resolved;
            entry.resolved = Some(resolution.clone());
        })
    }

    pub fn get(&self, id: u64) -> Result<Option<ConflictEntry>> {
        match self
            .db
            .get(conflict_key(id))
            .map_err(|err| EventError::Storage(err.to_string()))?
        {
            Some(bytes) => {
                let entry = serde_json::from_slice(&bytes)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    pub fn list(
        &self,
        status: Option<ConflictStatus>,
        limit: Option<usize>,
    ) -> Result<Vec<ConflictEntry>> {
        let iter = self.db.iterator(IteratorMode::From(
            conflict_prefix().as_slice(),
            Direction::Forward,
        ));
        let mut entries = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(conflict_prefix().as_slice()) {
                break;
            }
            let entry: ConflictEntry = serde_json::from_slice(&value)?;
            if status.map_or(true, |desired| entry.status == desired) {
                entries.push(entry);
                if let Some(cap) = limit {
                    if entries.len() >= cap {
                        break;
                    }
                }
            }
        }
        Ok(entries)
    }

    fn update_entry<F>(&self, id: u64, mut transform: F) -> Result<ConflictEntry>
    where
        F: FnMut(&mut ConflictEntry),
    {
        let key = conflict_key(id);
        let Some(bytes) = self
            .db
            .get(&key)
            .map_err(|err| EventError::Storage(err.to_string()))?
        else {
            return Err(EventError::Storage(format!("conflict {} not found", id)));
        };
        let mut entry: ConflictEntry = serde_json::from_slice(&bytes)?;
        transform(&mut entry);
        let encoded = serde_json::to_vec(&entry)?;
        self.db
            .put(key, encoded)
            .map_err(|err| EventError::Storage(err.to_string()))?;
        Ok(entry)
    }

    pub fn next_id(&self) -> SnowflakeId {
        let mut guard = self.id_generator.lock();
        guard.next_id()
    }
}

fn conflict_key(id: u64) -> Vec<u8> {
    let mut key = conflict_prefix();
    key.push(SEP);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn conflict_prefix() -> Vec<u8> {
    PREFIX_CONFLICT.to_vec()
}
