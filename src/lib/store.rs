use std::{collections::BTreeMap, path::PathBuf};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    error::{EventfulError, Result},
    merkle::{compute_merkle_root, empty_root},
    token::TokenGrant,
};

const SEP: u8 = 0x1F;
const PREFIX_EVENT: &str = "evt";
const PREFIX_META: &str = "meta";
const PREFIX_STATE: &str = "state";
const PREFIX_SNAPSHOT: &str = "snapshot";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: BTreeMap<String, String>,
    pub metadata: EventMetadata,
    pub version: u64,
    pub hash: String,
    pub merkle_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub event_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub issued_by: Option<ActorClaims>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorClaims {
    pub identifier_type: String,
    pub identifier_id: String,
}

impl From<TokenGrant> for ActorClaims {
    fn from(value: TokenGrant) -> Self {
        Self {
            identifier_type: value.identifier_type,
            identifier_id: value.identifier_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendEvent {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: BTreeMap<String, String>,
    pub issued_by: Option<ActorClaims>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AggregateState {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: u64,
    pub state: BTreeMap<String, String>,
    pub merkle_root: String,
    pub archived: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AggregateMeta {
    aggregate_type: String,
    aggregate_id: String,
    version: u64,
    event_hashes: Vec<String>,
    merkle_root: String,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    archived_at: Option<DateTime<Utc>>,
    #[serde(default)]
    archive_comment: Option<String>,
}

impl AggregateMeta {
    fn new(aggregate_type: String, aggregate_id: String) -> Self {
        Self {
            aggregate_type,
            aggregate_id,
            version: 0,
            event_hashes: Vec::new(),
            merkle_root: empty_root(),
            archived: false,
            archived_at: None,
            archive_comment: None,
        }
    }
}

pub struct EventStore {
    db: DBWithThreadMode<MultiThreaded>,
    write_lock: Mutex<()>,
    read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: u64,
    pub state: BTreeMap<String, String>,
    pub merkle_root: String,
    pub created_at: DateTime<Utc>,
    pub comment: Option<String>,
}

impl EventStore {
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DBWithThreadMode::<MultiThreaded>::open(&options, path)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        Ok(Self {
            db,
            write_lock: Mutex::new(()),
            read_only: false,
        })
    }

    pub fn open_read_only(path: PathBuf) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(false);
        let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&options, path, false)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        Ok(Self {
            db,
            write_lock: Mutex::new(()),
            read_only: true,
        })
    }

    pub fn append(&self, input: AppendEvent) -> Result<EventRecord> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();

        let mut meta = self
            .load_meta(&input.aggregate_type, &input.aggregate_id)?
            .unwrap_or_else(|| {
                AggregateMeta::new(input.aggregate_type.clone(), input.aggregate_id.clone())
            });

        if meta.archived {
            return Err(EventfulError::AggregateArchived);
        }
        let mut state = self.load_state_map(&input.aggregate_type, &input.aggregate_id)?;

        let version = meta.version + 1;
        let created_at = Utc::now();
        let event_id = Uuid::new_v4();
        let hash = hash_event(
            &input.aggregate_type,
            &input.aggregate_id,
            version,
            &input.event_type,
            &input.payload,
        );

        meta.event_hashes.push(hash.clone());
        meta.version = version;
        meta.merkle_root = compute_merkle_root(&meta.event_hashes);

        for (key, value) in &input.payload {
            state.insert(key.clone(), value.clone());
        }

        let record = EventRecord {
            aggregate_type: input.aggregate_type.clone(),
            aggregate_id: input.aggregate_id.clone(),
            event_type: input.event_type,
            payload: input.payload,
            metadata: EventMetadata {
                event_id,
                created_at,
                issued_by: input.issued_by,
            },
            version,
            hash,
            merkle_root: meta.merkle_root.clone(),
        };

        self.db
            .put(
                event_key(&record.aggregate_type, &record.aggregate_id, version),
                serde_json::to_vec(&record)?,
            )
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        self.db
            .put(
                meta_key(&record.aggregate_type, &record.aggregate_id),
                serde_json::to_vec(&meta)?,
            )
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        self.db
            .put(
                state_key(&record.aggregate_type, &record.aggregate_id),
                serde_json::to_vec(&state)?,
            )
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        Ok(record)
    }

    pub fn get_aggregate_state(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<AggregateState> {
        let meta = self
            .load_meta(aggregate_type, aggregate_id)?
            .ok_or(EventfulError::AggregateNotFound)?;
        let state = self.load_state_map(aggregate_type, aggregate_id)?;

        Ok(AggregateState {
            aggregate_type: meta.aggregate_type,
            aggregate_id: meta.aggregate_id,
            version: meta.version,
            state,
            merkle_root: meta.merkle_root,
            archived: meta.archived,
        })
    }

    pub fn list_events(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<EventRecord>> {
        // Ensure aggregate exists
        if self.load_meta(aggregate_type, aggregate_id)?.is_none() {
            return Err(EventfulError::AggregateNotFound);
        }

        let prefix = event_prefix(aggregate_type, aggregate_id);
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));

        let mut events = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|err| EventfulError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let record: EventRecord = serde_json::from_slice(&value)?;
            events.push(record);
        }

        Ok(events)
    }

    pub fn verify(&self, aggregate_type: &str, aggregate_id: &str) -> Result<String> {
        let meta = self
            .load_meta(aggregate_type, aggregate_id)?
            .ok_or(EventfulError::AggregateNotFound)?;
        Ok(meta.merkle_root)
    }

    pub fn create_snapshot(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        comment: Option<String>,
    ) -> Result<SnapshotRecord> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();
        let state = self.get_aggregate_state(aggregate_type, aggregate_id)?;
        let created_at = Utc::now();
        let record = SnapshotRecord {
            aggregate_type: state.aggregate_type.clone(),
            aggregate_id: state.aggregate_id.clone(),
            version: state.version,
            state: state.state.clone(),
            merkle_root: state.merkle_root.clone(),
            created_at,
            comment,
        };

        let key = snapshot_key(aggregate_type, aggregate_id, created_at);
        self.db
            .put(key, serde_json::to_vec(&record)?)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        Ok(record)
    }

    pub fn set_archive(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        archived: bool,
        comment: Option<String>,
    ) -> Result<AggregateState> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();
        let mut meta = self
            .load_meta(aggregate_type, aggregate_id)?
            .ok_or(EventfulError::AggregateNotFound)?;

        meta.archived = archived;
        if archived {
            meta.archived_at = Some(Utc::now());
            meta.archive_comment = comment;
        } else {
            meta.archived_at = None;
            meta.archive_comment = None;
        }

        self.db
            .put(
                meta_key(aggregate_type, aggregate_id),
                serde_json::to_vec(&meta)?,
            )
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        drop(meta);
        self.get_aggregate_state(aggregate_type, aggregate_id)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            Err(EventfulError::Storage(
                "event store opened in read-only mode".into(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn aggregates(&self) -> Vec<AggregateState> {
        let prefix = meta_prefix();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut items = Vec::new();

        for item in iter {
            let Ok((key, value)) = item else {
                continue;
            };
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            if key.len() > prefix.len() && key[prefix.len()] != SEP {
                break;
            }

            let meta: AggregateMeta = match serde_json::from_slice(&value) {
                Ok(meta) => meta,
                Err(_) => continue,
            };

            let state = match self.load_state_map(&meta.aggregate_type, &meta.aggregate_id) {
                Ok(state) => state,
                Err(_) => continue,
            };

            items.push(AggregateState {
                aggregate_type: meta.aggregate_type.clone(),
                aggregate_id: meta.aggregate_id.clone(),
                version: meta.version,
                state,
                merkle_root: meta.merkle_root,
                archived: meta.archived,
            });
        }

        items
    }

    fn load_meta(&self, aggregate_type: &str, aggregate_id: &str) -> Result<Option<AggregateMeta>> {
        let key = meta_key(aggregate_type, aggregate_id);
        let value = self
            .db
            .get(key)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        if let Some(value) = value {
            Ok(Some(serde_json::from_slice(&value)?))
        } else {
            Ok(None)
        }
    }

    fn load_state_map(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<BTreeMap<String, String>> {
        let key = state_key(aggregate_type, aggregate_id);
        let value = self
            .db
            .get(key)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        if let Some(value) = value {
            Ok(serde_json::from_slice(&value)?)
        } else {
            Ok(BTreeMap::new())
        }
    }
}

fn meta_prefix() -> Vec<u8> {
    key_with_segments(&[PREFIX_META])
}

fn meta_key(aggregate_type: &str, aggregate_id: &str) -> Vec<u8> {
    key_with_segments(&[PREFIX_META, aggregate_type, aggregate_id])
}

fn state_key(aggregate_type: &str, aggregate_id: &str) -> Vec<u8> {
    key_with_segments(&[PREFIX_STATE, aggregate_type, aggregate_id])
}

fn event_prefix(aggregate_type: &str, aggregate_id: &str) -> Vec<u8> {
    let mut prefix = key_with_segments(&[PREFIX_EVENT, aggregate_type, aggregate_id]);
    prefix.push(SEP);
    prefix
}

fn event_key(aggregate_type: &str, aggregate_id: &str, version: u64) -> Vec<u8> {
    let mut key = event_prefix(aggregate_type, aggregate_id);
    key.extend_from_slice(&version.to_be_bytes());
    key
}

fn snapshot_key(aggregate_type: &str, aggregate_id: &str, created_at: DateTime<Utc>) -> Vec<u8> {
    let mut key = key_with_segments(&[PREFIX_SNAPSHOT, aggregate_type, aggregate_id]);
    key.push(SEP);
    key.extend_from_slice(&created_at.timestamp_millis().to_be_bytes());
    key
}

fn key_with_segments(parts: &[&str]) -> Vec<u8> {
    let mut key = Vec::new();
    let mut iter = parts.iter();
    if let Some(first) = iter.next() {
        key.extend_from_slice(first.as_bytes());
    }
    for part in iter {
        key.push(SEP);
        key.extend_from_slice(part.as_bytes());
    }
    key
}

fn hash_event(
    aggregate_type: &str,
    aggregate_id: &str,
    version: u64,
    event_type: &str,
    payload: &BTreeMap<String, String>,
) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(aggregate_type.as_bytes());
    hasher.update(aggregate_id.as_bytes());
    hasher.update(version.to_be_bytes());
    hasher.update(event_type.as_bytes());

    for (key, value) in payload {
        hasher.update(key.as_bytes());
        hasher.update(value.as_bytes());
    }

    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_consistent_merkle_root() {
        let root = compute_merkle_root(&[]);
        assert_eq!(root, empty_root());

        let root = compute_merkle_root(&["abc".into()]);
        let root2 = compute_merkle_root(&["abc".into()]);
        assert_eq!(root, root2);
    }

    #[test]
    fn append_and_retrieve_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");

        {
            let store = EventStore::open(path.clone()).unwrap();

            let mut payload = BTreeMap::new();
            payload.insert("name".into(), "Alice".into());

            let record = store
                .append(AppendEvent {
                    aggregate_type: "patient".into(),
                    aggregate_id: "patient-1".into(),
                    event_type: "patient-created".into(),
                    payload: payload.clone(),
                    issued_by: None,
                })
                .unwrap();

            assert_eq!(record.version, 1);
            assert_eq!(record.payload["name"], "Alice");

            let state = store.get_aggregate_state("patient", "patient-1").unwrap();
            assert_eq!(state.version, 1);
            assert_eq!(state.state["name"], "Alice");
            assert!(!state.archived);

            let events = store.list_events("patient", "patient-1").unwrap();
            assert_eq!(events.len(), 1);
        }
    }

    #[test]
    fn snapshots_and_archive_flow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path).unwrap();

        let mut payload = BTreeMap::new();
        payload.insert("status".into(), "active".into());

        store
            .append(AppendEvent {
                aggregate_type: "order".into(),
                aggregate_id: "order-1".into(),
                event_type: "order-created".into(),
                payload,
                issued_by: None,
            })
            .unwrap();

        let snapshot = store
            .create_snapshot("order", "order-1", Some("initial".into()))
            .unwrap();
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.comment.as_deref(), Some("initial"));

        let state = store
            .set_archive("order", "order-1", true, Some("closed".into()))
            .unwrap();
        assert!(state.archived);

        let err = store
            .append(AppendEvent {
                aggregate_type: "order".into(),
                aggregate_id: "order-1".into(),
                event_type: "order-updated".into(),
                payload: BTreeMap::new(),
                issued_by: None,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::EventfulError::AggregateArchived
        ));

        let state = store.set_archive("order", "order-1", false, None).unwrap();
        assert!(!state.archived);
    }
}
