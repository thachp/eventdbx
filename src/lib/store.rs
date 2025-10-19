use std::{collections::BTreeMap, path::PathBuf};

use chrono::{DateTime, Utc};
use parking_lot::{Mutex, MutexGuard};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::{
    error::{EventError, Result},
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
    pub payload: Value,
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
    pub group: String,
    pub user: String,
}

impl From<TokenGrant> for ActorClaims {
    fn from(value: TokenGrant) -> Self {
        Self {
            group: value.group,
            user: value.user,
        }
    }
}

impl<'a> Transaction<'a> {
    pub fn append(&mut self, input: AppendEvent) -> Result<EventRecord> {
        let key = AggregateKey::new(input.aggregate_type.clone(), input.aggregate_id.clone());
        let (meta, state) = self.ensure_cache(&key)?;
        if meta.archived {
            return Err(EventError::AggregateArchived);
        }
        let record = apply_event(meta, state, input);
        let event_value = serde_json::to_vec(&record)?;

        self.pending_events.push(PendingEvent {
            key: event_key(&record.aggregate_type, &record.aggregate_id, record.version),
            value: event_value,
        });
        self.pending_records.push(record.clone());
        Ok(record)
    }

    pub fn commit(mut self) -> Result<Vec<EventRecord>> {
        if self.pending_events.is_empty()
            && self.meta_cache.is_empty()
            && self.state_cache.is_empty()
        {
            return Ok(Vec::new());
        }

        let pending_events = std::mem::take(&mut self.pending_events);
        let meta_cache = std::mem::take(&mut self.meta_cache);
        let state_cache = std::mem::take(&mut self.state_cache);
        let records = std::mem::take(&mut self.pending_records);

        let mut batch = WriteBatch::default();

        for pending in pending_events {
            batch_put(&mut batch, pending.key, pending.value)?;
        }

        for meta in meta_cache.into_values() {
            let key = meta_key(&meta.aggregate_type, &meta.aggregate_id);
            batch_put(&mut batch, key, serde_json::to_vec(&meta)?)?;
        }

        for (key, state) in state_cache {
            let (aggregate_type, aggregate_id) = key.parts();
            let state_bytes = serde_json::to_vec(&state)?;
            batch_put(
                &mut batch,
                state_key(aggregate_type, aggregate_id),
                state_bytes,
            )?;
        }

        self.store.write_batch(batch)?;
        Ok(records)
    }

    fn ensure_cache(
        &mut self,
        key: &AggregateKey,
    ) -> Result<(&mut AggregateMeta, &mut BTreeMap<String, String>)> {
        if !self.meta_cache.contains_key(key) {
            let (aggregate_type, aggregate_id) = key.parts();
            let meta = self
                .store
                .load_meta(aggregate_type, aggregate_id)?
                .unwrap_or_else(|| {
                    AggregateMeta::new(aggregate_type.to_string(), aggregate_id.to_string())
                });
            self.meta_cache.insert(key.clone(), meta);
        }

        if !self.state_cache.contains_key(key) {
            let (aggregate_type, aggregate_id) = key.parts();
            let state = self.store.load_state_map(aggregate_type, aggregate_id)?;
            self.state_cache.insert(key.clone(), state);
        }

        let meta = self
            .meta_cache
            .get_mut(key)
            .expect("meta cache missing after insertion");
        let state = self
            .state_cache
            .get_mut(key)
            .expect("state cache missing after insertion");
        Ok((meta, state))
    }
}

fn apply_event(
    meta: &mut AggregateMeta,
    state: &mut BTreeMap<String, String>,
    input: AppendEvent,
) -> EventRecord {
    let AppendEvent {
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        issued_by,
    } = input;

    debug_assert_eq!(&meta.aggregate_type, &aggregate_type);
    debug_assert_eq!(&meta.aggregate_id, &aggregate_id);

    let version = meta.version + 1;
    let created_at = Utc::now();
    let event_id = Uuid::new_v4();
    let payload_map = payload_to_map(&payload);
    let hash = hash_event(
        &aggregate_type,
        &aggregate_id,
        version,
        &event_type,
        &payload_map,
    );

    meta.event_hashes.push(hash.clone());
    meta.version = version;
    meta.merkle_root = compute_merkle_root(&meta.event_hashes);

    for (key, value) in payload_map {
        state.insert(key, value);
    }

    EventRecord {
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        metadata: EventMetadata {
            event_id,
            created_at,
            issued_by,
        },
        version,
        hash,
        merkle_root: meta.merkle_root.clone(),
    }
}

fn batch_put(batch: &mut WriteBatch, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    batch.put(key, value);
    Ok(())
}

#[derive(Debug, Clone)]
pub struct AppendEvent {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: Value,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AggregateKey {
    aggregate_type: String,
    aggregate_id: String,
}

impl AggregateKey {
    fn new(aggregate_type: impl Into<String>, aggregate_id: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
        }
    }

    fn parts(&self) -> (&str, &str) {
        (&self.aggregate_type, &self.aggregate_id)
    }
}

struct PendingEvent {
    key: Vec<u8>,
    value: Vec<u8>,
}

pub struct EventStore {
    db: DBWithThreadMode<MultiThreaded>,
    write_lock: Mutex<()>,
    read_only: bool,
}

pub struct Transaction<'a> {
    store: &'a EventStore,
    _guard: MutexGuard<'a, ()>,
    pending_events: Vec<PendingEvent>,
    pending_records: Vec<EventRecord>,
    meta_cache: BTreeMap<AggregateKey, AggregateMeta>,
    state_cache: BTreeMap<AggregateKey, BTreeMap<String, String>>,
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

#[derive(Debug, Clone)]
pub struct AggregatePositionEntry {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: u64,
}

impl EventStore {
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DBWithThreadMode::<MultiThreaded>::open(&options, path)
            .map_err(|err| EventError::Storage(err.to_string()))?;

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
            .map_err(|err| EventError::Storage(err.to_string()))?;

        Ok(Self {
            db,
            write_lock: Mutex::new(()),
            read_only: true,
        })
    }

    pub fn append(&self, input: AppendEvent) -> Result<EventRecord> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();

        let aggregate_type = input.aggregate_type.clone();
        let aggregate_id = input.aggregate_id.clone();

        let mut meta = self
            .load_meta(&aggregate_type, &aggregate_id)?
            .unwrap_or_else(|| AggregateMeta::new(aggregate_type.clone(), aggregate_id.clone()));

        if meta.archived {
            return Err(EventError::AggregateArchived);
        }
        let mut state = self.load_state_map(&aggregate_type, &aggregate_id)?;
        let record = apply_event(&mut meta, &mut state, input);

        let mut batch = WriteBatch::default();
        batch_put(
            &mut batch,
            event_key(&record.aggregate_type, &record.aggregate_id, record.version),
            serde_json::to_vec(&record)?,
        )?;
        batch_put(
            &mut batch,
            meta_key(&record.aggregate_type, &record.aggregate_id),
            serde_json::to_vec(&meta)?,
        )?;
        batch_put(
            &mut batch,
            state_key(&record.aggregate_type, &record.aggregate_id),
            serde_json::to_vec(&state)?,
        )?;

        self.write_batch(batch)?;

        Ok(record)
    }

    pub fn append_replica(&self, record: EventRecord) -> Result<()> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();

        let aggregate_type = record.aggregate_type.clone();
        let aggregate_id = record.aggregate_id.clone();

        let mut meta = self
            .load_meta(&aggregate_type, &aggregate_id)?
            .unwrap_or_else(|| AggregateMeta::new(aggregate_type.clone(), aggregate_id.clone()));

        if meta.version + 1 != record.version {
            return Err(EventError::Storage(format!(
                "replication version mismatch for {}::{} (expected {}, got {})",
                aggregate_type,
                aggregate_id,
                meta.version + 1,
                record.version
            )));
        }

        let mut state = self.load_state_map(&aggregate_type, &aggregate_id)?;
        let payload_map = payload_to_map(&record.payload);

        let computed_hash = hash_event(
            &aggregate_type,
            &aggregate_id,
            record.version,
            &record.event_type,
            &payload_map,
        );

        if computed_hash != record.hash {
            return Err(EventError::Storage(format!(
                "replication hash mismatch for {}::{} v{}",
                aggregate_type, aggregate_id, record.version
            )));
        }

        for (key, value) in payload_map {
            state.insert(key, value);
        }

        meta.event_hashes.push(record.hash.clone());
        meta.version = record.version;
        meta.merkle_root = compute_merkle_root(&meta.event_hashes);

        if meta.merkle_root != record.merkle_root {
            return Err(EventError::Storage(format!(
                "replication merkle root mismatch for {}::{} v{}",
                aggregate_type, aggregate_id, record.version
            )));
        }

        let mut batch = WriteBatch::default();
        batch_put(
            &mut batch,
            event_key(&aggregate_type, &aggregate_id, record.version),
            serde_json::to_vec(&record)?,
        )?;
        batch_put(
            &mut batch,
            meta_key(&aggregate_type, &aggregate_id),
            serde_json::to_vec(&meta)?,
        )?;
        batch_put(
            &mut batch,
            state_key(&aggregate_type, &aggregate_id),
            serde_json::to_vec(&state)?,
        )?;

        self.write_batch(batch)?;
        Ok(())
    }

    pub fn transaction(&self) -> Result<Transaction<'_>> {
        self.ensure_writable()?;
        let guard = self.write_lock.lock();
        Ok(Transaction {
            store: self,
            _guard: guard,
            pending_events: Vec::new(),
            pending_records: Vec::new(),
            meta_cache: BTreeMap::new(),
            state_cache: BTreeMap::new(),
        })
    }

    pub fn get_aggregate_state(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<AggregateState> {
        let meta = self
            .load_meta(aggregate_type, aggregate_id)?
            .ok_or(EventError::AggregateNotFound)?;
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
            return Err(EventError::AggregateNotFound);
        }

        let prefix = event_prefix(aggregate_type, aggregate_id);
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));

        let mut events = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let record: EventRecord = serde_json::from_slice(&value)?;
            events.push(record);
        }

        Ok(events)
    }

    pub fn events_after(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        from_version: u64,
        limit: Option<usize>,
    ) -> Result<Vec<EventRecord>> {
        let start_key = event_key(aggregate_type, aggregate_id, from_version + 1);
        let prefix = event_prefix(aggregate_type, aggregate_id);
        let iter = self
            .db
            .iterator(IteratorMode::From(start_key.as_slice(), Direction::Forward));

        let mut events = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let record: EventRecord = serde_json::from_slice(&value)?;
            if record.version <= from_version {
                continue;
            }
            events.push(record);
            if let Some(limit) = limit {
                if events.len() >= limit {
                    break;
                }
            }
        }

        Ok(events)
    }

    pub fn list_aggregate_ids(&self, aggregate_type: &str) -> Result<Vec<String>> {
        let mut prefix = key_with_segments(&[PREFIX_META, aggregate_type]);
        prefix.push(SEP);

        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));

        let mut ids = Vec::new();
        for item in iter {
            let (key, _) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let remainder = &key[prefix.len()..];
            if remainder.is_empty() {
                continue;
            }
            let id = std::str::from_utf8(remainder)
                .map_err(|err| EventError::Storage(err.to_string()))?;
            ids.push(id.to_string());
        }
        Ok(ids)
    }

    pub fn verify(&self, aggregate_type: &str, aggregate_id: &str) -> Result<String> {
        let meta = self
            .load_meta(aggregate_type, aggregate_id)?
            .ok_or(EventError::AggregateNotFound)?;
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
            .map_err(|err| EventError::Storage(err.to_string()))?;

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
            .ok_or(EventError::AggregateNotFound)?;

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
            .map_err(|err| EventError::Storage(err.to_string()))?;

        drop(meta);
        self.get_aggregate_state(aggregate_type, aggregate_id)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            Err(EventError::Storage(
                "event store opened in read-only mode".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db
            .write(batch)
            .map_err(|err| EventError::Storage(err.to_string()))
    }

    pub fn aggregates(&self) -> Vec<AggregateState> {
        self.aggregates_paginated(0, None)
    }

    pub fn aggregates_paginated(&self, skip: usize, take: Option<usize>) -> Vec<AggregateState> {
        let prefix = meta_prefix();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut items = Vec::new();
        let mut skipped = 0usize;

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

            if skipped < skip {
                skipped += 1;
                continue;
            }

            items.push(AggregateState {
                aggregate_type: meta.aggregate_type.clone(),
                aggregate_id: meta.aggregate_id.clone(),
                version: meta.version,
                state,
                merkle_root: meta.merkle_root,
                archived: meta.archived,
            });

            if let Some(limit) = take {
                if items.len() >= limit {
                    break;
                }
            }
        }

        items
    }

    pub fn aggregate_positions(&self) -> Result<Vec<AggregatePositionEntry>> {
        let prefix = meta_prefix();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut items = Vec::new();

        for item in iter {
            let (key, value) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            if key.len() > prefix.len() && key[prefix.len()] != SEP {
                break;
            }

            let meta: AggregateMeta = serde_json::from_slice(&value)?;
            items.push(AggregatePositionEntry {
                aggregate_type: meta.aggregate_type,
                aggregate_id: meta.aggregate_id,
                version: meta.version,
            });
        }

        Ok(items)
    }

    fn load_meta(&self, aggregate_type: &str, aggregate_id: &str) -> Result<Option<AggregateMeta>> {
        let key = meta_key(aggregate_type, aggregate_id);
        let value = self
            .db
            .get(key)
            .map_err(|err| EventError::Storage(err.to_string()))?;
        if let Some(value) = value {
            Ok(Some(serde_json::from_slice(&value)?))
        } else {
            Ok(None)
        }
    }

    pub fn create_aggregate(&self, aggregate_type: &str, aggregate_id: &str) -> Result<()> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();

        if self.load_meta(aggregate_type, aggregate_id)?.is_some() {
            return Err(EventError::Storage(format!(
                "aggregate {}:{} already exists",
                aggregate_type, aggregate_id
            )));
        }

        let meta = AggregateMeta::new(aggregate_type.to_string(), aggregate_id.to_string());
        let state: BTreeMap<String, String> = BTreeMap::new();

        self.db
            .put(
                meta_key(aggregate_type, aggregate_id),
                serde_json::to_vec(&meta)?,
            )
            .map_err(|err| EventError::Storage(err.to_string()))?;

        self.db
            .put(
                state_key(aggregate_type, aggregate_id),
                serde_json::to_vec(&state)?,
            )
            .map_err(|err| EventError::Storage(err.to_string()))?;

        Ok(())
    }

    pub fn remove_aggregate(&self, aggregate_type: &str, aggregate_id: &str) -> Result<()> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();

        let Some(meta) = self.load_meta(aggregate_type, aggregate_id)? else {
            return Err(EventError::AggregateNotFound);
        };

        if meta.version > 0 {
            return Err(EventError::Storage(format!(
                "aggregate {}:{} cannot be removed while events exist",
                aggregate_type, aggregate_id
            )));
        }

        self.db
            .delete(meta_key(aggregate_type, aggregate_id))
            .map_err(|err| EventError::Storage(err.to_string()))?;

        self.db
            .delete(state_key(aggregate_type, aggregate_id))
            .map_err(|err| EventError::Storage(err.to_string()))?;

        Ok(())
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
            .map_err(|err| EventError::Storage(err.to_string()))?;
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

pub fn payload_to_map(value: &Value) -> BTreeMap<String, String> {
    fn normalize(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Null => String::new(),
            _ => value.to_string(),
        }
    }

    match value {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| (k.clone(), normalize(v)))
            .collect::<BTreeMap<_, _>>(),
        other => {
            let mut map = BTreeMap::new();
            if !other.is_null() {
                map.insert("_value".into(), normalize(other));
            }
            map
        }
    }
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

            let payload = serde_json::json!({ "name": "Alice" });

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
    fn transaction_appends_multiple_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path).unwrap();

        let mut tx = store.transaction().unwrap();
        let first = tx
            .append(AppendEvent {
                aggregate_type: "order".into(),
                aggregate_id: "order-42".into(),
                event_type: "order-created".into(),
                payload: serde_json::json!({ "status": "processing" }),
                issued_by: None,
            })
            .unwrap();
        assert_eq!(first.version, 1);

        let second = tx
            .append(AppendEvent {
                aggregate_type: "order".into(),
                aggregate_id: "order-42".into(),
                event_type: "order-updated".into(),
                payload: serde_json::json!({
                    "status": "shipped",
                    "tracking": "abc123"
                }),
                issued_by: None,
            })
            .unwrap();
        assert_eq!(second.version, 2);

        let committed = tx.commit().unwrap();
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].version, 1);
        assert_eq!(committed[1].version, 2);

        let state = store.get_aggregate_state("order", "order-42").unwrap();
        assert_eq!(state.version, 2);
        assert_eq!(state.state["status"], "shipped");
        assert_eq!(state.state["tracking"], "abc123");

        let events = store.list_events("order", "order-42").unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn transaction_without_commit_discards_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path.clone()).unwrap();

        {
            let mut tx = store.transaction().unwrap();
            tx.append(AppendEvent {
                aggregate_type: "invoice".into(),
                aggregate_id: "inv-1".into(),
                event_type: "invoice-created".into(),
                payload: serde_json::json!({ "total": "100.00" }),
                issued_by: None,
            })
            .unwrap();
        }

        let err = store.list_events("invoice", "inv-1").unwrap_err();
        assert!(matches!(err, crate::error::EventError::AggregateNotFound));
    }

    #[test]
    fn snapshots_and_archive_flow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path).unwrap();

        let payload = serde_json::json!({ "status": "active" });

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
                payload: serde_json::json!({}),
                issued_by: None,
            })
            .unwrap_err();
        assert!(matches!(err, crate::error::EventError::AggregateArchived));

        let state = store.set_archive("order", "order-1", false, None).unwrap();
        assert!(!state.archived);
    }
}
