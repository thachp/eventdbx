use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str,
};

use chrono::{DateTime, Utc};
use parking_lot::{Mutex, MutexGuard};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use json_patch::Patch;

use super::{
    encryption::{self, Encryptor},
    error::{EventError, Result},
    merkle::{compute_merkle_root, empty_root},
    schema::MAX_EVENT_NOTE_LENGTH,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Value>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorClaims {
    pub group: String,
    pub user: String,
}

impl<'a> Transaction<'a> {
    pub fn append(&mut self, input: AppendEvent) -> Result<EventRecord> {
        let key = AggregateKey::new(input.aggregate_type.clone(), input.aggregate_id.clone());
        let (meta, state) = self.ensure_cache(&key)?;
        if meta.archived {
            return Err(EventError::AggregateArchived);
        }
        if let Some(note) = input.note.as_ref() {
            if note.chars().count() > MAX_EVENT_NOTE_LENGTH {
                return Err(EventError::InvalidSchema(format!(
                    "event note cannot exceed {} characters",
                    MAX_EVENT_NOTE_LENGTH
                )));
            }
        }
        let record = apply_event(meta, state, input);
        let stored_record = self.store.encode_record(&record)?;
        let event_value = serde_json::to_vec(&stored_record)?;

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
            let state_bytes = self.store.encode_state_map(&state)?;
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
        metadata,
        issued_by,
        note,
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
        extensions: metadata,
        metadata: EventMetadata {
            event_id,
            created_at,
            issued_by,
            note,
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

fn decode_pointer_segment(segment: &str) -> String {
    let mut result = String::with_capacity(segment.len());
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            if let Some(next) = chars.next() {
                match next {
                    '0' => result.push('~'),
                    '1' => result.push('/'),
                    other => {
                        result.push('~');
                        result.push(other);
                    }
                }
            } else {
                result.push('~');
            }
        } else {
            result.push(ch);
        }
    }
    result
}

fn top_level_key_from_path(path: &str) -> std::result::Result<String, String> {
    if !path.starts_with('/') {
        return Err(format!("patch path '{}' must start with '/'", path));
    }
    let trimmed = &path[1..];
    if trimmed.is_empty() {
        return Err("patch operations targeting the document root are not supported".into());
    }
    let segment = trimmed
        .split('/')
        .next()
        .expect("split returns at least one segment");
    Ok(decode_pointer_segment(segment))
}

fn collect_top_level_keys(patch: &Value) -> std::result::Result<BTreeSet<String>, String> {
    let array = patch
        .as_array()
        .ok_or_else(|| "patch must be an array".to_string())?;
    let mut keys = BTreeSet::new();
    for entry in array {
        let op = entry
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| "patch entry is missing 'op'".to_string())?;
        if op != "add" && op != "replace" {
            return Err(format!("unsupported patch operation '{}'", op));
        }
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "patch entry is missing 'path'".to_string())?;
        let key = top_level_key_from_path(path)?;
        keys.insert(key);
    }
    Ok(keys)
}

fn parse_state_value(raw: &str) -> Value {
    if let Ok(parsed) = serde_json::from_str(raw) {
        return parsed;
    }
    Value::String(raw.to_string())
}

fn state_map_to_value(map: &BTreeMap<String, String>) -> Value {
    let mut object = serde_json::Map::new();
    for (key, value) in map {
        object.insert(key.clone(), parse_state_value(value));
    }
    Value::Object(object)
}

#[derive(Debug, Clone)]
pub struct AppendEvent {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: Value,
    pub metadata: Option<Value>,
    pub issued_by: Option<ActorClaims>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    encryptor: Option<Encryptor>,
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
    pub fn open(path: PathBuf, encryptor: Option<Encryptor>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DBWithThreadMode::<MultiThreaded>::open(&options, path)
            .map_err(|err| EventError::Storage(err.to_string()))?;

        Ok(Self {
            db,
            write_lock: Mutex::new(()),
            read_only: false,
            encryptor,
        })
    }

    pub fn open_read_only(path: PathBuf, encryptor: Option<Encryptor>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(false);
        let db = DBWithThreadMode::<MultiThreaded>::open_for_read_only(&options, path, false)
            .map_err(|err| EventError::Storage(err.to_string()))?;

        Ok(Self {
            db,
            write_lock: Mutex::new(()),
            read_only: true,
            encryptor,
        })
    }

    pub fn prepare_payload_from_patch(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        patch_value: &Value,
    ) -> Result<Value> {
        if !patch_value.is_array() {
            return Err(EventError::InvalidSchema(
                "patch must be provided as a JSON array".into(),
            ));
        }

        let top_level_keys =
            collect_top_level_keys(patch_value).map_err(EventError::InvalidSchema)?;
        if top_level_keys.is_empty() {
            return Err(EventError::InvalidSchema(
                "patch must modify at least one path".into(),
            ));
        }

        let patch: Patch = serde_json::from_value(patch_value.clone())
            .map_err(|err| EventError::InvalidSchema(format!("failed to parse patch: {}", err)))?;

        let state_map = self.load_state_map(aggregate_type, aggregate_id)?;
        let mut document = state_map_to_value(&state_map);
        if !document.is_object() {
            document = Value::Object(serde_json::Map::new());
        }

        json_patch::patch(&mut document, &patch)
            .map_err(|err| EventError::InvalidSchema(format!("failed to apply patch: {}", err)))?;

        let document_object = document.as_object().ok_or_else(|| {
            EventError::InvalidSchema("patched document must be a JSON object".into())
        })?;

        let mut payload = serde_json::Map::new();
        for key in top_level_keys {
            let value = document_object.get(&key).ok_or_else(|| {
                EventError::InvalidSchema(format!(
                    "patch removed key '{}' and removals are not supported",
                    key
                ))
            })?;
            payload.insert(key, value.clone());
        }

        Ok(Value::Object(payload))
    }

    pub fn append(&self, input: AppendEvent) -> Result<EventRecord> {
        self.ensure_writable()?;
        let _guard = self.write_lock.lock();

        if let Some(note) = input.note.as_ref() {
            if note.chars().count() > MAX_EVENT_NOTE_LENGTH {
                return Err(EventError::InvalidSchema(format!(
                    "event note cannot exceed {} characters",
                    MAX_EVENT_NOTE_LENGTH
                )));
            }
        }

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
        let stored_record = self.encode_record(&record)?;
        let encoded_state = self.encode_state_map(&state)?;

        let mut batch = WriteBatch::default();
        batch_put(
            &mut batch,
            event_key(&record.aggregate_type, &record.aggregate_id, record.version),
            serde_json::to_vec(&stored_record)?,
        )?;
        batch_put(
            &mut batch,
            meta_key(&record.aggregate_type, &record.aggregate_id),
            serde_json::to_vec(&meta)?,
        )?;
        batch_put(
            &mut batch,
            state_key(&record.aggregate_type, &record.aggregate_id),
            encoded_state,
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

        let stored_record = self.encode_record(&record)?;

        let mut batch = WriteBatch::default();
        batch_put(
            &mut batch,
            event_key(&aggregate_type, &aggregate_id, record.version),
            serde_json::to_vec(&stored_record)?,
        )?;
        batch_put(
            &mut batch,
            meta_key(&aggregate_type, &aggregate_id),
            serde_json::to_vec(&meta)?,
        )?;
        batch_put(
            &mut batch,
            state_key(&aggregate_type, &aggregate_id),
            self.encode_state_map(&state)?,
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
            let record = self.decode_record(record)?;
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
            let record = self.decode_record(record)?;
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

    pub fn aggregate_version(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<u64>> {
        let meta = self.load_meta(aggregate_type, aggregate_id)?;
        Ok(meta.map(|meta| meta.version))
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

    fn encode_record(&self, record: &EventRecord) -> Result<EventRecord> {
        if let Some(enc) = &self.encryptor {
            if encryption::extract_encrypted_value(&record.payload).is_some() {
                return Ok(record.clone());
            }
            let payload_bytes = serde_json::to_vec(&record.payload)?;
            let ciphertext = enc.encrypt_to_string(&payload_bytes)?;
            let mut stored = record.clone();
            stored.payload = encryption::wrap_encrypted_value(ciphertext);
            Ok(stored)
        } else {
            Ok(record.clone())
        }
    }

    fn decode_record(&self, mut record: EventRecord) -> Result<EventRecord> {
        if let Some(enc) = &self.encryptor {
            if let Some(ciphertext) = encryption::extract_encrypted_value(&record.payload) {
                let bytes = enc.decrypt_from_str(ciphertext)?;
                record.payload = serde_json::from_slice(&bytes)?;
            }
            Ok(record)
        } else {
            if encryption::extract_encrypted_value(&record.payload).is_some() {
                return Err(EventError::Config(
                    "data encryption key must be configured to read encrypted events".to_string(),
                ));
            }
            Ok(record)
        }
    }

    fn encode_state_map(&self, state: &BTreeMap<String, String>) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(state)?;
        if let Some(enc) = &self.encryptor {
            let ciphertext = enc.encrypt_to_string(&json)?;
            Ok(ciphertext.into_bytes())
        } else {
            Ok(json)
        }
    }

    fn decode_state_map(&self, bytes: &[u8]) -> Result<BTreeMap<String, String>> {
        if let Some(enc) = &self.encryptor {
            let text = str::from_utf8(bytes).map_err(|err| EventError::Storage(err.to_string()))?;
            if encryption::is_encrypted_blob(text.trim()) {
                let decrypted = enc.decrypt_from_str(text.trim())?;
                Ok(serde_json::from_slice(&decrypted)?)
            } else if text.trim().is_empty() {
                Ok(BTreeMap::new())
            } else {
                Ok(serde_json::from_str(text)?)
            }
        } else {
            if let Ok(text) = str::from_utf8(bytes) {
                if encryption::is_encrypted_blob(text.trim()) {
                    return Err(EventError::Config(
                        "data encryption key must be configured to read encrypted aggregates"
                            .to_string(),
                    ));
                }
            }
            Ok(serde_json::from_slice(bytes)?)
        }
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
            self.decode_state_map(&value)
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
            let store = EventStore::open(path.clone(), None).unwrap();

            let payload = serde_json::json!({ "name": "Alice" });

            let record = store
                .append(AppendEvent {
                    aggregate_type: "patient".into(),
                    aggregate_id: "patient-1".into(),
                    event_type: "patient-created".into(),
                    payload: payload.clone(),
                    metadata: None,
                    issued_by: None,
                    note: None,
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
        let store = EventStore::open(path, None).unwrap();

        let mut tx = store.transaction().unwrap();
        let first = tx
            .append(AppendEvent {
                aggregate_type: "order".into(),
                aggregate_id: "order-42".into(),
                event_type: "order-created".into(),
                payload: serde_json::json!({ "status": "processing" }),
                metadata: None,
                issued_by: None,
                note: None,
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
                metadata: None,
                issued_by: None,
                note: None,
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
        let store = EventStore::open(path.clone(), None).unwrap();

        {
            let mut tx = store.transaction().unwrap();
            tx.append(AppendEvent {
                aggregate_type: "invoice".into(),
                aggregate_id: "inv-1".into(),
                event_type: "invoice-created".into(),
                payload: serde_json::json!({ "total": "100.00" }),
                metadata: None,
                issued_by: None,
                note: None,
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
        let store = EventStore::open(path, None).unwrap();

        let payload = serde_json::json!({ "status": "active" });

        store
            .append(AppendEvent {
                aggregate_type: "order".into(),
                aggregate_id: "order-1".into(),
                event_type: "order-created".into(),
                payload,
                metadata: None,
                issued_by: None,
                note: None,
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
                metadata: None,
                issued_by: None,
                note: None,
            })
            .unwrap_err();
        assert!(matches!(err, crate::error::EventError::AggregateArchived));

        let state = store.set_archive("order", "order-1", false, None).unwrap();
        assert!(!state.archived);
    }

    #[test]
    fn append_event_with_note_persists_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path, None).unwrap();

        let note_text = "initial import".to_string();
        let record = store
            .append(AppendEvent {
                aggregate_type: "account".into(),
                aggregate_id: "acct-1".into(),
                event_type: "account-created".into(),
                payload: serde_json::json!({ "status": "active" }),
                metadata: None,
                issued_by: None,
                note: Some(note_text.clone()),
            })
            .unwrap();

        assert_eq!(record.metadata.note.as_deref(), Some(note_text.as_str()));
    }

    #[test]
    fn reject_event_note_exceeding_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path, None).unwrap();

        let long_note = "x".repeat(MAX_EVENT_NOTE_LENGTH + 1);
        let err = store
            .append(AppendEvent {
                aggregate_type: "account".into(),
                aggregate_id: "acct-2".into(),
                event_type: "account-created".into(),
                payload: serde_json::json!({ "status": "pending" }),
                metadata: None,
                issued_by: None,
                note: Some(long_note),
            })
            .unwrap_err();

        assert!(matches!(err, crate::error::EventError::InvalidSchema(_)));
    }

    #[test]
    fn append_event_with_extensions_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path.clone(), None).unwrap();

        let metadata = serde_json::json!({"@plugin": {"mode": "debug"}});
        let record = store
            .append(AppendEvent {
                aggregate_type: "account".into(),
                aggregate_id: "acct-3".into(),
                event_type: "account-created".into(),
                payload: serde_json::json!({ "status": "active" }),
                metadata: Some(metadata.clone()),
                issued_by: None,
                note: None,
            })
            .unwrap();

        assert_eq!(record.extensions, Some(metadata.clone()));

        drop(store);
        let reopened = EventStore::open(path, None).unwrap();
        let events = reopened.list_events("account", "acct-3").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].extensions, Some(metadata));
    }

    #[test]
    fn prepare_payload_from_patch_updates_nested_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path.clone(), None).unwrap();

        store
            .append(AppendEvent {
                aggregate_type: "patient".into(),
                aggregate_id: "p-1".into(),
                event_type: "patient-created".into(),
                payload: serde_json::json!({
                    "status": "active",
                    "contact": {
                        "address": { "city": "Portland" }
                    }
                }),
                metadata: None,
                issued_by: None,
                note: None,
            })
            .unwrap();

        let patch = serde_json::json!([
            { "op": "replace", "path": "/status", "value": "inactive" },
            { "op": "replace", "path": "/contact/address/city", "value": "Spokane" }
        ]);

        let payload = store
            .prepare_payload_from_patch("patient", "p-1", &patch)
            .unwrap();

        assert_eq!(
            payload,
            serde_json::json!({
                "contact": { "address": { "city": "Spokane" } },
                "status": "inactive"
            })
        );
    }

    #[test]
    fn prepare_payload_from_patch_rejects_unsupported_operations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path, None).unwrap();

        let patch = serde_json::json!([
            { "op": "remove", "path": "/status" }
        ]);

        let err = store
            .prepare_payload_from_patch("patient", "p-unknown", &patch)
            .unwrap_err();

        assert!(matches!(err, crate::error::EventError::InvalidSchema(_)));
    }

    #[test]
    fn prepare_payload_from_patch_supports_add_on_new_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event_store");
        let store = EventStore::open(path, None).unwrap();

        let patch = serde_json::json!([
            { "op": "add", "path": "/status", "value": "inactive" }
        ]);

        let payload = store
            .prepare_payload_from_patch("patient", "p-new", &patch)
            .unwrap();

        assert_eq!(
            payload,
            serde_json::json!({
                "status": "inactive"
            })
        );
    }
}
