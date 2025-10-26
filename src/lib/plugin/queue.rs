use std::{fs, path::Path, sync::Arc};

use chrono::Utc;
use parking_lot::Mutex;
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use tracing::warn;

use crate::{
    error::{EventError, Result},
    schema::AggregateSchema,
    snowflake::{SnowflakeGenerator, SnowflakeId},
    store::{AggregateState, EventRecord},
};

const SEP: u8 = 0x1F;
const PREFIX_JOB: &[u8] = b"job";
const PREFIX_STATUS: &[u8] = b"status";

const PROCESSING_TIMEOUT_MS: i64 = 60_000;

#[derive(Serialize, Deserialize, Clone)]
pub struct JobRecord {
    pub id: u64,
    pub plugin: String,
    pub payload: Value,
    pub status: JobStatus,
    pub attempts: u8,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub next_retry_at: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Processing,
    Done,
    Dead,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Processing => "processing",
            JobStatus::Done => "done",
            JobStatus::Dead => "dead",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct JobPayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<EventRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<AggregateState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<AggregateSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy: Option<LegacyJobInfo>,
}

impl JobPayload {
    pub fn is_empty(&self) -> bool {
        self.record.is_none() && self.state.is_none() && self.schema.is_none()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LegacyJobInfo {
    pub event_id: SnowflakeId,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
}

#[derive(Default)]
pub struct PluginQueueStatus {
    pub pending: Vec<JobRecord>,
    pub processing: Vec<JobRecord>,
    pub done: Vec<JobRecord>,
    pub dead: Vec<JobRecord>,
}

#[derive(Clone)]
pub struct PluginQueueStore {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    id_generator: Arc<Mutex<SnowflakeGenerator>>,
}

impl PluginQueueStore {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_internal(path)
    }

    pub fn open_with_legacy(db_path: &Path, legacy_json_path: &Path) -> Result<Self> {
        let store = Self::open_internal(db_path)?;
        store.migrate_from_legacy(legacy_json_path)?;
        Ok(store)
    }

    pub fn enqueue_job(&self, plugin: &str, payload: Value, created_at: i64) -> Result<JobRecord> {
        let id = self.next_id().as_u64();
        let record = JobRecord {
            id,
            plugin: plugin.to_string(),
            payload,
            status: JobStatus::Pending,
            attempts: 0,
            last_error: None,
            created_at,
            next_retry_at: None,
        };

        let mut batch = WriteBatch::default();
        batch.put(job_key(id), serde_json::to_vec(&record)?);
        batch.put(status_key(JobStatus::Pending, plugin, id), []);

        self.db
            .write(batch)
            .map_err(|err| EventError::Storage(err.to_string()))?;

        metrics::counter!(
            "eventdbx_plugin_jobs_enqueued_total",
            1,
            "plugin" => plugin.to_string()
        );

        Ok(record)
    }

    pub fn dispatch_jobs(&self, plugin: &str, limit: usize, now: i64) -> Result<Vec<JobRecord>> {
        let prefix = status_prefix(JobStatus::Pending, plugin);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut selected: Vec<JobRecord> = Vec::new();
        let mut batch = WriteBatch::default();

        while selected.len() < limit {
            let Some(item) = iter.next() else {
                break;
            };
            let (key, _) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }

            let job_id = parse_job_id(&key)?;
            let mut job = match self.load_job(job_id)? {
                Some(job) => job,
                None => {
                    batch.delete(key.clone());
                    continue;
                }
            };

            if job.status != JobStatus::Pending {
                batch.delete(key.clone());
                continue;
            }

            if let Some(next_retry_at) = job.next_retry_at {
                if next_retry_at > now {
                    continue;
                }
            }

            job.status = JobStatus::Processing;
            job.attempts = job.attempts.saturating_add(1);
            job.next_retry_at = Some(now + PROCESSING_TIMEOUT_MS);

            batch.delete(key.clone());
            batch.put(status_key(JobStatus::Processing, plugin, job.id), []);
            batch.put(job_key(job.id), serde_json::to_vec(&job)?);

            selected.push(job);
        }

        if !selected.is_empty() {
            self.db
                .write(batch)
                .map_err(|err| EventError::Storage(err.to_string()))?;
        }

        Ok(selected)
    }

    pub fn complete_job(&self, job_id: u64) -> Result<JobRecord> {
        let job = self.transition_job(job_id, |job| {
            job.status = JobStatus::Done;
            job.next_retry_at = None;
            job.last_error = None;
        })?;

        metrics::counter!(
            "eventdbx_plugin_jobs_completed_total",
            1,
            "plugin" => job.plugin.clone()
        );

        Ok(job)
    }

    pub fn fail_job(
        &self,
        job_id: u64,
        error: String,
        now: i64,
        max_attempts: u8,
    ) -> Result<JobRecord> {
        let job = self.transition_job(job_id, |job| {
            job.last_error = Some(error);
            if job.attempts >= max_attempts {
                job.status = JobStatus::Dead;
                job.next_retry_at = None;
            } else {
                job.status = JobStatus::Pending;
                let backoff = backoff_delay(job.attempts);
                job.next_retry_at = Some(now + backoff);
            }
        })?;

        metrics::counter!(
            "eventdbx_plugin_jobs_failed_total",
            1,
            "plugin" => job.plugin.clone()
        );

        Ok(job)
    }

    pub fn recover_stuck_jobs(
        &self,
        plugin: &str,
        now: i64,
        max_attempts: u8,
    ) -> Result<Vec<JobRecord>> {
        let prefix = status_prefix(JobStatus::Processing, plugin);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut recovered = Vec::new();

        while let Some(item) = iter.next() {
            let (key, _) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }

            let job_id = parse_job_id(&key)?;
            let Some(job) = self.load_job(job_id)? else {
                continue;
            };

            if job.status != JobStatus::Processing {
                continue;
            }

            if let Some(deadline) = job.next_retry_at {
                if deadline > now {
                    continue;
                }
            }

            let updated = self.transition_job(job_id, |job| {
                if job.attempts >= max_attempts {
                    job.status = JobStatus::Dead;
                    job.next_retry_at = None;
                } else {
                    job.status = JobStatus::Pending;
                    let backoff = backoff_delay(job.attempts);
                    job.next_retry_at = Some(now + backoff);
                }
            })?;
            recovered.push(updated);
        }

        Ok(recovered)
    }

    pub fn retry_dead_job(&self, job_id: u64, when: i64) -> Result<JobRecord> {
        let job = self
            .load_job(job_id)?
            .ok_or_else(|| EventError::Storage(format!("job {} not found", job_id)))?;
        if job.status != JobStatus::Dead {
            return Err(EventError::Config(format!("job {} is not dead", job_id)));
        }
        self.transition_job(job_id, |job| {
            job.status = JobStatus::Pending;
            job.next_retry_at = Some(when);
        })
    }

    pub fn clear_dead(&self) -> Result<usize> {
        let prefix = status_prefix_any(JobStatus::Dead);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut batch = WriteBatch::default();
        let mut removed = 0usize;

        while let Some(item) = iter.next() {
            let (key, _) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let job_id = parse_job_id(&key)?;
            batch.delete(key.clone());
            batch.delete(job_key(job_id));
            removed += 1;
        }

        if removed > 0 {
            self.db
                .write(batch)
                .map_err(|err| EventError::Storage(err.to_string()))?;
        }

        Ok(removed)
    }

    pub fn clear_done(&self, older_than: Option<i64>) -> Result<usize> {
        let prefix = status_prefix_any(JobStatus::Done);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut batch = WriteBatch::default();
        let mut removed = 0usize;

        while let Some(item) = iter.next() {
            let (key, _) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let job_id = parse_job_id(&key)?;
            let Some(job) = self.load_job(job_id)? else {
                batch.delete(key.clone());
                continue;
            };

            if let Some(cutoff) = older_than {
                if job.created_at >= cutoff {
                    continue;
                }
            }

            batch.delete(key.clone());
            batch.delete(job_key(job_id));
            removed += 1;
        }

        if removed > 0 {
            self.db
                .write(batch)
                .map_err(|err| EventError::Storage(err.to_string()))?;
        }

        Ok(removed)
    }

    pub fn truncate_done(&self, retain: usize) -> Result<usize> {
        if retain == 0 {
            return self.clear_done(None);
        }

        let prefix = status_prefix_any(JobStatus::Done);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut keys: Vec<Vec<u8>> = Vec::new();

        while let Some(item) = iter.next() {
            let (key, _) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            keys.push(key.to_vec());
        }

        if keys.len() <= retain {
            return Ok(0);
        }

        let to_remove = keys.len().saturating_sub(retain);
        let mut batch = WriteBatch::default();
        let mut removed = 0usize;

        for key in keys.into_iter().take(to_remove) {
            let job_id = parse_job_id(&key)?;
            batch.delete(key);
            batch.delete(job_key(job_id));
            removed += 1;
        }

        if removed > 0 {
            self.db
                .write(batch)
                .map_err(|err| EventError::Storage(err.to_string()))?;
        }

        Ok(removed)
    }

    pub fn status(&self) -> Result<PluginQueueStatus> {
        let mut status = PluginQueueStatus::default();
        status.pending = self.collect_jobs(JobStatus::Pending)?;
        status.processing = self.collect_jobs(JobStatus::Processing)?;
        status.done = self.collect_jobs(JobStatus::Done)?;
        status.dead = self.collect_jobs(JobStatus::Dead)?;
        metrics::gauge!(
            "eventdbx_plugin_queue_jobs",
            status.pending.len() as f64,
            "state" => "pending"
        );
        metrics::gauge!(
            "eventdbx_plugin_queue_jobs",
            status.processing.len() as f64,
            "state" => "processing"
        );
        metrics::gauge!(
            "eventdbx_plugin_queue_jobs",
            status.done.len() as f64,
            "state" => "done"
        );
        metrics::gauge!(
            "eventdbx_plugin_queue_jobs",
            status.dead.len() as f64,
            "state" => "dead"
        );
        Ok(status)
    }

    pub fn load_job(&self, job_id: u64) -> Result<Option<JobRecord>> {
        let key = job_key(job_id);
        let value = match self
            .db
            .get(&key)
            .map_err(|err| EventError::Storage(err.to_string()))?
        {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let job = serde_json::from_slice(&value)?;
        Ok(Some(job))
    }

    fn collect_jobs(&self, status: JobStatus) -> Result<Vec<JobRecord>> {
        let prefix = status_prefix_any(status);
        let mut iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        let mut items = Vec::new();

        while let Some(item) = iter.next() {
            let (key, _) = item.map_err(|err| EventError::Storage(err.to_string()))?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let job_id = parse_job_id(&key)?;
            if let Some(job) = self.load_job(job_id)? {
                items.push(job);
            }
        }

        Ok(items)
    }

    fn transition_job<F>(&self, job_id: u64, update: F) -> Result<JobRecord>
    where
        F: FnOnce(&mut JobRecord),
    {
        let mut job = self
            .load_job(job_id)?
            .ok_or_else(|| EventError::Storage(format!("job {} not found", job_id)))?;
        let old_status = job.status;
        let plugin = job.plugin.clone();

        update(&mut job);

        let mut batch = WriteBatch::default();
        batch.put(job_key(job.id), serde_json::to_vec(&job)?);
        batch.delete(status_key(old_status, &plugin, job.id));
        batch.put(status_key(job.status, &plugin, job.id), []);

        self.db
            .write(batch)
            .map_err(|err| EventError::Storage(err.to_string()))?;

        Ok(job)
    }

    fn next_id(&self) -> SnowflakeId {
        let mut guard = self.id_generator.lock();
        guard.next_id()
    }

    fn open_internal(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|err| {
                    EventError::Storage(format!(
                        "failed to create plugin queue directory {}: {}",
                        parent.display(),
                        err
                    ))
                })?;
            }
        }

        let mut options = Options::default();
        options.create_if_missing(true);

        let db = DBWithThreadMode::<MultiThreaded>::open(&options, path).map_err(|err| {
            EventError::Storage(format!(
                "failed to open plugin queue store at {}: {}",
                path.display(),
                err
            ))
        })?;

        Ok(Self {
            db: Arc::new(db),
            id_generator: Arc::new(Mutex::new(SnowflakeGenerator::new(0))),
        })
    }

    fn migrate_from_legacy(&self, legacy_path: &Path) -> Result<()> {
        if !legacy_path.exists() {
            return Ok(());
        }

        if !self.is_empty()? {
            return Ok(());
        }

        let contents = fs::read_to_string(legacy_path).map_err(|err| {
            EventError::Storage(format!(
                "failed to read legacy queue file at {}: {}",
                legacy_path.display(),
                err
            ))
        })?;

        let trimmed = contents.trim();
        if trimmed.is_empty() {
            self.archive_legacy_file(legacy_path);
            return Ok(());
        }

        #[derive(Deserialize)]
        struct LegacyEvent {
            event_id: SnowflakeId,
            aggregate_type: String,
            aggregate_id: String,
            event_type: String,
            _attempts: u32,
        }

        #[derive(Deserialize)]
        struct LegacyQueue {
            #[serde(default)]
            pending_events: Vec<LegacyEvent>,
            #[serde(default)]
            dead_events: Vec<LegacyEvent>,
        }

        let legacy: LegacyQueue = serde_json::from_str(trimmed)?;

        for event in legacy.pending_events.into_iter() {
            let payload = legacy_payload(
                event.event_id,
                event.aggregate_type,
                event.aggregate_id,
                event.event_type,
            );
            let _ = self.enqueue_job("legacy", payload, Utc::now().timestamp_millis())?;
        }

        for event in legacy.dead_events.into_iter() {
            let payload = legacy_payload(
                event.event_id,
                event.aggregate_type,
                event.aggregate_id,
                event.event_type,
            );
            let job = self.enqueue_job("legacy", payload, Utc::now().timestamp_millis())?;
            let _ = self.fail_job(
                job.id,
                "migrated from legacy dead queue".to_string(),
                Utc::now().timestamp_millis(),
                u8::MAX,
            )?;
        }

        self.archive_legacy_file(legacy_path);
        Ok(())
    }

    fn is_empty(&self) -> Result<bool> {
        let mut iter = self.db.iterator(IteratorMode::Start);
        match iter.next() {
            None => Ok(true),
            Some(Ok(_)) => Ok(false),
            Some(Err(err)) => Err(EventError::Storage(format!(
                "failed to read plugin queue: {}",
                err
            ))),
        }
    }

    fn archive_legacy_file(&self, legacy_path: &Path) {
        let backup = if let Some(ext) = legacy_path.extension() {
            legacy_path.with_extension(format!("{}.migrated", ext.to_string_lossy()))
        } else {
            legacy_path.with_extension("migrated")
        };

        if let Err(err) = fs::rename(legacy_path, &backup) {
            warn!(
                target: "eventdbx.plugin",
                "failed to archive legacy queue file {} ({})",
                legacy_path.display(),
                err
            );
            if let Err(remove_err) = fs::remove_file(legacy_path) {
                warn!(
                    target: "eventdbx.plugin",
                    "failed to remove legacy queue file {} ({})",
                    legacy_path.display(),
                    remove_err
                );
            }
        }
    }
}

fn job_key(id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_JOB.len() + 1 + 8);
    key.extend_from_slice(PREFIX_JOB);
    key.push(SEP);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn status_key(status: JobStatus, plugin: &str, id: u64) -> Vec<u8> {
    let mut key = status_prefix(status, plugin);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn status_prefix(status: JobStatus, plugin: &str) -> Vec<u8> {
    let mut key = status_prefix_any(status);
    key.extend_from_slice(plugin.as_bytes());
    key.push(SEP);
    key
}

fn status_prefix_any(status: JobStatus) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_STATUS.len() + 1 + status.as_str().len() + 1);
    key.extend_from_slice(PREFIX_STATUS);
    key.push(SEP);
    key.extend_from_slice(status.as_str().as_bytes());
    key.push(SEP);
    key
}

fn parse_job_id(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(EventError::Storage("job key too short".into()));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&key[key.len() - 8..]);
    Ok(u64::from_be_bytes(buf))
}

fn backoff_delay(attempts: u8) -> i64 {
    match attempts {
        0 | 1 => 1_000,
        2 => 2_000,
        3 => 4_000,
        _ => 10_000,
    }
}

fn legacy_payload(
    event_id: SnowflakeId,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
) -> Value {
    serde_json::to_value(JobPayload {
        legacy: Some(LegacyJobInfo {
            event_id,
            aggregate_type,
            aggregate_id,
            event_type,
        }),
        ..Default::default()
    })
    .expect("legacy payload serialization should succeed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use chrono::Utc;
    use serde_json::json;
    use tempfile::TempDir;

    fn open_store(tmp: &TempDir) -> Result<PluginQueueStore> {
        let path = tmp.path().join("queue.db");
        PluginQueueStore::open(path.as_path())
    }

    #[test]
    fn clear_done_removes_records() -> Result<()> {
        let tmp = TempDir::new().expect("create temp dir");
        let store = open_store(&tmp)?;
        let now = Utc::now().timestamp_millis();

        let job = store.enqueue_job("rest", json!({}), now)?;
        store.complete_job(job.id)?;

        assert_eq!(store.clear_done(None)?, 1);
        assert!(store.load_job(job.id)?.is_none());

        Ok(())
    }

    #[test]
    fn clear_done_respects_cutoff() -> Result<()> {
        let tmp = TempDir::new().expect("create temp dir");
        let store = open_store(&tmp)?;
        let now = Utc::now().timestamp_millis();
        let earlier = now - 60_000;

        let old_job = store.enqueue_job("rest", json!({ "id": 1 }), earlier)?;
        let new_job = store.enqueue_job("rest", json!({ "id": 2 }), now)?;
        store.complete_job(old_job.id)?;
        store.complete_job(new_job.id)?;

        let cutoff = now - 1_000;
        assert_eq!(store.clear_done(Some(cutoff))?, 1);
        assert!(store.load_job(old_job.id)?.is_none());
        assert!(store.load_job(new_job.id)?.is_some());

        Ok(())
    }

    #[test]
    fn truncate_done_enforces_limit() -> Result<()> {
        let tmp = TempDir::new().expect("create temp dir");
        let store = open_store(&tmp)?;
        let now = Utc::now().timestamp_millis();

        for idx in 0..5 {
            let job = store.enqueue_job("rest", json!({ "seq": idx }), now + idx as i64)?;
            store.complete_job(job.id)?;
        }

        let removed = store.truncate_done(2)?;
        assert_eq!(removed, 3);
        let status = store.status()?;
        assert_eq!(status.done.len(), 2);

        Ok(())
    }

    #[test]
    fn truncate_done_zero_clears_all() -> Result<()> {
        let tmp = TempDir::new().expect("create temp dir");
        let store = open_store(&tmp)?;
        let now = Utc::now().timestamp_millis();

        let job = store.enqueue_job("rest", json!({}), now)?;
        store.complete_job(job.id)?;

        assert_eq!(store.truncate_done(0)?, 1);
        assert!(store.status()?.done.is_empty());
        Ok(())
    }
}
