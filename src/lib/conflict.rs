use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::store::EventRecord;

/// Describes the specific reason a replication attempt could not be applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictKind {
    VersionMismatch { expected: u64, found: u64 },
    HashMismatch { expected: String, found: String },
    MerkleMismatch { expected: String, found: String },
}

/// Represents a replication conflict raised while attempting to append a record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConflict {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub kind: ConflictKind,
    pub incoming: EventRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing: Option<EventRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
}

impl ReplicationConflict {
    pub fn new(
        aggregate_type: String,
        aggregate_id: String,
        kind: ConflictKind,
        incoming: EventRecord,
        existing: Option<EventRecord>,
    ) -> Self {
        Self {
            aggregate_type,
            aggregate_id,
            kind,
            incoming,
            existing,
            observed_at: Some(Utc::now()),
        }
    }
}

impl fmt::Display for ReplicationConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ConflictKind::VersionMismatch { expected, found } => write!(
                f,
                "replication conflict for {}::{} (version mismatch: expected next version {}, received {})",
                self.aggregate_type, self.aggregate_id, expected, found
            ),
            ConflictKind::HashMismatch { expected, found } => write!(
                f,
                "replication conflict for {}::{} (hash mismatch: expected {}, received {})",
                self.aggregate_type, self.aggregate_id, expected, found
            ),
            ConflictKind::MerkleMismatch { expected, found } => write!(
                f,
                "replication conflict for {}::{} (merkle root mismatch: expected {}, received {})",
                self.aggregate_type, self.aggregate_id, expected, found
            ),
        }
    }
}

impl std::error::Error for ReplicationConflict {}
