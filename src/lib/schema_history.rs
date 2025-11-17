use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::{
    error::{EventError, Result},
    schema::AggregateSchema,
};

const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaManifest {
    #[serde(default = "default_manifest_version")]
    pub manifest_version: u32,
    #[serde(default)]
    pub active_version: Option<String>,
    #[serde(default)]
    pub versions: Vec<SchemaVersionEntry>,
    #[serde(default)]
    pub audit_log: Vec<SchemaAuditEntry>,
}

impl Default for SchemaManifest {
    fn default() -> Self {
        Self {
            manifest_version: MANIFEST_VERSION,
            active_version: None,
            versions: Vec::new(),
            audit_log: Vec::new(),
        }
    }
}

fn default_manifest_version() -> u32 {
    MANIFEST_VERSION
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersionEntry {
    pub id: String,
    pub file: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaAuditEntry {
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    pub action: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl SchemaAuditEntry {
    fn new(
        action: SchemaAuditAction,
        version: impl Into<String>,
        actor: Option<&str>,
        reason: Option<&str>,
        details: Option<String>,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            actor: actor.and_then(normalize_string),
            action: action.as_str().to_string(),
            version: version.into(),
            reason: reason.and_then(normalize_string),
            details,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaAuditAction {
    Publish,
    Activate,
    Rollback,
}

impl SchemaAuditAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Activate => "activate",
            Self::Rollback => "rollback",
        }
    }
}

#[derive(Debug)]
pub struct SchemaHistoryManager {
    root: PathBuf,
    manifest_path: PathBuf,
    versions_dir: PathBuf,
    active_schema_path: PathBuf,
}

impl SchemaHistoryManager {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let manifest_root = root.join("schemas");
        Self {
            manifest_path: manifest_root.join("schema_manifest.json"),
            versions_dir: manifest_root.join("versions"),
            active_schema_path: root.join("schemas.json"),
            root,
        }
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn versions_dir(&self) -> &Path {
        &self.versions_dir
    }

    pub fn active_schema_path(&self) -> &Path {
        &self.active_schema_path
    }

    pub fn manifest(&self) -> Result<SchemaManifest> {
        if self.manifest_path.exists() {
            let contents = fs::read_to_string(&self.manifest_path)?;
            if contents.trim().is_empty() {
                Ok(SchemaManifest::default())
            } else {
                Ok(serde_json::from_str(&contents)?)
            }
        } else {
            Ok(SchemaManifest::default())
        }
    }

    fn load_or_init_manifest(&self) -> Result<SchemaManifest> {
        if self.manifest_path.exists() {
            self.manifest()
        } else {
            let manifest = SchemaManifest::default();
            self.persist_manifest(&manifest)?;
            Ok(manifest)
        }
    }

    pub fn publish(&self, options: PublishOptions<'_>) -> Result<SchemaPublishOutcome> {
        let mut manifest = self.load_or_init_manifest()?;
        let canonical = Self::canonicalize_schema(options.schema_json)?;
        let checksum = checksum_for(&canonical);

        if options.skip_if_identical {
            if let Some(latest) = manifest.versions.last().cloned() {
                if latest.checksum.as_deref() == Some(&checksum) {
                    let mut activated = false;
                    if options.activate
                        && manifest.active_version.as_deref() != Some(latest.id.as_str())
                    {
                        activated = self.apply_activation(
                            &mut manifest,
                            &latest,
                            options.actor,
                            options.reason,
                            SchemaAuditAction::Activate,
                            Some(&canonical),
                        )?;
                    }
                    self.persist_manifest(&manifest)?;
                    return Ok(SchemaPublishOutcome {
                        manifest,
                        version_id: latest.id.clone(),
                        activated,
                        skipped: true,
                    });
                }
            }
        }

        let version_id = self.next_version_id(&manifest);
        let entry = SchemaVersionEntry {
            id: version_id.clone(),
            file: self.relative_version_path(&version_id),
            created_at: Utc::now(),
            author: options.actor.and_then(normalize_string),
            reason: options.reason.and_then(normalize_string),
            parent: manifest.versions.last().map(|existing| existing.id.clone()),
            checksum: Some(checksum),
            labels: options
                .labels
                .iter()
                .filter_map(|label| normalize_string(label))
                .collect(),
        };

        self.write_version_payload(&entry.file, &canonical)?;
        manifest.versions.push(entry.clone());
        manifest.audit_log.push(SchemaAuditEntry::new(
            SchemaAuditAction::Publish,
            &entry.id,
            options.actor,
            options.reason,
            None,
        ));

        let mut activated = false;
        if options.activate {
            activated = self.apply_activation(
                &mut manifest,
                &entry,
                options.actor,
                options.reason,
                SchemaAuditAction::Activate,
                Some(&canonical),
            )?;
        }

        self.persist_manifest(&manifest)?;
        Ok(SchemaPublishOutcome {
            manifest,
            version_id,
            activated,
            skipped: false,
        })
    }

    pub fn activate_version(
        &self,
        version_id: &str,
        actor: Option<&str>,
        reason: Option<&str>,
        action: SchemaAuditAction,
    ) -> Result<SchemaActivationResult> {
        let mut manifest = self.load_or_init_manifest()?;
        let entry = manifest
            .versions
            .iter()
            .find(|item| item.id == version_id)
            .cloned()
            .ok_or_else(|| {
                EventError::Config(format!("schema version '{version_id}' was not found"))
            })?;

        let changed = self.apply_activation(&mut manifest, &entry, actor, reason, action, None)?;
        self.persist_manifest(&manifest)?;

        Ok(SchemaActivationResult {
            manifest,
            version_id: entry.id,
            changed,
        })
    }

    pub fn version_payload(&self, version_id: &str) -> Result<String> {
        let manifest = self.load_or_init_manifest()?;
        let entry = manifest
            .versions
            .iter()
            .find(|item| item.id == version_id)
            .ok_or_else(|| {
                EventError::Config(format!("schema version '{version_id}' was not found"))
            })?;
        let path = self.root.join(&entry.file);
        let payload = fs::read_to_string(&path)?;
        Ok(payload)
    }

    pub fn latest_version_id(manifest: &SchemaManifest) -> Option<String> {
        manifest.versions.last().map(|entry| entry.id.clone())
    }

    fn apply_activation(
        &self,
        manifest: &mut SchemaManifest,
        entry: &SchemaVersionEntry,
        actor: Option<&str>,
        reason: Option<&str>,
        action: SchemaAuditAction,
        payload_override: Option<&str>,
    ) -> Result<bool> {
        let already_active = manifest.active_version.as_deref() == Some(entry.id.as_str());

        let payload = if let Some(contents) = payload_override {
            Cow::Borrowed(contents)
        } else {
            let full_path = self.root.join(&entry.file);
            Cow::Owned(fs::read_to_string(full_path)?)
        };

        self.write_active_schema(&payload)?;
        manifest.active_version = Some(entry.id.clone());
        manifest.audit_log.push(SchemaAuditEntry::new(
            action,
            &entry.id,
            actor,
            reason,
            if already_active {
                Some("requested version already active".into())
            } else {
                None
            },
        ));
        Ok(!already_active)
    }

    fn write_active_schema(&self, payload: &str) -> Result<()> {
        self.write_text(&self.active_schema_path, payload)
    }

    fn write_version_payload(&self, relative_path: &str, payload: &str) -> Result<()> {
        let path = self.root.join(relative_path);
        self.write_text(&path, payload)
    }

    fn write_text(&self, path: &Path, payload: &str) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut tmp = NamedTempFile::new_in(
            path.parent()
                .ok_or_else(|| EventError::Storage("invalid schema path".into()))?,
        )?;
        tmp.write_all(payload.as_bytes())?;
        tmp.flush()?;
        tmp.persist(path).map_err(|err| EventError::Io(err.error))?;
        Ok(())
    }

    fn persist_manifest(&self, manifest: &SchemaManifest) -> Result<()> {
        let payload = serde_json::to_string_pretty(manifest)?;
        self.write_text(&self.manifest_path, &payload)
    }

    fn canonicalize_schema(raw: &str) -> Result<String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok("{}".to_string());
        }
        let map: BTreeMap<String, AggregateSchema> = serde_json::from_str(trimmed)?;
        Ok(serde_json::to_string_pretty(&map)?)
    }

    fn next_version_id(&self, manifest: &SchemaManifest) -> String {
        let base = Utc::now().format("v%Y%m%dT%H%M%S%3fZ").to_string();
        if !manifest.versions.iter().any(|entry| entry.id == base) {
            return base;
        }
        for idx in 1..999 {
            let candidate = format!("{base}-{idx:02}");
            if !manifest.versions.iter().any(|entry| entry.id == candidate) {
                return candidate;
            }
        }
        format!("{base}-{}", Uuid::new_v4().simple())
    }

    fn relative_version_path(&self, version_id: &str) -> String {
        format!("schemas/versions/{version_id}.json")
    }
}

fn checksum_for(payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn normalize_string(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[derive(Debug)]
pub struct PublishOptions<'a> {
    pub schema_json: &'a str,
    pub actor: Option<&'a str>,
    pub reason: Option<&'a str>,
    pub labels: &'a [String],
    pub activate: bool,
    pub skip_if_identical: bool,
}

#[derive(Debug)]
pub struct SchemaPublishOutcome {
    pub manifest: SchemaManifest,
    pub version_id: String,
    pub activated: bool,
    pub skipped: bool,
}

#[derive(Debug)]
pub struct SchemaActivationResult {
    pub manifest: SchemaManifest,
    pub version_id: String,
    pub changed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn simple_schema() -> String {
        r#"{
            "order": {
                "aggregate": "order",
                "snapshot_threshold": null,
                "locked": false,
                "field_locks": [],
                "hidden": false,
                "hidden_fields": [],
                "column_types": {},
                "events": {
                    "OrderCreated": {
                        "fields": [],
                        "notes": null
                    }
                },
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z"
            }
        }"#
        .to_string()
    }

    #[test]
    fn publish_and_activate_creates_manifest() {
        let dir = tempdir().unwrap();
        let manager = SchemaHistoryManager::new(dir.path());

        let outcome = manager
            .publish(PublishOptions {
                schema_json: &simple_schema(),
                actor: Some("alice"),
                reason: Some("initial"),
                labels: &[],
                activate: true,
                skip_if_identical: true,
            })
            .expect("publish");

        assert!(!outcome.skipped);
        assert!(outcome.activated);
        assert!(manager.manifest_path().exists());
        assert!(manager.active_schema_path().exists());
        assert!(manager.versions_dir().exists());
        assert_eq!(outcome.manifest.active_version, Some(outcome.version_id));
        assert_eq!(outcome.manifest.versions.len(), 1);
        assert_eq!(outcome.manifest.audit_log.len(), 2);
    }

    #[test]
    fn publishing_duplicate_skips_new_version() {
        let dir = tempdir().unwrap();
        let manager = SchemaHistoryManager::new(dir.path());

        manager
            .publish(PublishOptions {
                schema_json: &simple_schema(),
                actor: Some("alice"),
                reason: Some("initial"),
                labels: &[],
                activate: true,
                skip_if_identical: true,
            })
            .expect("publish");

        let second = manager
            .publish(PublishOptions {
                schema_json: &simple_schema(),
                actor: Some("bob"),
                reason: Some("noop"),
                labels: &[],
                activate: false,
                skip_if_identical: true,
            })
            .expect("second");

        assert!(second.skipped);
        assert_eq!(second.manifest.versions.len(), 1);
    }
}
