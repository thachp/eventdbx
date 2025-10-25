use std::sync::Arc;

use chrono::Utc;
use tracing::{error, warn};

use crate::{
    config::{Config, PluginConfig, PluginDefinition, PluginKind, PluginPayloadMode},
    error::{EventError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

mod process;
pub mod queue;
pub mod registry;
use queue::{JobPayload, PluginQueueStore};
mod tcp;
use tcp::TcpPlugin;
mod capnp;
use capnp::CapnpPlugin;
mod http;
use http::HttpPlugin;
mod log;
use log::LogPlugin;
use process::ProcessPlugin;

pub struct PluginDelivery<'a> {
    pub record: Option<&'a EventRecord>,
    pub state: Option<&'a AggregateState>,
    pub schema: Option<&'a AggregateSchema>,
}

pub trait Plugin: Send + Sync {
    fn name(&self) -> &'static str;
    fn notify_event(&self, delivery: PluginDelivery<'_>) -> Result<()>;
}

struct PluginEntry {
    label: String,
    mode: PluginPayloadMode,
    plugin: Box<dyn Plugin>,
}

#[derive(Clone)]
pub struct PluginManager {
    plugins: Arc<Vec<PluginEntry>>,
    queue: Option<PluginQueueStore>,
    max_attempts: u8,
}

impl PluginManager {
    pub fn from_config(config: &Config) -> Result<Self> {
        let mut entries = Vec::new();
        let mut definitions = config.load_plugins()?;
        if definitions.is_empty() && !config.plugins.is_empty() {
            definitions = config.plugins.clone();
        }

        for definition in definitions.into_iter() {
            if !definition.enabled {
                continue;
            }
            let label = plugin_label(&definition);
            let plugin = instantiate_plugin(&definition, config);
            entries.push(PluginEntry {
                label,
                mode: definition.payload_mode,
                plugin,
            });
        }

        let queue = match PluginQueueStore::open_with_legacy(
            config.plugin_queue_db_path().as_path(),
            config.plugin_queue_path().as_path(),
        ) {
            Ok(store) => Some(store),
            Err(err) => {
                warn!(
                    target: "eventdbx.plugin",
                    "failed to initialize plugin queue store: {}",
                    err
                );
                None
            }
        };

        Ok(Self {
            plugins: Arc::new(entries),
            queue,
            max_attempts: config.plugin_max_attempts.min(u8::MAX as u32) as u8,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    pub fn notify_event(
        &self,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        if self.plugins.is_empty() {
            return Ok(());
        }

        for entry in self.plugins.iter() {
            if let Some(queue) = &self.queue {
                let job_payload = build_job_payload(entry.mode, record, state, schema);
                let payload_value = serde_json::to_value(&job_payload)?;
                let now = Utc::now().timestamp_millis();
                match queue.enqueue_job(&entry.label, payload_value, now) {
                    Ok(_) => {
                        if let Err(err) = self.process_jobs_for_plugin(queue, entry) {
                            warn!(
                                target: "eventdbx.plugin",
                                "failed to process plugin queue for {}: {}",
                                entry.label,
                                err
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            target: "eventdbx.plugin",
                            "failed to enqueue plugin job for {}: {}",
                            entry.label,
                            err
                        );
                        let delivery = build_delivery(entry.mode, record, state, schema);
                        if let Err(err) = entry.plugin.notify_event(delivery) {
                            error!("plugin {} failed: {}", entry.label, err);
                        }
                    }
                }
            } else {
                let delivery = build_delivery(entry.mode, record, state, schema);
                if let Err(err) = entry.plugin.notify_event(delivery) {
                    error!("plugin {} failed: {}", entry.label, err);
                }
            }
        }

        Ok(())
    }

    fn process_jobs_for_plugin(&self, queue: &PluginQueueStore, entry: &PluginEntry) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        let _ = queue.recover_stuck_jobs(&entry.label, now, self.max_attempts);

        loop {
            let dispatch_now = Utc::now().timestamp_millis();
            let jobs = queue.dispatch_jobs(&entry.label, 8, dispatch_now)?;
            if jobs.is_empty() {
                break;
            }

            for job in jobs {
                let payload: JobPayload =
                    serde_json::from_value(job.payload.clone()).unwrap_or_default();

                if payload.legacy.is_some() && payload.is_empty() {
                    // legacy queue entry without enough data—mark completed.
                    queue.complete_job(job.id)?;
                    continue;
                }

                if payload.is_empty() {
                    queue.complete_job(job.id)?;
                    continue;
                }

                let delivery = PluginDelivery {
                    record: payload.record.as_ref(),
                    state: payload.state.as_ref(),
                    schema: payload.schema.as_ref(),
                };

                match entry.plugin.notify_event(delivery) {
                    Ok(_) => {
                        queue.complete_job(job.id)?;
                    }
                    Err(err) => {
                        let error = err.to_string();
                        let now = Utc::now().timestamp_millis();
                        queue.fail_job(job.id, error, now, self.max_attempts)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn dispatch_pending(&self) -> Result<()> {
        if let Some(queue) = &self.queue {
            for entry in self.plugins.iter() {
                self.process_jobs_for_plugin(queue, entry)?;
            }
        }
        Ok(())
    }
}

fn build_job_payload(
    mode: PluginPayloadMode,
    record: &EventRecord,
    state: &AggregateState,
    schema: Option<&AggregateSchema>,
) -> JobPayload {
    let include_event = mode.includes_event();
    let include_state = mode.includes_state();
    let include_schema = mode.includes_schema();
    JobPayload {
        record: include_event.then(|| record.clone()),
        state: include_state.then(|| state.clone()),
        schema: include_schema.then_some(schema.cloned()).flatten(),
        legacy: None,
    }
}

fn build_delivery<'a>(
    mode: PluginPayloadMode,
    record: &'a EventRecord,
    state: &'a AggregateState,
    schema: Option<&'a AggregateSchema>,
) -> PluginDelivery<'a> {
    let include_event = mode.includes_event();
    let include_state = mode.includes_state();
    let include_schema = mode.includes_schema();
    PluginDelivery {
        record: include_event.then_some(record),
        state: include_state.then_some(state),
        schema: include_schema.then_some(schema).flatten(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        snowflake::SnowflakeId,
        store::{AggregateState, EventMetadata, EventRecord},
    };
    use chrono::Utc;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    struct FailingPlugin;

    impl Plugin for FailingPlugin {
        fn name(&self) -> &'static str {
            "failing"
        }

        fn notify_event(&self, _delivery: PluginDelivery<'_>) -> Result<()> {
            Err(EventError::Storage("simulated failure".into()))
        }
    }

    struct SuccessfulPlugin;

    impl Plugin for SuccessfulPlugin {
        fn name(&self) -> &'static str {
            "successful"
        }

        fn notify_event(&self, _delivery: PluginDelivery<'_>) -> Result<()> {
            Ok(())
        }
    }

    fn sample_record(event_id: u64) -> (EventRecord, AggregateState) {
        let record = EventRecord {
            aggregate_type: "order".into(),
            aggregate_id: "order-1".into(),
            event_type: "created".into(),
            payload: json!({"status": "created"}),
            extensions: None,
            metadata: EventMetadata {
                event_id: SnowflakeId::from_u64(event_id),
                created_at: Utc::now(),
                issued_by: None,
                note: None,
            },
            version: 1,
            hash: "hash".into(),
            merkle_root: "merkle".into(),
        };
        let state = AggregateState {
            aggregate_type: record.aggregate_type.clone(),
            aggregate_id: record.aggregate_id.clone(),
            version: record.version,
            state: BTreeMap::from([("status".into(), "created".into())]),
            merkle_root: record.merkle_root.clone(),
            archived: false,
        };
        (record, state)
    }

    #[test]
    fn queues_failed_plugin_events() -> Result<()> {
        let temp = TempDir::new().expect("tempdir");
        let queue_path = temp.path().join("queue.db");
        let queue_store = PluginQueueStore::open(queue_path.as_path())?;

        let manager = PluginManager {
            plugins: Arc::new(vec![PluginEntry {
                label: "failing".into(),
                mode: PluginPayloadMode::All,
                plugin: Box::new(FailingPlugin),
            }]),
            queue: Some(queue_store.clone()),
            max_attempts: 3,
        };

        let (record, state) = sample_record(7);
        manager.notify_event(&record, &state, None)?;

        let status = queue_store.status()?;
        assert_eq!(status.dead.len(), 0);
        assert_eq!(status.pending.len(), 1);
        assert_eq!(status.pending[0].attempts, 1);

        Ok(())
    }

    #[test]
    fn clears_queue_on_success() -> Result<()> {
        let temp = TempDir::new().expect("tempdir");
        let queue_path = temp.path().join("queue.db");
        let queue_store = PluginQueueStore::open(queue_path.as_path())?;

        let failing_manager = PluginManager {
            plugins: Arc::new(vec![PluginEntry {
                label: "failing".into(),
                mode: PluginPayloadMode::All,
                plugin: Box::new(FailingPlugin),
            }]),
            queue: Some(queue_store.clone()),
            max_attempts: 0,
        };

        let (record, state) = sample_record(15);
        failing_manager.notify_event(&record, &state, None)?;

        let status_after_fail = queue_store.status()?;
        assert_eq!(status_after_fail.dead.len(), 1);
        let job_id = status_after_fail.dead[0].id;
        queue_store.retry_dead_job(job_id, Utc::now().timestamp_millis())?;

        let success_manager = PluginManager {
            plugins: Arc::new(vec![PluginEntry {
                label: "failing".into(),
                mode: PluginPayloadMode::All,
                plugin: Box::new(SuccessfulPlugin),
            }]),
            queue: Some(queue_store.clone()),
            max_attempts: 3,
        };
        success_manager.dispatch_pending()?;

        let status = queue_store.status()?;
        assert!(status.pending.is_empty());
        assert!(status.processing.is_empty());
        assert_eq!(status.done.len(), 1);
        Ok(())
    }
}

pub fn establish_connection(definition: &PluginDefinition) -> Result<()> {
    match &definition.config {
        PluginConfig::Tcp(settings) => {
            let plugin = TcpPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Capnp(settings) => {
            let plugin = CapnpPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Http(settings) => {
            let plugin = HttpPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Log(settings) => {
            let plugin = LogPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Process(_) => Ok(()),
    }
}

pub fn instantiate_plugin(definition: &PluginDefinition, config: &Config) -> Box<dyn Plugin> {
    match &definition.config {
        PluginConfig::Tcp(settings) => Box::new(TcpPlugin::new(settings.clone())),
        PluginConfig::Capnp(settings) => Box::new(CapnpPlugin::new(settings.clone())),
        PluginConfig::Http(settings) => Box::new(HttpPlugin::new(settings.clone())),
        PluginConfig::Log(settings) => Box::new(LogPlugin::new(settings.clone())),
        PluginConfig::Process(settings) => {
            let identifier = definition
                .name
                .clone()
                .unwrap_or_else(|| settings.name.clone());
            match ProcessPlugin::new(identifier.clone(), settings.clone(), &config.data_dir) {
                Ok(plugin) => Box::new(plugin),
                Err(err) => {
                    tracing::error!(
                        target: "eventdbx.plugin",
                        "failed to initialize process plugin {}: {}",
                        identifier,
                        err
                    );
                    Box::new(UnavailablePlugin::new(identifier, err.to_string()))
                }
            }
        }
    }
}

struct UnavailablePlugin {
    label: String,
    reason: String,
}

impl UnavailablePlugin {
    fn new(label: String, reason: String) -> Self {
        Self { label, reason }
    }
}

impl Plugin for UnavailablePlugin {
    fn name(&self) -> &'static str {
        "unavailable"
    }

    fn notify_event(&self, _delivery: PluginDelivery<'_>) -> Result<()> {
        Err(EventError::Config(format!(
            "plugin '{}' is unavailable: {}",
            self.label, self.reason
        )))
    }
}

fn plugin_label(definition: &PluginDefinition) -> String {
    match definition.name.as_deref() {
        Some(name) if !name.trim().is_empty() => {
            format!("{} ({})", plugin_kind_name(definition.config.kind()), name)
        }
        _ => plugin_kind_name(definition.config.kind()).to_string(),
    }
}

fn plugin_kind_name(kind: PluginKind) -> &'static str {
    match kind {
        PluginKind::Tcp => "tcp",
        PluginKind::Capnp => "capnp",
        PluginKind::Http => "http",
        PluginKind::Log => "log",
        PluginKind::Process => "process",
    }
}
