use std::{
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::Arc,
};

use capnp::message::Builder;
use capnp::serialize;
use parking_lot::Mutex;
use serde_json;
use tracing::{debug, info, warn};

use crate::config::ProcessPluginConfig;
use crate::error::{EventError, Result};
use crate::plugin::registry::{InstalledPluginRecord, load_registry, registry_path};
use crate::plugin_capnp;
use crate::schema::AggregateSchema;
use crate::store::{AggregateState, EventRecord};

use super::{Plugin, PluginDelivery};

const CHANNEL_LABEL_STDOUT: &str = "stdout";
const CHANNEL_LABEL_STDERR: &str = "stderr";

fn sanitize_identifier(identifier: &str) -> String {
    identifier
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub fn status_file_path(data_dir: &Path, identifier: &str) -> PathBuf {
    data_dir
        .join("plugins")
        .join("run")
        .join(format!("{}.pid", sanitize_identifier(identifier)))
}

pub struct ProcessPlugin {
    connection: Arc<ProcessConnection>,
}

impl ProcessPlugin {
    pub fn new(identifier: String, config: ProcessPluginConfig, data_dir: &Path) -> Result<Self> {
        let registry_path = registry_path(data_dir);
        let registry = load_registry(&registry_path)?;
        let target = current_target_triple();
        let record = registry
            .iter()
            .find(|entry| {
                entry.name == config.name
                    && entry.version == config.version
                    && entry.target == target
            })
            .cloned()
            .ok_or_else(|| {
                EventError::Config(format!(
                    "plugin {}@{} is not installed for target {}",
                    config.name, config.version, target
                ))
            })?;

        let status_path = status_file_path(data_dir, &identifier);
        let connection = ProcessConnection::new(identifier, config, record, status_path)?;
        connection.ensure_started()?;

        Ok(Self {
            connection: Arc::new(connection),
        })
    }
}

struct ProcessConnection {
    identifier: String,
    config: ProcessPluginConfig,
    record: InstalledPluginRecord,
    status_path: PathBuf,
    inner: Mutex<Option<ProcessInner>>,
}

struct ProcessInner {
    child: Child,
    writer: BufWriter<ChildStdin>,
    sequence: u64,
}

impl ProcessConnection {
    fn new(
        identifier: String,
        config: ProcessPluginConfig,
        record: InstalledPluginRecord,
        status_path: PathBuf,
    ) -> Result<Self> {
        let connection = Self {
            identifier,
            config,
            record,
            status_path,
            inner: Mutex::new(None),
        };
        if let Err(err) = connection.clear_status_file() {
            warn!(
                target: "eventdbx.plugin",
                "failed to clear stale status file for {}: {}",
                connection.identifier,
                err
            );
        }
        Ok(connection)
    }

    fn ensure_started(&self) -> Result<()> {
        let mut guard = self.inner.lock();
        self.ensure_running(&mut guard)?;
        if guard.is_none() {
            self.restart(&mut guard)?;
        }
        Ok(())
    }

    fn send_event(
        &self,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let mut guard = self.inner.lock();
        self.ensure_running(&mut guard)?;
        if guard.is_none() {
            self.restart(&mut guard)?;
        }

        let inner = guard
            .as_mut()
            .ok_or_else(|| EventError::Storage("plugin process failed to start".into()))?;

        if let Err(err) = self.write_event(inner, record, state, schema) {
            warn!(
                target: "eventdbx.plugin",
                "failed to deliver event to plugin {} ({}), attempting restart: {}",
                self.identifier,
                self.config.name,
                err
            );
            self.restart(&mut guard)?;
            let inner = guard
                .as_mut()
                .ok_or_else(|| EventError::Storage("plugin process failed to restart".into()))?;
            self.write_event(inner, record, state, schema)?;
        }
        Ok(())
    }

    fn ensure_running(&self, guard: &mut Option<ProcessInner>) -> Result<()> {
        if let Some(inner) = guard.as_mut() {
            if let Ok(Some(status)) = inner.child.try_wait() {
                warn!(
                    target: "eventdbx.plugin",
                    "plugin {} exited with status {}",
                    self.identifier,
                    status
                );
                if let Err(err) = self.clear_status_file() {
                    warn!(
                        target: "eventdbx.plugin",
                        "failed to clear status file for {}: {}",
                        self.identifier,
                        err
                    );
                }
                *guard = None;
            }
        }
        Ok(())
    }

    fn restart(&self, guard: &mut Option<ProcessInner>) -> Result<()> {
        if let Err(err) = self.clear_status_file() {
            warn!(
                target: "eventdbx.plugin",
                "failed to clear status file for {} during restart: {}",
                self.identifier,
                err
            );
        }
        let mut inner = self.spawn_process()?;
        self.write_init(&mut inner)?;
        *guard = Some(inner);
        Ok(())
    }

    fn spawn_process(&self) -> Result<ProcessInner> {
        let mut command = Command::new(&self.record.binary_path);
        if !self.config.args.is_empty() {
            command.args(&self.config.args);
        }
        for (key, value) in &self.config.env {
            command.env(key, value);
        }
        if let Some(dir) = &self.config.working_dir {
            command.current_dir(dir);
        } else {
            command.current_dir(&self.record.install_dir);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn().map_err(|err| {
            EventError::Storage(format!(
                "failed to launch plugin {}: {}",
                self.identifier, err
            ))
        })?;

        let stdin = child.stdin.take().ok_or_else(|| {
            EventError::Storage(format!("plugin {} did not provide stdin", self.identifier))
        })?;

        if let Some(stdout) = child.stdout.take() {
            spawn_stream_logger(self.identifier.clone(), CHANNEL_LABEL_STDOUT, stdout);
        }

        if let Some(stderr) = child.stderr.take() {
            spawn_stream_logger(self.identifier.clone(), CHANNEL_LABEL_STDERR, stderr);
        }

        let writer = BufWriter::new(stdin);

        if let Err(err) = self.write_status_file(child.id()) {
            warn!(
                target: "eventdbx.plugin",
                "failed to write status file for {}: {}",
                self.identifier,
                err
            );
        }

        Ok(ProcessInner {
            child,
            writer,
            sequence: 0,
        })
    }

    fn write_init(&self, inner: &mut ProcessInner) -> Result<()> {
        let mut message = Builder::new_default();
        {
            let mut envelope = message.init_root::<plugin_capnp::plugin_envelope::Builder>();
            let mut message_builder = envelope.reborrow().init_message();
            let mut init = message_builder.reborrow().init_init();
            init.set_plugin_name(&self.config.name);
            init.set_version(&self.config.version);
            init.set_target(&self.record.target);
        }
        serialize::write_message(&mut inner.writer, &message)
            .map_err(|err| EventError::Serialization(err.to_string()))?;
        inner.writer.flush()?;
        Ok(())
    }

    fn write_event(
        &self,
        inner: &mut ProcessInner,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let event_id = record.metadata.event_id.to_string();
        let payload_json = serde_json::to_string(&record.payload)?;
        let metadata_json = serde_json::to_string(&record.metadata)?;
        let schema_json = match schema {
            Some(schema) => Some(serde_json::to_string(schema)?),
            None => None,
        };
        let extensions_json = match &record.extensions {
            Some(value) => Some(serde_json::to_string(value)?),
            None => None,
        };

        let mut message = Builder::new_default();
        {
            let mut envelope = message.init_root::<plugin_capnp::plugin_envelope::Builder>();
            let mut union_builder = envelope.reborrow().init_message();
            let mut event = union_builder.reborrow().init_event();
            event.set_sequence(inner.sequence);
            event.set_aggregate_type(&record.aggregate_type);
            event.set_aggregate_id(&record.aggregate_id);
            event.set_event_type(&record.event_type);
            event.set_event_version(record.version);
            event.set_event_id(&event_id);
            event.set_created_at_epoch_micros(record.metadata.created_at.timestamp_micros());
            event.set_payload_json(&payload_json);
            event.set_metadata_json(&metadata_json);
            match extensions_json {
                Some(ref json) => event.set_extensions_json(json),
                None => event.set_extensions_json("null"),
            }
            event.set_hash(&record.hash);
            event.set_merkle_root(&record.merkle_root);

            event.set_state_version(state.version);
            event.set_state_archived(state.archived);
            event.set_state_merkle_root(&state.merkle_root);

            match schema_json {
                Some(ref json) => event.set_schema_json(json),
                None => event.set_schema_json("null"),
            }

            let mut entries = event.init_state_entries(state.state.len() as u32);
            for (idx, (key, value)) in state.state.iter().enumerate() {
                let mut entry = entries.reborrow().get(idx as u32);
                entry.set_key(key);
                entry.set_value(value);
            }
        }

        serialize::write_message(&mut inner.writer, &message)
            .map_err(|err| EventError::Serialization(err.to_string()))?;
        inner.writer.flush()?;
        inner.sequence = inner.sequence.wrapping_add(1);
        Ok(())
    }

    fn write_status_file(&self, pid: u32) -> std::io::Result<()> {
        if let Some(parent) = self.status_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.status_path, format!("{pid}\n"))
    }

    fn clear_status_file(&self) -> std::io::Result<()> {
        match fs::remove_file(&self.status_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}

impl Drop for ProcessConnection {
    fn drop(&mut self) {
        if let Err(err) = self.clear_status_file() {
            warn!(
                target: "eventdbx.plugin",
                "failed to clear status file for {} on drop: {}",
                self.identifier,
                err
            );
        }
    }
}

impl Plugin for ProcessPlugin {
    fn name(&self) -> &'static str {
        "process"
    }

    fn notify_event(&self, delivery: PluginDelivery<'_>) -> Result<()> {
        let Some(record) = delivery.record else {
            return Ok(());
        };
        let Some(state) = delivery.state else {
            return Ok(());
        };
        self.connection.send_event(record, state, delivery.schema)
    }
}

fn spawn_stream_logger<R>(identifier: String, channel: &'static str, reader: R)
where
    R: std::io::Read + Send + 'static,
{
    std::thread::spawn(move || {
        let mut buf_reader = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            match buf_reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim_end_matches(&['\r', '\n'][..]);
                    if !trimmed.is_empty() {
                        info!(
                            target: "eventdbx.plugin",
                            "{} {}: {}",
                            identifier,
                            channel,
                            trimmed
                        );
                    }
                }
                Err(err) => {
                    debug!(
                        target: "eventdbx.plugin",
                        "error reading {} from plugin {}: {}",
                        channel,
                        identifier,
                        err
                    );
                    break;
                }
            }
        }
    });
}

fn current_target_triple() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}
