use std::{
    io::Write,
    net::TcpStream,
    sync::atomic::{AtomicU64, Ordering},
};

use capnp::message::Builder;
use capnp::serialize;

use crate::{
    config::CapnpPluginConfig,
    error::{EventError, Result},
    plugin_capnp,
};

use super::{Plugin, PluginDelivery};

pub(super) struct CapnpPlugin {
    config: CapnpPluginConfig,
    sequence: AtomicU64,
}

impl CapnpPlugin {
    pub(super) fn new(config: CapnpPluginConfig) -> Self {
        Self {
            config,
            sequence: AtomicU64::new(0),
        }
    }

    pub(super) fn ensure_ready(&self) -> Result<()> {
        self.connect().map(|_| ())
    }

    fn connect(&self) -> Result<TcpStream> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        TcpStream::connect(&addr).map_err(|err| EventError::Storage(err.to_string()))
    }
}

impl Plugin for CapnpPlugin {
    fn name(&self) -> &'static str {
        "capnp"
    }

    fn notify_event(&self, delivery: PluginDelivery<'_>) -> Result<()> {
        let Some(record) = delivery.record else {
            return Ok(());
        };
        let state_opt = delivery.state;
        let schema_opt = delivery.schema;

        let mut stream = self.connect()?;
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);

        let event_id = record.metadata.event_id.to_string();
        let payload_json = serde_json::to_string(&record.payload)
            .map_err(|err| EventError::Serialization(err.to_string()))?;
        let metadata_json = serde_json::to_string(&record.metadata)
            .map_err(|err| EventError::Serialization(err.to_string()))?;
        let schema_json = match schema_opt {
            Some(schema) => Some(
                serde_json::to_string(schema)
                    .map_err(|err| EventError::Serialization(err.to_string()))?,
            ),
            None => None,
        };
        let extensions_json = match &record.extensions {
            Some(value) => Some(
                serde_json::to_string(value)
                    .map_err(|err| EventError::Serialization(err.to_string()))?,
            ),
            None => None,
        };

        let mut message = Builder::new_default();
        {
            let mut envelope = message.init_root::<plugin_capnp::plugin_envelope::Builder>();
            let mut union_builder = envelope.reborrow().init_message();
            let mut event = union_builder.reborrow().init_event();
            event.set_sequence(sequence);
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

            match schema_json {
                Some(ref json) => event.set_schema_json(json),
                None => event.set_schema_json("null"),
            }

            if let Some(state) = state_opt {
                event.set_state_version(state.version);
                event.set_state_archived(state.archived);
                event.set_state_merkle_root(&state.merkle_root);
                let mut entries = event.init_state_entries(state.state.len() as u32);
                for (idx, (key, value)) in state.state.iter().enumerate() {
                    let mut entry = entries.reborrow().get(idx as u32);
                    entry.set_key(key);
                    entry.set_value(value);
                }
            } else {
                event.set_state_version(0);
                event.set_state_archived(false);
                event.set_state_merkle_root("");
                event.init_state_entries(0);
            }
        }

        serialize::write_message(&mut stream, &message)
            .map_err(|err| EventError::Serialization(err.to_string()))?;
        stream
            .flush()
            .map_err(|err| EventError::Storage(err.to_string()))?;
        Ok(())
    }
}
