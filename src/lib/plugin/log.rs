use serde_json::to_string;
use tracing::{Level, debug, error, info, trace, warn};

use crate::{
    config::{LogDetailMode, LogPluginConfig},
    error::{EventError, Result},
    schema::AggregateSchema,
    store::AggregateState,
    store::EventRecord,
};

use super::{Plugin, PluginDelivery};

pub(super) struct LogPlugin {
    config: LogPluginConfig,
}

impl LogPlugin {
    pub(super) fn new(config: LogPluginConfig) -> Self {
        Self { config }
    }

    pub(super) fn ensure_ready(&self) -> Result<()> {
        self.level().map(|_| ())
    }

    fn level(&self) -> Result<Level> {
        match self.config.level.to_ascii_lowercase().as_str() {
            "trace" => Ok(Level::TRACE),
            "debug" => Ok(Level::DEBUG),
            "info" => Ok(Level::INFO),
            "warn" => Ok(Level::WARN),
            "error" => Ok(Level::ERROR),
            other => Err(EventError::Config(format!(
                "unsupported log level '{}'",
                other
            ))),
        }
    }

    fn format_event_message(&self, record: &EventRecord) -> String {
        if let Some(template) = &self.config.template {
            template
                .replace("{aggregate}", &record.aggregate_type)
                .replace("{id}", &record.aggregate_id)
                .replace("{event}", &record.event_type)
        } else {
            match self.config.detail {
                LogDetailMode::Summary => format!(
                    "aggregate={} id={} event={}",
                    record.aggregate_type, record.aggregate_id, record.event_type
                ),
                LogDetailMode::Full => format!(
                    "aggregate={} id={} event={} payload={}",
                    record.aggregate_type,
                    record.aggregate_id,
                    record.event_type,
                    serde_json::to_string(&record.payload).unwrap_or_default()
                ),
            }
        }
    }

    fn format_state_message(&self, state: &AggregateState) -> String {
        match self.config.detail {
            LogDetailMode::Summary => {
                format!(
                    "aggregate={} id={} state=present",
                    state.aggregate_type, state.aggregate_id
                )
            }
            LogDetailMode::Full => format!(
                "aggregate={} id={} state={}",
                state.aggregate_type,
                state.aggregate_id,
                to_string(&state.state).unwrap_or_default()
            ),
        }
    }

    fn format_schema_message(&self, schema: &AggregateSchema) -> String {
        match self.config.detail {
            LogDetailMode::Summary => format!("schema={}", schema.aggregate),
            LogDetailMode::Full => {
                format!(
                    "schema={} payload={}",
                    schema.aggregate,
                    to_string(schema).unwrap_or_default()
                )
            }
        }
    }

    fn log(&self, level: Level, message: String) {
        match level {
            Level::TRACE => trace!(target: "eventdbx.plugin.log", "{}", message),
            Level::DEBUG => debug!(target: "eventdbx.plugin.log", "{}", message),
            Level::INFO => info!(target: "eventdbx.plugin.log", "{}", message),
            Level::WARN => warn!(target: "eventdbx.plugin.log", "{}", message),
            Level::ERROR => error!(target: "eventdbx.plugin.log", "{}", message),
        }
    }
}

impl Plugin for LogPlugin {
    fn name(&self) -> &'static str {
        "log"
    }

    fn notify_event(&self, delivery: PluginDelivery<'_>) -> Result<()> {
        let level = self.level()?;
        let mut emitted = false;
        if let Some(record) = delivery.record {
            let message = self.format_event_message(record);
            self.log(level, message);
            emitted = true;
        }
        if let Some(state) = delivery.state {
            let message = self.format_state_message(state);
            self.log(level, message);
            emitted = true;
        }
        if let Some(schema) = delivery.schema {
            let message = self.format_schema_message(schema);
            self.log(level, message);
            emitted = true;
        }
        if !emitted {
            self.log(level, "plugin job contained no data".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::LogPluginConfig,
        schema::{AggregateSchema, EventSchema},
        snowflake::SnowflakeId,
        store::{AggregateState, EventMetadata},
    };
    use chrono::Utc;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn sample_record() -> EventRecord {
        EventRecord {
            aggregate_type: "order".into(),
            aggregate_id: "order-1".into(),
            event_type: "created".into(),
            event_type_raw: Some("created".into()),
            payload: json!({"secret": "value"}),
            extensions: None,
            metadata: EventMetadata {
                event_id: SnowflakeId::from_u64(7),
                created_at: Utc::now(),
                issued_by: None,
                note: None,
            },
            version: 1,
            hash: "hash".into(),
            merkle_root: "merkle".into(),
        }
    }

    fn sample_state() -> AggregateState {
        AggregateState {
            aggregate_type: "order".into(),
            aggregate_id: "order-1".into(),
            version: 1,
            state: BTreeMap::from([("secret".into(), "value".into())]),
            extensions: BTreeMap::new(),
            merkle_root: "merkle".into(),
            created_at: None,
            updated_at: None,
            archived: false,
        }
    }

    fn sample_schema() -> AggregateSchema {
        AggregateSchema {
            aggregate: "order".into(),
            snapshot_threshold: None,
            locked: false,
            field_locks: Vec::new(),
            hidden: false,
            hidden_fields: Vec::new(),
            column_types: BTreeMap::new(),
            events: BTreeMap::from([("created".into(), EventSchema::default())]),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn summary_detail_omits_record_payload() {
        let plugin = LogPlugin::new(LogPluginConfig {
            level: "info".into(),
            template: None,
            detail: LogDetailMode::Summary,
        });

        let message = plugin.format_event_message(&sample_record());

        assert_eq!(message, "aggregate=order id=order-1 event=created");
        assert!(!message.contains("secret"));
    }

    #[test]
    fn full_detail_preserves_record_payload() {
        let plugin = LogPlugin::new(LogPluginConfig {
            level: "info".into(),
            template: None,
            detail: LogDetailMode::Full,
        });

        let message = plugin.format_event_message(&sample_record());

        assert!(message.contains("payload="));
        assert!(message.contains("secret"));
    }

    #[test]
    fn summary_detail_omits_state_body() {
        let plugin = LogPlugin::new(LogPluginConfig {
            level: "info".into(),
            template: None,
            detail: LogDetailMode::Summary,
        });

        let message = plugin.format_state_message(&sample_state());

        assert_eq!(message, "aggregate=order id=order-1 state=present");
    }

    #[test]
    fn summary_detail_omits_schema_body() {
        let plugin = LogPlugin::new(LogPluginConfig {
            level: "info".into(),
            template: None,
            detail: LogDetailMode::Summary,
        });

        let message = plugin.format_schema_message(&sample_schema());

        assert_eq!(message, "schema=order");
    }
}
