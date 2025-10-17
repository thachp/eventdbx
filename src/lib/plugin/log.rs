use tracing::{Level, debug, error, info, trace, warn};

use crate::{
    config::LogPluginConfig,
    error::{EventfulError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

use super::Plugin;

pub(super) struct LogPlugin {
    config: LogPluginConfig,
}

impl LogPlugin {
    pub(super) fn new(config: LogPluginConfig) -> Self {
        Self { config }
    }

    fn level(&self) -> Result<Level> {
        match self.config.level.to_ascii_lowercase().as_str() {
            "trace" => Ok(Level::TRACE),
            "debug" => Ok(Level::DEBUG),
            "info" => Ok(Level::INFO),
            "warn" => Ok(Level::WARN),
            "error" => Ok(Level::ERROR),
            other => Err(EventfulError::Config(format!(
                "unsupported log level '{}'",
                other
            ))),
        }
    }

    fn format_message(&self, record: &EventRecord) -> String {
        if let Some(template) = &self.config.template {
            template
                .replace("{aggregate}", &record.aggregate_type)
                .replace("{id}", &record.aggregate_id)
                .replace("{event}", &record.event_type)
        } else {
            format!(
                "aggregate={} id={} event={} payload={}",
                record.aggregate_type,
                record.aggregate_id,
                record.event_type,
                serde_json::to_string(&record.payload).unwrap_or_default()
            )
        }
    }

    fn log(&self, level: Level, message: String) {
        match level {
            Level::TRACE => trace!(target: "eventdb.plugin.log", "{}", message),
            Level::DEBUG => debug!(target: "eventdb.plugin.log", "{}", message),
            Level::INFO => info!(target: "eventdb.plugin.log", "{}", message),
            Level::WARN => warn!(target: "eventdb.plugin.log", "{}", message),
            Level::ERROR => error!(target: "eventdb.plugin.log", "{}", message),
        }
    }
}

impl Plugin for LogPlugin {
    fn name(&self) -> &'static str {
        "log"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        _state: &AggregateState,
        _schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let level = self.level()?;
        let message = self.format_message(record);
        self.log(level, message);
        Ok(())
    }
}
