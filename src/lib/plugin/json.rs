use std::{fs::OpenOptions, io::Write};

use crate::{
    config::JsonPluginConfig,
    error::{EventfulError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

use super::Plugin;

pub(super) struct JsonPlugin {
    config: JsonPluginConfig,
}

impl JsonPlugin {
    pub(super) fn new(config: JsonPluginConfig) -> Self {
        Self { config }
    }

    fn open_file(&self) -> Result<std::fs::File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.path)
            .map_err(|err| EventfulError::Storage(err.to_string()))
    }
}

impl Plugin for JsonPlugin {
    fn name(&self) -> &'static str {
        "json"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        _state: &AggregateState,
        _schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let mut file = self.open_file()?;
        if self.config.pretty {
            serde_json::to_writer_pretty(&mut file, record)
        } else {
            serde_json::to_writer(&mut file, record)
        }
        .map_err(|err| EventfulError::Serialization(err.to_string()))?;
        file.write_all(b"\n")
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        Ok(())
    }
}
