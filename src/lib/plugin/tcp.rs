use std::{io::Write, net::TcpStream};

use crate::{
    config::TcpPluginConfig,
    error::{EventfulError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

use super::Plugin;

pub(super) struct TcpPlugin {
    config: TcpPluginConfig,
}

impl TcpPlugin {
    pub(super) fn new(config: TcpPluginConfig) -> Self {
        Self { config }
    }

    fn connect(&self) -> Result<TcpStream> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        TcpStream::connect(&addr).map_err(|err| EventfulError::Storage(err.to_string()))
    }
}

impl Plugin for TcpPlugin {
    fn name(&self) -> &'static str {
        "tcp"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        _state: &AggregateState,
        _schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let mut stream = self.connect()?;
        let payload = serde_json::to_string(record)
            .map_err(|err| EventfulError::Serialization(err.to_string()))?;
        stream
            .write_all(payload.as_bytes())
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        stream
            .write_all(b"\n")
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        Ok(())
    }
}
