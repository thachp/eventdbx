use std::{io::Write, net::TcpStream};

use crate::{
    config::TcpPluginConfig,
    error::{EventError, Result},
};

use serde_json::{Map, Value as JsonValue};

use super::{Plugin, PluginDelivery};

pub(super) struct TcpPlugin {
    config: TcpPluginConfig,
}

impl TcpPlugin {
    pub(super) fn new(config: TcpPluginConfig) -> Self {
        Self { config }
    }

    pub(super) fn ensure_ready(&self) -> Result<()> {
        self.connect().map(|_| ())
    }

    fn connect(&self) -> Result<TcpStream> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        TcpStream::connect(&addr).map_err(|err| EventError::Storage(err.to_string()))
    }
}

impl Plugin for TcpPlugin {
    fn name(&self) -> &'static str {
        "tcp"
    }

    fn notify_event(&self, delivery: PluginDelivery<'_>) -> Result<()> {
        let mut stream = self.connect()?;

        let mut body = Map::new();
        if let Some(record) = delivery.record {
            body.insert(
                "event".to_string(),
                serde_json::to_value(record).unwrap_or(JsonValue::Null),
            );
        }
        if let Some(state) = delivery.state {
            body.insert(
                "state".to_string(),
                serde_json::to_value(state).unwrap_or(JsonValue::Null),
            );
        }
        if let Some(schema) = delivery.schema {
            body.insert(
                "schema".to_string(),
                serde_json::to_value(schema).unwrap_or(JsonValue::Null),
            );
        }

        if body.is_empty() {
            return Ok(());
        }

        let payload = serde_json::to_string(&body)
            .map_err(|err| EventError::Serialization(err.to_string()))?;
        stream
            .write_all(payload.as_bytes())
            .map_err(|err| EventError::Storage(err.to_string()))?;
        stream
            .write_all(b"\n")
            .map_err(|err| EventError::Storage(err.to_string()))?;
        Ok(())
    }
}
