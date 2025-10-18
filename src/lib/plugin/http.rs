use std::time::Duration;

use reqwest::blocking::Client;

use crate::{
    config::HttpPluginConfig,
    error::{EventError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

use super::Plugin;

pub(super) struct HttpPlugin {
    config: HttpPluginConfig,
    client: Client,
}

impl HttpPlugin {
    pub(super) fn new(config: HttpPluginConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build http client");
        Self { config, client }
    }

    fn resolved_endpoint(&self) -> String {
        let endpoint = self.config.endpoint.trim();
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else if self.config.https {
            format!("https://{}", endpoint)
        } else {
            format!("http://{}", endpoint)
        }
    }

    pub(super) fn ensure_ready(&self) -> Result<()> {
        let resolved = self.resolved_endpoint();
        let mut request = self.client.head(&resolved);
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }
        request
            .send()
            .map(|_| ())
            .map_err(|err| EventError::Storage(err.to_string()))
    }
}

impl Plugin for HttpPlugin {
    fn name(&self) -> &'static str {
        "http"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        _state: &AggregateState,
        _schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let resolved = self.resolved_endpoint();
        let mut request = self.client.post(&resolved);
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        request
            .json(record)
            .send()
            .map_err(|err| EventError::Storage(err.to_string()))?;
        Ok(())
    }
}
