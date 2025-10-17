use std::time::Duration;

use reqwest::blocking::Client;

use crate::{
    config::HttpPluginConfig,
    error::{EventfulError, Result},
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
        let mut request = self.client.post(&self.config.endpoint);
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        request
            .json(record)
            .send()
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        Ok(())
    }
}
