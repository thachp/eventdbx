use std::{future::Future, result::Result as StdResult, time::Duration};

use reqwest::Client;
use serde_json::{Map, Value as JsonValue};
use tokio::{
    runtime::{Handle, Runtime},
    task::block_in_place,
};

use crate::{
    config::HttpPluginConfig,
    error::{EventError, Result},
};

use super::{Plugin, PluginDelivery};

pub(super) struct HttpPlugin {
    config: HttpPluginConfig,
    client: StdResult<Client, String>,
}

impl HttpPlugin {
    pub(super) fn new(config: HttpPluginConfig) -> Self {
        let client: StdResult<_, String> = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|err| err.to_string());
        Self { config, client }
    }

    fn client(&self) -> Result<&Client> {
        self.client
            .as_ref()
            .map_err(|err| EventError::Config(format!("failed to build http client: {err}")))
    }

    fn run_with_client<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(Client) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if let Ok(handle) = Handle::try_current() {
            let client = self.client()?.clone();
            block_in_place(|| handle.block_on(operation(client)))
        } else {
            let client = self.client()?.clone();
            let runtime = Runtime::new().map_err(|err| {
                EventError::Storage(format!("failed to build http runtime: {err}"))
            })?;
            runtime.block_on(operation(client))
        }
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
        let headers: Vec<(String, String)> = self
            .config
            .headers
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        let response = self.run_with_client(|client| {
            let resolved = resolved.clone();
            let headers = headers.clone();
            async move {
                let mut request = client.head(&resolved);
                for (key, value) in headers {
                    request = request.header(&key, &value);
                }
                request
                    .send()
                    .await
                    .map_err(|err| EventError::Storage(err.to_string()))
            }
        })?;

        if !response.status().is_success() {
            return Err(EventError::Storage(format!(
                "http plugin readiness check failed ({})",
                response.status()
            )));
        }

        Ok(())
    }
}

impl Plugin for HttpPlugin {
    fn name(&self) -> &'static str {
        "http"
    }

    fn notify_event(&self, delivery: PluginDelivery<'_>) -> Result<()> {
        let resolved = self.resolved_endpoint();
        let headers: Vec<(String, String)> = self
            .config
            .headers
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

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

        let payload = JsonValue::Object(body);

        self.run_with_client(|client| {
            let resolved = resolved.clone();
            let headers = headers.clone();
            let payload = payload.clone();
            async move {
                let mut request = client.post(&resolved);
                for (key, value) in headers {
                    request = request.header(&key, &value);
                }
                let response = request
                    .json(&payload)
                    .send()
                    .await
                    .map_err(|err| EventError::Storage(err.to_string()))?;

                if response.status().is_success() {
                    return Ok(());
                }

                let status = response.status();
                let body = response
                    .text()
                    .await
                    .map_err(|err| EventError::Storage(err.to_string()))?;
                let body = body.trim();
                let body = if body.is_empty() {
                    String::new()
                } else {
                    let mut truncated = String::new();
                    let mut chars = body.chars();
                    for _ in 0..256 {
                        if let Some(ch) = chars.next() {
                            truncated.push(ch);
                        } else {
                            break;
                        }
                    }
                    if chars.next().is_some() {
                        truncated.push('…');
                    }
                    truncated
                };

                let mut message = format!("http plugin request failed ({status})");
                if !body.is_empty() {
                    message.push_str(": ");
                    message.push_str(&body);
                }
                Err(EventError::Storage(message))
            }
        })?;
        Ok(())
    }
}
