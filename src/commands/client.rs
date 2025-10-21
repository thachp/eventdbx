use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use eventdbx::{config::Config, store::EventRecord};
use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_HOST: &str = "127.0.0.1";

#[derive(Clone)]
pub struct ServerClient {
    base_url: String,
    client: Client,
}

impl ServerClient {
    pub fn new(config: &Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("failed to build HTTP client")?;

        let base_url = format!("http://{}:{}", DEFAULT_HOST, config.port);

        Ok(Self { base_url, client })
    }

    pub fn append_event(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: &Value,
    ) -> Result<EventRecord> {
        let request = AppendEventRequest {
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
        };
        let url = format!("{}/v1/events", self.base_url);

        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(&request)
            .send()
            .context("failed to call EventDBX HTTP API")?;

        if response.status().is_success() {
            let record = response
                .json::<EventRecord>()
                .context("failed to parse append response")?;
            return Ok(record);
        }

        let status = response.status();
        let body = response.text().unwrap_or_default();
        if let Ok(error) = serde_json::from_str::<ErrorBody>(&body) {
            return Err(anyhow!("server returned {}: {}", status, error.message));
        }
        if !body.trim().is_empty() {
            return Err(anyhow!("server returned {}: {}", status, body.trim()));
        }

        match status {
            StatusCode::UNAUTHORIZED => Err(anyhow!(
                "server rejected the request (unauthorized). Provide a valid token with \
                 --token or EVENTDBX_TOKEN."
            )),
            StatusCode::FORBIDDEN => Err(anyhow!(
                "server rejected the request (forbidden). Check token permissions."
            )),
            StatusCode::NOT_FOUND => Err(anyhow!(
                "server returned 404. Ensure the REST API is enabled on the running daemon."
            )),
            _ => Err(anyhow!("server returned {}", status)),
        }
    }
}

#[derive(Serialize)]
struct AppendEventRequest<'a> {
    aggregate_type: &'a str,
    aggregate_id: &'a str,
    event_type: &'a str,
    payload: &'a Value,
}

#[derive(Deserialize)]
struct ErrorBody {
    message: String,
}
