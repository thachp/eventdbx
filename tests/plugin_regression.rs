use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::{Router, body::Bytes, extract::State, http::StatusCode, routing::post};
use chrono::Utc;
use eventdbx::{
    config::{Config, HttpPluginConfig, PluginConfig, PluginDefinition},
    plugin::instantiate_plugin,
    store::{AggregateState, EventMetadata, EventRecord},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc};

#[tokio::test(flavor = "multi_thread")]
async fn http_plugin_posts_event_payload() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind HTTP hook listener")?;
    let addr = listener
        .local_addr()
        .context("failed to read listener address")?;
    let app = Router::new()
        .route(
            "/hook",
            post(
                |State(tx): State<mpsc::Sender<Vec<u8>>>, body: Bytes| async move {
                    if tx.send(body.to_vec()).await.is_err() {
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                    StatusCode::OK
                },
            ),
        )
        .with_state(tx.clone());

    let server_handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            eprintln!("hook server error: {err}");
        }
    });

    let endpoint = format!("http://{}/hook", addr);
    let plugin_definition = PluginDefinition {
        enabled: true,
        name: Some("http-regression".to_string()),
        config: PluginConfig::Http(HttpPluginConfig {
            endpoint,
            headers: BTreeMap::from([("X-Test".to_string(), "ok".to_string())]),
            https: false,
        }),
    };

    let plugin = instantiate_plugin(&plugin_definition, &Config::default());

    let record = EventRecord {
        aggregate_type: "order".into(),
        aggregate_id: "order-123".into(),
        event_type: "OrderCreated".into(),
        payload: json!({ "status": "created" }),
        metadata: EventMetadata {
            event_id: uuid::Uuid::new_v4(),
            created_at: Utc::now(),
            issued_by: None,
            note: Some("regression".into()),
        },
        version: 5,
        hash: "hash-order-123-v5".into(),
        merkle_root: "merkle-order-123-v5".into(),
    };
    let state = AggregateState {
        aggregate_type: record.aggregate_type.clone(),
        aggregate_id: record.aggregate_id.clone(),
        version: 5,
        state: BTreeMap::from([("status".into(), "created".into())]),
        merkle_root: "state-root".into(),
        archived: false,
    };

    plugin
        .notify_event(&record, &state, None)
        .context("plugin notify_event failed")?;

    let body = rx
        .recv()
        .await
        .context("server did not receive plugin request")?;
    server_handle.abort();

    let received: Value =
        serde_json::from_slice(&body).context("failed to decode posted payload as JSON")?;
    assert_eq!(received["aggregate_type"], "order");
    assert_eq!(received["aggregate_id"], "order-123");
    assert_eq!(received["event_type"], "OrderCreated");
    assert_eq!(received["version"], 5);
    assert_eq!(received["metadata"]["note"], "regression");
    assert_eq!(
        received["metadata"]["event_id"],
        record.metadata.event_id.to_string()
    );

    Ok(())
}
