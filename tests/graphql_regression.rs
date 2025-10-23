use std::{net::TcpListener, path::PathBuf, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use eventdbx::{
    config::Config,
    restrict::RestrictMode,
    server,
    token::{IssueTokenInput, TokenManager},
};
use reqwest::Client;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread")]
async fn graphql_append_and_query_flow() -> TestResult<()> {
    let temp = TempDir::new()?;
    let mut config = Config::default();
    config.data_dir = temp.path().join("data");
    let port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping graphql regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let socket_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping graphql regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    config.port = port;
    config.restrict = RestrictMode::Off;
    config.data_encryption_key = Some(STANDARD.encode([42u8; 32]));
    config.socket.bind_addr = format!("127.0.0.1:{socket_port}");
    config.ensure_data_dir()?;
    let config_path = temp.path().join("config.toml");
    config.save(&config_path)?;

    let encryptor = config
        .encryption_key()?
        .expect("encryption key should be configured");
    let token_manager = TokenManager::load(config.tokens_path(), Some(encryptor.clone()))?;
    let token = token_manager
        .issue(IssueTokenInput {
            group: "testers".into(),
            user: "graphql-regression".into(),
            expiration_secs: Some(3600),
            limit: None,
            keep_alive: true,
        })?
        .token;
    drop(token_manager);

    let server_handle = spawn_server(config.clone(), config_path.clone())?;

    let base_url = format!("http://127.0.0.1:{}", config.port);
    wait_for_health(&base_url).await?;
    let client = Client::new();

    let aggregate_type = "graphql-person";
    let aggregate_id = "gp-001";
    let mutation = r#"
        mutation Append($input: AppendEventInput!) {
            appendEvent(input: $input) {
                aggregateType
                aggregateId
                eventType
                version
                payload
                merkleRoot
                metadata {
                    note
                }
            }
        }
    "#;

    let event_payload = json!({
        "status": "active",
        "notes": "Created via GraphQL regression test"
    });
    let event_note = "GraphQL regression bootstrap";

    let mutation_response = graphql_call(
        &client,
        &base_url,
        Some(&token),
        mutation,
        json!({ "input": {
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id,
            "eventType": "person-upserted",
            "payload": event_payload.clone(),
            "note": event_note
        }}),
    )
    .await?;

    assert!(
        mutation_response.get("errors").is_none(),
        "unexpected GraphQL errors: {:?}",
        mutation_response
    );
    let append_event = &mutation_response["data"]["appendEvent"];
    assert_eq!(
        append_event["aggregateType"].as_str(),
        Some(aggregate_type),
        "aggregate type should match"
    );
    assert_eq!(
        append_event["aggregateId"].as_str(),
        Some(aggregate_id),
        "aggregate id should match"
    );
    assert_eq!(
        append_event["eventType"].as_str(),
        Some("person-upserted"),
        "event type should match"
    );
    assert_eq!(
        append_event["version"].as_u64(),
        Some(1),
        "version should start at 1"
    );
    assert_eq!(
        append_event["metadata"]["note"].as_str(),
        Some(event_note),
        "metadata note should reflect supplied note"
    );
    let _initial_merkle_root = append_event["merkleRoot"]
        .as_str()
        .expect("merkle root should be present")
        .to_string();

    let aggregates_query = r#"
        query Aggregates($take: Int!) {
            aggregates(take: $take) {
                aggregateType
                aggregateId
                version
                state
            }
        }
    "#;
    let aggregates_response = graphql_call(
        &client,
        &base_url,
        None,
        aggregates_query,
        json!({ "take": 10 }),
    )
    .await?;
    assert!(
        aggregates_response.get("errors").is_none(),
        "aggregates query should succeed: {:?}",
        aggregates_response
    );
    let aggregates = aggregates_response["data"]["aggregates"]
        .as_array()
        .expect("aggregates response should be array");
    let found = aggregates.iter().find(|entry| {
        entry["aggregateType"].as_str() == Some(aggregate_type)
            && entry["aggregateId"].as_str() == Some(aggregate_id)
    });
    let aggregate_entry = found.expect("aggregate should be returned by aggregates query");
    assert_eq!(
        aggregate_entry["version"].as_u64(),
        Some(1),
        "aggregate version should be 1"
    );
    assert_eq!(
        aggregate_entry["state"]["status"].as_str(),
        Some("active"),
        "aggregate state should include persisted status"
    );

    let events_query = r#"
        query Events($aggregateType: String!, $aggregateId: String!) {
            aggregateEvents(aggregateType: $aggregateType, aggregateId: $aggregateId) {
                eventType
                metadata {
                    note
                }
            }
        }
    "#;
    let events_response = graphql_call(
        &client,
        &base_url,
        None,
        events_query,
        json!({
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id
        }),
    )
    .await?;
    assert!(
        events_response.get("errors").is_none(),
        "events query should succeed: {:?}",
        events_response
    );
    let events = events_response["data"]["aggregateEvents"]
        .as_array()
        .expect("events response should be array");
    assert_eq!(events.len(), 1, "should return a single event");
    assert_eq!(
        events[0]["metadata"]["note"].as_str(),
        Some(event_note),
        "event metadata should propagate note"
    );

    let aggregate_query = r#"
        query Aggregate($aggregateType: String!, $aggregateId: String!) {
            aggregate(aggregateType: $aggregateType, aggregateId: $aggregateId) {
                aggregateType
                aggregateId
                version
                state
            }
        }
    "#;
    let aggregate_response = graphql_call(
        &client,
        &base_url,
        None,
        aggregate_query,
        json!({
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id
        }),
    )
    .await?;
    assert!(
        aggregate_response.get("errors").is_none(),
        "aggregate query should succeed: {:?}",
        aggregate_response
    );
    let aggregate = aggregate_response["data"]["aggregate"]
        .as_object()
        .expect("aggregate query should return an object");
    assert_eq!(
        aggregate.get("aggregateType").and_then(Value::as_str),
        Some(aggregate_type),
        "aggregate query type should match"
    );
    assert_eq!(
        aggregate.get("aggregateId").and_then(Value::as_str),
        Some(aggregate_id),
        "aggregate query id should match"
    );
    assert_eq!(
        aggregate.get("version").and_then(Value::as_u64),
        Some(1),
        "aggregate version should be 1"
    );
    let aggregate_state = aggregate
        .get("state")
        .and_then(Value::as_object)
        .expect("aggregate state should be an object");
    assert_eq!(
        aggregate_state.get("notes").and_then(Value::as_str),
        Some("Created via GraphQL regression test"),
        "aggregate state should retain notes"
    );

    let patch_document = json!([
        { "op": "replace", "path": "/status", "value": "inactive" },
        { "op": "replace", "path": "/contact/address/city", "value": "Spokane" }
    ]);
    let patch_note = "GraphQL regression patch";

    let patch_response = graphql_call(
        &client,
        &base_url,
        Some(&token),
        mutation,
        json!({ "input": {
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id,
            "eventType": "person-patched",
            "patch": patch_document,
            "note": patch_note
        }}),
    )
    .await?;
    assert!(
        patch_response.get("errors").is_none(),
        "patch mutation should succeed: {:?}",
        patch_response
    );
    let patch_event = &patch_response["data"]["appendEvent"];
    assert_eq!(
        patch_event["metadata"]["note"].as_str(),
        Some(patch_note),
        "patch metadata note should reflect supplied value"
    );
    let merkle_root = patch_event["merkleRoot"]
        .as_str()
        .expect("patch mutation should return merkle root")
        .to_string();

    let aggregate_after_patch = graphql_call(
        &client,
        &base_url,
        None,
        aggregate_query,
        json!({
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id
        }),
    )
    .await?;
    assert!(
        aggregate_after_patch.get("errors").is_none(),
        "aggregate query after patch should succeed: {:?}",
        aggregate_after_patch
    );
    let aggregate = aggregate_after_patch["data"]["aggregate"]
        .as_object()
        .expect("aggregate query should return an object");
    assert_eq!(
        aggregate.get("version").and_then(Value::as_u64),
        Some(2),
        "aggregate version should advance after patch"
    );
    let aggregate_state = aggregate
        .get("state")
        .and_then(Value::as_object)
        .expect("aggregate state should be an object");
    assert_eq!(
        aggregate_state.get("status").and_then(Value::as_str),
        Some("inactive"),
        "patch should update status"
    );
    assert_eq!(
        aggregate_state
            .get("contact")
            .and_then(Value::as_object)
            .and_then(|contact| contact
                .get("address")
                .and_then(Value::as_object)
                .and_then(|address| address.get("city").and_then(Value::as_str))),
        Some("Spokane"),
        "patch should update nested contact address"
    );

    let events_query = r#"
        query Events($aggregateType: String!, $aggregateId: String!) {
            aggregateEvents(aggregateType: $aggregateType, aggregateId: $aggregateId) {
                eventType
                version
                payload
                metadata {
                    note
                }
            }
        }
    "#;
    let events_response = graphql_call(
        &client,
        &base_url,
        None,
        events_query,
        json!({
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id
        }),
    )
    .await?;
    assert!(
        events_response.get("errors").is_none(),
        "events query should succeed: {:?}",
        events_response
    );
    let events = events_response["data"]["aggregateEvents"]
        .as_array()
        .expect("events response should be array");
    assert_eq!(
        events.len(),
        2,
        "should have both creation and patch events"
    );
    let first_event = &events[0];
    assert_eq!(
        first_event["payload"]["status"].as_str(),
        Some("active"),
        "first event payload should match initial status"
    );
    assert_eq!(
        first_event["metadata"]["note"].as_str(),
        Some(event_note),
        "first event note should match initial mutation"
    );
    let second_event = &events[1];
    assert_eq!(
        second_event["eventType"].as_str(),
        Some("person-patched"),
        "second event should reflect patch"
    );
    assert_eq!(
        second_event["metadata"]["note"].as_str(),
        Some(patch_note),
        "second event should include patch note"
    );
    assert_eq!(
        second_event["payload"]["contact"]["address"]["city"].as_str(),
        Some("Spokane"),
        "second event payload should include patched city"
    );

    let verify_query = r#"
        query Verify($aggregateType: String!, $aggregateId: String!) {
            verifyAggregate(aggregateType: $aggregateType, aggregateId: $aggregateId) {
                merkleRoot
            }
        }
    "#;
    let verify_response = graphql_call(
        &client,
        &base_url,
        None,
        verify_query,
        json!({
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id
        }),
    )
    .await?;
    let verify_merkle = verify_response["data"]["verifyAggregate"]["merkleRoot"]
        .as_str()
        .expect("verifyAggregate should return merkle root");
    assert_eq!(
        verify_merkle, merkle_root,
        "verifyAggregate should return the event merkle root"
    );

    let unauthorized_response = graphql_call(
        &client,
        &base_url,
        None,
        mutation,
        json!({ "input": {
            "aggregateType": aggregate_type,
            "aggregateId": "gp-unauthorized",
            "eventType": "person-upserted",
            "payload": event_payload
        }}),
    )
    .await?;
    let errors = unauthorized_response["errors"]
        .as_array()
        .expect("unauthorized mutation should return errors");
    assert!(
        errors
            .iter()
            .any(|err| err["message"].as_str() == Some("unauthorized")),
        "unauthorized mutation should include unauthorized error"
    );

    server_handle.abort();
    let _ = server_handle.await;
    Ok(())
}

fn allocate_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn spawn_server(
    config: Config,
    config_path: PathBuf,
) -> TestResult<JoinHandle<eventdbx::error::Result<()>>> {
    Ok(tokio::spawn(async move {
        server::run(config, config_path).await
    }))
}

async fn wait_for_health(base_url: &str) -> TestResult<()> {
    let client = Client::new();
    for _ in 0..40 {
        if let Ok(resp) = client.get(format!("{base_url}/health")).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err("server did not become healthy in time".into())
}

async fn graphql_call(
    client: &Client,
    base_url: &str,
    token: Option<&str>,
    query: &str,
    variables: Value,
) -> TestResult<Value> {
    let mut request = client
        .post(format!("{base_url}/graphql"))
        .header("Content-Type", "application/json");
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .json(&json!({ "query": query, "variables": variables }))
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("graphql request failed ({status}): {body}").into());
    }

    let payload: Value = response.json().await?;
    Ok(payload)
}
