use std::{io, net::TcpListener, path::PathBuf, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use eventdbx::{
    api_grpc::proto::{
        AppendEventRequest, GetAggregateRequest, ListAggregatesRequest, ListEventsRequest,
        VerifyAggregateRequest, event_service_client::EventServiceClient,
    },
    config::Config,
    server,
    token::{IssueTokenInput, TokenManager},
};
use serde_json::json;
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};
use tonic::{Code, Request, Status};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_and_query_flow() -> TestResult<()> {
    let temp = TempDir::new()?;
    let mut config = Config::default();
    config.data_dir = temp.path().join("data");
    let http_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping grpc regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let grpc_port = match allocate_port() {
        Ok(port) => port,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping grpc regression test: port binding not permitted ({err})");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    config.port = http_port;
    config.restrict = false;
    config.data_encryption_key = Some(STANDARD.encode([7u8; 32]));
    config.grpc.enabled = true;
    config.grpc.bind_addr = format!("127.0.0.1:{grpc_port}");
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
            user: "grpc-regression".into(),
            expiration_secs: Some(3600),
            limit: None,
            keep_alive: true,
        })?
        .token;
    drop(token_manager);

    let server_handle = spawn_server(config.clone(), config_path.clone())?;

    let base_url = format!("http://127.0.0.1:{}", http_port);
    wait_for_http_health(&base_url).await?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = wait_for_grpc(&endpoint).await?;

    let aggregate_type = "grpc-person";
    let aggregate_id = "gp-001";
    let payload_json = json!({
        "status": "active",
        "notes": "Created via gRPC regression test"
    })
    .to_string();

    let mut append_request = Request::new(AppendEventRequest {
        aggregate_type: aggregate_type.to_string(),
        aggregate_id: aggregate_id.to_string(),
        event_type: "person-upserted".to_string(),
        payload_json: payload_json.clone(),
    });
    set_bearer(&mut append_request, &token)?;
    let append_response = client.append_event(append_request).await?;
    let event = append_response
        .into_inner()
        .event
        .expect("appendEvent should return an event");

    assert_eq!(event.aggregate_type, aggregate_type);
    assert_eq!(event.aggregate_id, aggregate_id);
    assert_eq!(event.event_type, "person-upserted");
    assert_eq!(event.version, 1);
    let event_payload: serde_json::Value = serde_json::from_str(&event.payload_json)?;
    assert_eq!(event_payload["notes"], "Created via gRPC regression test");
    let merkle_root = event.merkle_root.clone();

    let aggregates_response = client
        .list_aggregates(ListAggregatesRequest { skip: 0, take: 10 })
        .await?
        .into_inner();
    let aggregate_entry = aggregates_response
        .aggregates
        .iter()
        .find(|agg| agg.aggregate_type == aggregate_type && agg.aggregate_id == aggregate_id)
        .expect("aggregate should appear in aggregates list");
    assert_eq!(aggregate_entry.version, 1);
    assert_eq!(
        aggregate_entry.state.get("status").map(String::as_str),
        Some("active")
    );

    let aggregate_response = client
        .get_aggregate(GetAggregateRequest {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
        })
        .await?
        .into_inner()
        .aggregate
        .expect("aggregate query should return a record");
    assert_eq!(aggregate_response.version, 1);
    assert_eq!(
        aggregate_response.state.get("notes").map(String::as_str),
        Some("Created via gRPC regression test")
    );

    let events_response = client
        .list_events(ListEventsRequest {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
            skip: 0,
            take: 10,
        })
        .await?
        .into_inner();
    assert_eq!(events_response.events.len(), 1);
    let first_event = &events_response.events[0];
    assert_eq!(first_event.event_type, "person-upserted");
    assert_eq!(first_event.version, 1);
    assert_eq!(first_event.merkle_root, merkle_root);
    let first_payload: serde_json::Value = serde_json::from_str(&first_event.payload_json)?;
    assert_eq!(
        first_payload["status"], "active",
        "payload status should match"
    );

    let verify_response = client
        .verify_aggregate(VerifyAggregateRequest {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: aggregate_id.to_string(),
        })
        .await?
        .into_inner();
    assert_eq!(verify_response.merkle_root, merkle_root);

    let unauthorized_result = client
        .append_event(Request::new(AppendEventRequest {
            aggregate_type: aggregate_type.to_string(),
            aggregate_id: "gp-unauthorized".to_string(),
            event_type: "person-upserted".to_string(),
            payload_json,
        }))
        .await;
    match unauthorized_result {
        Err(status) => assert_eq!(status.code(), Code::Unauthenticated),
        Ok(_) => panic!("unauthorized append should fail"),
    }

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

async fn wait_for_http_health(base_url: &str) -> TestResult<()> {
    for _ in 0..40 {
        if let Ok(resp) = reqwest::get(format!("{base_url}/health")).await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err("server did not become healthy in time".into())
}

async fn wait_for_grpc(
    endpoint: &str,
) -> TestResult<EventServiceClient<tonic::transport::Channel>> {
    for _ in 0..40 {
        match EventServiceClient::connect(endpoint.to_string()).await {
            Ok(client) => return Ok(client),
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    Err("grpc server did not become ready in time".into())
}

fn set_bearer<T>(request: &mut Request<T>, token: &str) -> Result<(), Status> {
    let value = format!("Bearer {token}");
    let header_value = value
        .parse()
        .map_err(|_| Status::unauthenticated("invalid bearer token"))?;
    request.metadata_mut().insert("authorization", header_value);
    Ok(())
}
