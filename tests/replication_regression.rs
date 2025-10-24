use std::{io, net::SocketAddr};

use anyhow::{Context, Result, anyhow};
use capnp::{message::Builder, serialize::write_message_to_words};
use capnp_futures::serialize::read_message;
use chrono::{TimeZone, Utc};
use eventdbx::{
    replication_capnp::{
        replication_hello, replication_hello_response, replication_request, replication_response,
    },
    replication_capnp_client::{CapnpReplicationClient, REPLICATION_PROTOCOL_VERSION},
    store::{AggregatePositionEntry, EventMetadata, EventRecord},
};
use futures::AsyncWriteExt;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn capnp_regression_list_positions_and_pull_events() -> Result<()> {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("capnp regression skipped: {}", err);
            return Ok(());
        }
        Err(err) => {
            return Err(err).context("failed to bind mock replication server");
        }
    };
    let addr: SocketAddr = listener
        .local_addr()
        .context("failed to read mock server address")?;

    let expected_key = vec![0xAB; 32];

    let positions = vec![
        AggregatePositionEntry {
            aggregate_type: "order".to_string(),
            aggregate_id: "order-1".to_string(),
            version: 3,
        },
        AggregatePositionEntry {
            aggregate_type: "inventory".to_string(),
            aggregate_id: "sku-42".to_string(),
            version: 7,
        },
    ];

    let event_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("static uuid");
    let created_at = Utc
        .timestamp_opt(1_700_000_000, 123_000)
        .single()
        .expect("valid timestamp");
    let sample_event = EventRecord {
        aggregate_type: "order".to_string(),
        aggregate_id: "order-1".to_string(),
        event_type: "OrderCreated".to_string(),
        payload: json!({"status": "created"}),
        metadata: EventMetadata {
            event_id,
            created_at,
            issued_by: None,
            note: Some("initial import".to_string()),
        },
        version: 3,
        hash: "hash-order-1-v3".to_string(),
        merkle_root: "root-order-1-v3".to_string(),
    };

    let server_handle = tokio::spawn(run_mock_replication_server(
        listener,
        expected_key.clone(),
        positions.clone(),
        vec![sample_event.clone()],
    ));

    let endpoint = addr.to_string();
    let mut client = CapnpReplicationClient::connect(&endpoint, &expected_key)
        .await
        .context("client failed to connect to mock server")?;

    let received_positions = client
        .list_positions()
        .await
        .context("list_positions call failed")?;
    assert_eq!(received_positions.len(), positions.len());
    for (expected, actual) in positions.iter().zip(received_positions.iter()) {
        assert_eq!(expected.aggregate_type, actual.aggregate_type);
        assert_eq!(expected.aggregate_id, actual.aggregate_id);
        assert_eq!(expected.version, actual.version);
    }

    let pulled_events = client
        .pull_events("order", "order-1", 0, Some(10))
        .await
        .context("pull_events call failed")?;
    assert_eq!(pulled_events.len(), 1);
    let event = &pulled_events[0];
    assert_eq!(event.aggregate_type, sample_event.aggregate_type);
    assert_eq!(event.aggregate_id, sample_event.aggregate_id);
    assert_eq!(event.event_type, sample_event.event_type);
    assert_eq!(event.version, sample_event.version);
    assert_eq!(event.hash, sample_event.hash);
    assert_eq!(event.merkle_root, sample_event.merkle_root);
    assert_eq!(event.payload, sample_event.payload);
    assert_eq!(event.metadata.event_id, sample_event.metadata.event_id);
    assert_eq!(event.metadata.created_at, sample_event.metadata.created_at);
    assert_eq!(event.metadata.note, sample_event.metadata.note);
    assert_eq!(event.metadata.issued_by.is_none(), true);

    drop(client);

    server_handle
        .await
        .context("mock server task panicked")?
        .context("mock server failed")?;

    Ok(())
}

async fn run_mock_replication_server(
    listener: TcpListener,
    expected_key: Vec<u8>,
    positions: Vec<AggregatePositionEntry>,
    events: Vec<EventRecord>,
) -> Result<()> {
    let (stream, _) = listener
        .accept()
        .await
        .context("failed to accept mock replication connection")?;

    let (read_half, write_half) = stream.into_split();
    let mut reader = read_half.compat();
    let mut writer = write_half.compat_write();

    let hello_message = read_message(&mut reader, Default::default())
        .await
        .context("failed to read handshake")?;
    {
        let hello = hello_message
            .get_root::<replication_hello::Reader>()
            .context("failed to decode handshake")?;
        if hello.get_protocol_version() != REPLICATION_PROTOCOL_VERSION {
            return Err(anyhow!(
                "unexpected protocol version {}",
                hello.get_protocol_version()
            ));
        }
        let received_key = hello
            .get_expected_public_key()
            .context("failed to read expected key")?;
        if received_key != expected_key.as_slice() {
            return Err(anyhow!("handshake carried unexpected key"));
        }
    }

    let mut hello_response = Builder::new_default();
    {
        let mut response = hello_response.init_root::<replication_hello_response::Builder>();
        response.set_accepted(true);
        response.set_message("");
    }
    let response_bytes = write_message_to_words(&hello_response);
    writer
        .write_all(&response_bytes)
        .await
        .context("failed to send handshake response")?;
    writer.flush().await.context("failed to flush handshake")?;

    let list_request_message = read_message(&mut reader, Default::default())
        .await
        .context("failed to read list_positions request")?;
    {
        let list_request = list_request_message
            .get_root::<replication_request::Reader>()
            .context("failed to decode list_positions request")?;
        if !matches!(
            list_request
                .which()
                .context("invalid request discriminant")?,
            replication_request::Which::ListPositions(())
        ) {
            return Err(anyhow!("unexpected first request"));
        }
    }

    let mut response = Builder::new_default();
    {
        let mut envelope = response.init_root::<replication_response::Builder>();
        let mut ok = envelope.reborrow().init_list_positions();
        let mut list = ok.reborrow().init_positions(positions.len() as u32);
        for (idx, entry) in positions.iter().enumerate() {
            let mut builder = list.reborrow().get(idx as u32);
            builder.set_aggregate_type(&entry.aggregate_type);
            builder.set_aggregate_id(&entry.aggregate_id);
            builder.set_version(entry.version);
        }
    }
    let bytes = write_message_to_words(&response);
    writer
        .write_all(&bytes)
        .await
        .context("failed to send list_positions response")?;
    writer
        .flush()
        .await
        .context("failed to flush list_positions response")?;

    let pull_request_message = read_message(&mut reader, Default::default())
        .await
        .context("failed to read pull_events request")?;
    let (requested_type, requested_id, from_version, limit) = {
        let pull_request = pull_request_message
            .get_root::<replication_request::Reader>()
            .context("failed to decode pull_events request")?;
        match pull_request
            .which()
            .context("invalid request discriminant")?
        {
            replication_request::Which::PullEvents(Ok(request)) => {
                let aggregate_type = request
                    .get_aggregate_type()
                    .context("failed to read aggregate type from request")?
                    .to_str()
                    .context("invalid UTF-8 in aggregate type")?
                    .to_string();
                let aggregate_id = request
                    .get_aggregate_id()
                    .context("failed to read aggregate id from request")?
                    .to_str()
                    .context("invalid UTF-8 in aggregate id")?
                    .to_string();
                let from_version = request.get_from_version();
                let limit = request.get_limit() as usize;
                (aggregate_type, aggregate_id, from_version, limit)
            }
            _ => return Err(anyhow!("unexpected second request")),
        }
    };

    let mut filtered: Vec<EventRecord> = events
        .iter()
        .filter(|event| {
            event.aggregate_type == requested_type
                && event.aggregate_id == requested_id
                && event.version > from_version
        })
        .cloned()
        .collect();
    if limit > 0 && filtered.len() > limit {
        filtered.truncate(limit);
    }

    let mut response = Builder::new_default();
    {
        let mut envelope = response.init_root::<replication_response::Builder>();
        let mut pull = envelope.reborrow().init_pull_events();
        let mut list = pull.reborrow().init_events(filtered.len() as u32);
        for (idx, record) in filtered.iter().enumerate() {
            let mut builder = list.reborrow().get(idx as u32);
            builder.set_aggregate_type(&record.aggregate_type);
            builder.set_aggregate_id(&record.aggregate_id);
            builder.set_event_type(&record.event_type);
            builder.set_version(record.version);
            builder.set_merkle_root(&record.merkle_root);
            builder.set_hash(&record.hash);

            let payload = serde_json::to_vec(&record.payload)
                .context("failed to encode payload for pull_events response")?;
            let metadata = serde_json::to_vec(&record.metadata)
                .context("failed to encode metadata for pull_events response")?;
            builder.set_payload(&payload);
            builder.set_metadata(&metadata);
        }
    }
    let bytes = write_message_to_words(&response);
    writer
        .write_all(&bytes)
        .await
        .context("failed to send pull_events response")?;
    writer
        .flush()
        .await
        .context("failed to flush pull_events response")?;

    Ok(())
}
