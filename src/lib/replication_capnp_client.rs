use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use capnp::message::ReaderOptions;
use capnp::serialize::{OwnedSegments, write_message_to_words};
use capnp_futures::serialize::read_message;
use futures::AsyncWriteExt;
use serde_json::{self, Value};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::{
    replication_capnp::{
        replication_hello, replication_hello_response, replication_request, replication_response,
    },
    schema::AggregateSchema,
    store::{AggregatePositionEntry, EventMetadata, EventRecord},
};

pub const REPLICATION_PROTOCOL_VERSION: u16 = 1;

pub struct CapnpReplicationClient {
    reader: Compat<tokio::net::tcp::OwnedReadHalf>,
    writer: Compat<tokio::net::tcp::OwnedWriteHalf>,
}

impl CapnpReplicationClient {
    pub async fn connect(endpoint: &str, expected_key: &[u8]) -> Result<Self> {
        let stream = TcpStream::connect(endpoint)
            .await
            .with_context(|| format!("failed to connect to replication endpoint {endpoint}"))?;
        let (reader_half, writer_half) = stream.into_split();
        let mut reader = reader_half.compat();
        let mut writer = writer_half.compat_write();

        send_handshake(&mut reader, &mut writer, expected_key).await?;

        Ok(Self { reader, writer })
    }

    pub async fn list_positions(&mut self) -> Result<Vec<AggregatePositionEntry>> {
        let response = self
            .send_request(|mut request| {
                request.set_list_positions(());
                Ok(())
            })
            .await?;

        match response
            .get_root::<replication_response::Reader>()?
            .which()?
        {
            replication_response::Which::ListPositions(Ok(list)) => {
                let positions = list.get_positions()?;
                let mut entries = Vec::with_capacity(positions.len() as usize);
                for position in positions.iter() {
                    entries.push(AggregatePositionEntry {
                        aggregate_type: read_text(position.get_aggregate_type(), "aggregate type")?,
                        aggregate_id: read_text(position.get_aggregate_id(), "aggregate id")?,
                        version: position.get_version(),
                    });
                }
                Ok(entries)
            }
            replication_response::Which::ListPositions(Err(err)) => {
                Err(anyhow!("failed to decode list_positions response: {err}"))
            }
            replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode error response: {err}"))
            }
            _ => bail!("unexpected response variant for list_positions response"),
        }
    }

    pub async fn pull_events(
        &mut self,
        aggregate_type: &str,
        aggregate_id: &str,
        from_version: u64,
        limit: Option<usize>,
    ) -> Result<Vec<EventRecord>> {
        let limit = limit.unwrap_or(0).min(u32::MAX as usize) as u32;
        let response = self
            .send_request(|request| {
                let mut pull = request.init_pull_events();
                pull.set_aggregate_type(aggregate_type);
                pull.set_aggregate_id(aggregate_id);
                pull.set_from_version(from_version);
                pull.set_limit(limit);
                Ok(())
            })
            .await?;

        match response
            .get_root::<replication_response::Reader>()?
            .which()?
        {
            replication_response::Which::PullEvents(Ok(list)) => {
                let events = list.get_events()?;
                let mut records = Vec::with_capacity(events.len() as usize);
                for event in events.iter() {
                    records.push(decode_event(event)?);
                }
                Ok(records)
            }
            replication_response::Which::PullEvents(Err(err)) => {
                Err(anyhow!("failed to decode pull_events response: {err}"))
            }
            replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode error response: {err}"))
            }
            _ => bail!("unexpected response variant for pull_events response"),
        }
    }

    pub async fn apply_events(&mut self, sequence: u64, events: &[EventRecord]) -> Result<u64> {
        let response = self
            .send_request(|request| {
                let mut apply = request.init_apply_events();
                apply.set_sequence(sequence);
                let mut list = apply.reborrow().init_events(events.len() as u32);
                for (idx, event) in events.iter().enumerate() {
                    encode_event(list.reborrow().get(idx as u32), event)?;
                }
                Ok(())
            })
            .await?;

        match response
            .get_root::<replication_response::Reader>()?
            .which()?
        {
            replication_response::Which::ApplyEvents(Ok(resp)) => Ok(resp.get_applied_sequence()),
            replication_response::Which::ApplyEvents(Err(err)) => {
                Err(anyhow!("failed to decode apply_events response: {err}"))
            }
            replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode error response: {err}"))
            }
            _ => bail!("unexpected response variant for apply_events response"),
        }
    }

    pub async fn pull_schemas(&mut self) -> Result<Vec<u8>> {
        let response = self
            .send_request(|mut request| {
                request.set_pull_schemas(());
                Ok(())
            })
            .await?;

        match response
            .get_root::<replication_response::Reader>()?
            .which()?
        {
            replication_response::Which::PullSchemas(Ok(resp)) => {
                Ok(resp.get_schemas_json()?.to_vec())
            }
            replication_response::Which::PullSchemas(Err(err)) => {
                Err(anyhow!("failed to decode pull_schemas response: {err}"))
            }
            replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode error response: {err}"))
            }
            _ => bail!("unexpected response variant for pull_schemas response"),
        }
    }

    pub async fn apply_schemas(&mut self, payload: &[u8]) -> Result<u32> {
        let response = self
            .send_request(|request| {
                let mut apply = request.init_apply_schemas();
                apply.set_schemas_json(payload);
                Ok(())
            })
            .await?;

        match response
            .get_root::<replication_response::Reader>()?
            .which()?
        {
            replication_response::Which::ApplySchemas(Ok(resp)) => Ok(resp.get_aggregate_count()),
            replication_response::Which::ApplySchemas(Err(err)) => {
                Err(anyhow!("failed to decode apply_schemas response: {err}"))
            }
            replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode error response: {err}"))
            }
            _ => bail!("unexpected response variant for apply_schemas response"),
        }
    }

    async fn send_request<F>(&mut self, build: F) -> Result<capnp::message::Reader<OwnedSegments>>
    where
        F: FnOnce(replication_request::Builder<'_>) -> Result<()>,
    {
        let request_bytes = {
            let mut message = capnp::message::Builder::new_default();
            {
                let request = message.init_root::<replication_request::Builder>();
                build(request)?;
            }
            write_message_to_words(&message)
        };

        self.writer
            .write_all(&request_bytes)
            .await
            .context("failed to write replication request")?;
        self.writer
            .flush()
            .await
            .context("failed to flush replication request")?;

        read_message(&mut self.reader, ReaderOptions::new())
            .await
            .context("failed to read replication response")
    }
}

async fn send_handshake<R, W>(reader: &mut R, writer: &mut W, expected_key: &[u8]) -> Result<()>
where
    R: futures::AsyncRead + Unpin,
    W: futures::AsyncWrite + Unpin,
{
    let hello_bytes = {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut hello = message.init_root::<replication_hello::Builder>();
            hello.set_protocol_version(REPLICATION_PROTOCOL_VERSION);
            hello.set_expected_public_key(expected_key);
        }
        write_message_to_words(&message)
    };

    writer
        .write_all(&hello_bytes)
        .await
        .context("failed to send replication handshake")?;
    writer
        .flush()
        .await
        .context("failed to flush replication handshake")?;

    let response = read_message(reader, ReaderOptions::new())
        .await
        .context("failed to read replication handshake response")?;
    let hello = response
        .get_root::<replication_hello_response::Reader>()
        .context("failed to decode replication handshake response")?;
    if !hello.get_accepted() {
        let reason = read_text(hello.get_message(), "handshake rejection message")?;
        bail!("replication handshake rejected: {reason}");
    }
    Ok(())
}

fn encode_event(
    mut builder: crate::replication_capnp::event_record::Builder<'_>,
    event: &EventRecord,
) -> Result<()> {
    builder.set_aggregate_type(&event.aggregate_type);
    builder.set_aggregate_id(&event.aggregate_id);
    builder.set_event_type(&event.event_type);
    builder.set_version(event.version);
    builder.set_merkle_root(&event.merkle_root);
    builder.set_hash(&event.hash);

    let payload = serde_json::to_vec(&event.payload)
        .map_err(|err| anyhow!("failed to encode event payload: {err}"))?;
    builder.set_payload(&payload);

    let metadata = serde_json::to_vec(&event.metadata)
        .map_err(|err| anyhow!("failed to encode event metadata: {err}"))?;
    builder.set_metadata(&metadata);

    Ok(())
}

fn decode_event(reader: crate::replication_capnp::event_record::Reader<'_>) -> Result<EventRecord> {
    let aggregate_type = read_text(reader.get_aggregate_type(), "aggregate type")?;
    let aggregate_id = read_text(reader.get_aggregate_id(), "aggregate id")?;
    let event_type = read_text(reader.get_event_type(), "event type")?;
    let version = reader.get_version();
    let merkle_root = read_text(reader.get_merkle_root(), "merkle root")?;
    let hash = read_text(reader.get_hash(), "hash")?;

    let payload_bytes = reader
        .get_payload()
        .map_err(|err| anyhow!("failed to read event payload: {err}"))?;
    let metadata_bytes = reader
        .get_metadata()
        .map_err(|err| anyhow!("failed to read event metadata: {err}"))?;

    let payload: Value = serde_json::from_slice(payload_bytes)
        .map_err(|err| anyhow!("failed to decode event payload: {err}"))?;
    let metadata: EventMetadata = serde_json::from_slice(metadata_bytes)
        .map_err(|err| anyhow!("failed to decode event metadata: {err}"))?;

    Ok(EventRecord {
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        metadata,
        version,
        hash,
        merkle_root,
    })
}

fn read_text(value: capnp::Result<capnp::text::Reader<'_>>, field: &str) -> Result<String> {
    let reader = value.map_err(|err| anyhow!("failed to read {field}: {err}"))?;
    reader
        .to_str()
        .map_err(|err| anyhow!("invalid UTF-8 in {field}: {err}"))
        .map(|s| s.to_string())
}

pub fn decode_schemas(payload: &[u8]) -> Result<BTreeMap<String, AggregateSchema>> {
    if payload.is_empty() {
        Ok(BTreeMap::new())
    } else {
        serde_json::from_slice(payload)
            .map_err(|err| anyhow!("failed to decode schema payload: {err}"))
    }
}

pub fn normalize_capnp_endpoint(endpoint: &str) -> Result<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        bail!("remote endpoint cannot be empty");
    }

    if let Some(rest) = trimmed.strip_prefix("tcp://") {
        Ok(rest.to_string())
    } else if let Some(rest) = trimmed.strip_prefix("capnp://") {
        Ok(rest.to_string())
    } else if trimmed.contains("://") {
        bail!("unsupported replication endpoint scheme: {endpoint}");
    } else {
        Ok(trimmed.to_string())
    }
}

pub fn decode_public_key_bytes(raw: &str) -> Result<Vec<u8>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("public key cannot be empty");
    }

    let bytes = STANDARD_NO_PAD
        .decode(trimmed)
        .or_else(|_| STANDARD.decode(trimmed))
        .map_err(|err| anyhow!("invalid base64 public key: {err}"))?;
    if bytes.len() != 32 {
        bail!("public key must decode to 32 bytes");
    }
    Ok(bytes)
}
