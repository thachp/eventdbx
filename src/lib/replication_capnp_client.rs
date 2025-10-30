use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
};

use anyhow::{Context, Result, anyhow, bail};
use capnp::message::ReaderOptions;
use capnp::serialize::{OwnedSegments, write_message_to_words};
use capnp_futures::serialize::read_message as read_async_message;
use futures::AsyncWriteExt;
use serde_json::{self, Value};
use snow::TransportState;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::warn;

use crate::{
    replication_capnp::{
        replication_hello, replication_hello_response, replication_request, replication_response,
    },
    replication_noise::{perform_client_handshake, read_encrypted_frame, write_encrypted_frame},
    schema::AggregateSchema,
    store::{AggregatePositionEntry, EventMetadata, EventRecord},
};

pub const REPLICATION_PROTOCOL_VERSION: u16 = 1;

pub struct CapnpReplicationClient {
    reader: Compat<tokio::net::tcp::OwnedReadHalf>,
    writer: Compat<tokio::net::tcp::OwnedWriteHalf>,
    noise: TransportState,
}

impl CapnpReplicationClient {
    pub async fn connect(endpoint: &str, token: &str) -> Result<Self> {
        Self::connect_with_capnp(endpoint, token).await
    }

    async fn connect_with_capnp(endpoint: &str, token: &str) -> Result<Self> {
        let stream = TcpStream::connect(endpoint)
            .await
            .with_context(|| format!("failed to connect to replication endpoint {endpoint}"))?;
        let (reader_half, writer_half) = stream.into_split();
        let mut reader = reader_half.compat();
        let mut writer = writer_half.compat_write();

        let noise = send_handshake(&mut reader, &mut writer, token).await?;

        Ok(Self {
            reader,
            writer,
            noise,
        })
    }

    pub async fn list_positions(&mut self) -> Result<Vec<AggregatePositionEntry>> {
        let response = self
            .send_request(|mut request| {
                request.set_list_positions(());
                Ok(())
            })
            .await?;

        let root = response.get_root::<replication_response::Reader>()?;
        match root.which()? {
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
            replication_response::Which::Ok(()) => Ok(Vec::new()),
            replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode error response: {err}"))
            }
            replication_response::Which::PullSchemas(_)
            | replication_response::Which::ApplySchemas(_)
            | replication_response::Which::PullEvents(_)
            | replication_response::Which::ApplyEvents(_) => {
                bail!("unexpected response variant for list_positions response");
            }
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
        let response: capnp::message::Reader<OwnedSegments> = self
            .send_request(|request| {
                let mut pull = request.init_pull_events();
                pull.set_aggregate_type(aggregate_type);
                pull.set_aggregate_id(aggregate_id);
                pull.set_from_version(from_version);
                pull.set_limit(limit);
                Ok(())
            })
            .await?;

        let root: replication_response::Reader<'_> =
            response.get_root::<replication_response::Reader>()?;
        match root.which()? {
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
            other => bail!(
                "unexpected response variant for pull_events response: {}",
                replication_response_variant_name(&other)
            ),
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

        let root = response.get_root::<replication_response::Reader>()?;
        match root.which()? {
            replication_response::Which::ApplyEvents(Ok(resp)) => Ok(resp.get_applied_sequence()),
            replication_response::Which::ApplyEvents(Err(err)) => {
                Err(anyhow!("failed to decode apply_events response: {err}"))
            }
            replication_response::Which::PullEvents(Ok(resp)) => {
                let ack = resp
                    .get_events()
                    .map_err(|err| anyhow!("failed to decode legacy apply_events ack: {err}"))?;
                if ack.is_empty() {
                    // Some legacy replicas acknowledge apply_events using an empty pull_events.
                    Ok(sequence)
                } else {
                    Self::ensure_apply_events_ack_matches(events, ack)?;
                    Ok(sequence)
                }
            }
            replication_response::Which::PullEvents(Err(err)) => {
                Err(anyhow!("failed to decode apply_events response: {err}"))
            }
            replication_response::Which::Ok(_) => {
                // Older servers acknowledge success via the generic ok variant.
                Ok(sequence)
            }
            replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode error response: {err}"))
            }
            other => bail!(
                "unexpected response variant for apply_events response: {}",
                replication_response_variant_name(&other)
            ),
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
            other => bail!(
                "unexpected response variant for pull_schemas response: {}",
                replication_response_variant_name(&other)
            ),
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
            other => bail!(
                "unexpected response variant for apply_schemas response: {}",
                replication_response_variant_name(&other)
            ),
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

        self.write_encrypted_message(&request_bytes)
            .await
            .context("failed to send encrypted replication request")?;
        let response_bytes = self
            .read_encrypted_message()
            .await
            .context("failed to read encrypted replication response")?;
        let mut cursor = Cursor::new(&response_bytes);
        capnp::serialize::read_message(&mut cursor, ReaderOptions::new())
            .context("failed to decode replication response message")
    }

    async fn write_encrypted_message(&mut self, payload: &[u8]) -> Result<()> {
        write_encrypted_frame(&mut self.writer, &mut self.noise, payload).await
    }

    async fn read_encrypted_message(&mut self) -> Result<Vec<u8>> {
        match read_encrypted_frame(&mut self.reader, &mut self.noise).await? {
            Some(bytes) => Ok(bytes),
            None => bail!("replication connection closed unexpectedly"),
        }
    }

    fn ensure_apply_events_ack_matches(
        requested: &[EventRecord],
        response: capnp::struct_list::Reader<'_, crate::replication_capnp::event_record::Owned>,
    ) -> Result<()> {
        let requested_len = requested.len();
        let ack_len = response.len() as usize;

        type EventAckKey = (String, String, u64);
        let mut ack_map: HashMap<EventAckKey, Vec<u32>> = HashMap::new();
        for ack_index in 0..ack_len {
            let actual = response.get(ack_index as u32);
            let key = (
                read_text(
                    actual.get_aggregate_type(),
                    "apply_events ack aggregate type",
                )?,
                read_text(actual.get_aggregate_id(), "apply_events ack aggregate id")?,
                actual.get_version(),
            );
            ack_map.entry(key).or_default().push(ack_index as u32);
        }

        let mut missing = Vec::new();
        let mut mismatched = Vec::new();

        for expected in requested {
            let key = (
                expected.aggregate_type.clone(),
                expected.aggregate_id.clone(),
                expected.version,
            );
            match ack_map.get_mut(&key).and_then(|indices| indices.pop()) {
                Some(ack_index) => {
                    let actual = response.get(ack_index);
                    if let Err(err) = Self::ensure_event_ack_matches(expected, actual) {
                        mismatched.push(format!(
                            "{}::{} v{} (index {}): {}",
                            expected.aggregate_type,
                            expected.aggregate_id,
                            expected.version,
                            ack_index,
                            err
                        ));
                    }
                }
                None => {
                    missing.push(format!(
                        "{}::{} v{}",
                        expected.aggregate_type, expected.aggregate_id, expected.version
                    ));
                }
            }
        }

        if ack_len < requested_len || !missing.is_empty() || !mismatched.is_empty() {
            warn!(
                target: "replication",
                requested = requested_len,
                acked = ack_len,
                missing = %missing.join(", "),
                mismatched = %mismatched.join(", "),
                "apply_events ack did not fully match request; continuing for compatibility"
            );
        }

        Ok(())
    }

    fn ensure_event_ack_matches(
        expected: &EventRecord,
        actual: crate::replication_capnp::event_record::Reader<'_>,
    ) -> Result<()> {
        let aggregate_type = read_text(
            actual.get_aggregate_type(),
            "apply_events ack aggregate type",
        )?;
        if expected.aggregate_type != aggregate_type {
            bail!(
                "aggregate type mismatch (expected '{}', got '{}')",
                expected.aggregate_type,
                aggregate_type
            );
        }
        let aggregate_id = read_text(actual.get_aggregate_id(), "apply_events ack aggregate id")?;
        if expected.aggregate_id != aggregate_id {
            bail!(
                "aggregate id mismatch (expected '{}', got '{}')",
                expected.aggregate_id,
                aggregate_id
            );
        }
        let version = actual.get_version();
        if expected.version != version {
            bail!(
                "version mismatch (expected {}, got {})",
                expected.version,
                version
            );
        }
        Ok(())
    }
}

async fn send_handshake<R, W>(reader: &mut R, writer: &mut W, token: &str) -> Result<TransportState>
where
    R: futures::AsyncRead + Unpin,
    W: futures::AsyncWrite + Unpin,
{
    if token.is_empty() {
        bail!("replication token cannot be empty");
    }
    if token.split('.').count() != 3 {
        bail!("replication token must contain three JWT segments");
    }

    let hello_bytes = {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut hello = message.init_root::<replication_hello::Builder>();
            hello.set_protocol_version(REPLICATION_PROTOCOL_VERSION);
            hello.set_token(token);
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

    let response = read_async_message(&mut *reader, ReaderOptions::new())
        .await
        .context("failed to read replication handshake response")?;
    let hello = response
        .get_root::<replication_hello_response::Reader>()
        .context("failed to decode replication handshake response")?;
    if !hello.get_accepted() {
        let reason = read_text(hello.get_message(), "handshake rejection message")?;
        bail!("replication handshake rejected: {reason}");
    }

    perform_client_handshake(reader, writer, token.as_bytes())
        .await
        .context("failed to establish encrypted replication channel")
}

fn replication_response_variant_name<A0, A1, A2, A3, A4, A5>(
    variant: &replication_response::Which<A0, A1, A2, A3, A4, A5>,
) -> &'static str {
    match variant {
        replication_response::Which::Ok(_) => "ok",
        replication_response::Which::ListPositions(_) => "list_positions",
        replication_response::Which::PullEvents(_) => "pull_events",
        replication_response::Which::ApplyEvents(_) => "apply_events",
        replication_response::Which::PullSchemas(_) => "pull_schemas",
        replication_response::Which::ApplySchemas(_) => "apply_schemas",
        replication_response::Which::Error(_) => "error",
    }
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

    if let Some(extensions) = &event.extensions {
        let bytes = serde_json::to_vec(extensions)
            .map_err(|err| anyhow!("failed to encode event extensions: {err}"))?;
        builder.set_extensions(&bytes);
    } else {
        builder.set_extensions(&[]);
    }

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
    let extensions_bytes = reader
        .get_extensions()
        .map_err(|err| anyhow!("failed to read event extensions: {err}"))?;

    let payload: Value = serde_json::from_slice(payload_bytes)
        .map_err(|err| anyhow!("failed to decode event payload: {err}"))?;
    let metadata: EventMetadata = serde_json::from_slice(metadata_bytes)
        .map_err(|err| anyhow!("failed to decode event metadata: {err}"))?;
    let extensions = if extensions_bytes.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice(extensions_bytes)
                .map_err(|err| anyhow!("failed to decode event extensions: {err}"))?,
        )
    };

    Ok(EventRecord {
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        extensions,
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
