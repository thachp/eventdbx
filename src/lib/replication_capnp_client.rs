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
use tracing::warn;

use crate::{
    replication_capnp::{
        replication_hello, replication_hello_response, replication_request, replication_response,
    },
    replication_legacy_capnp as legacy_capnp,
    schema::AggregateSchema,
    store::{AggregatePositionEntry, EventMetadata, EventRecord},
};

pub const REPLICATION_PROTOCOL_VERSION: u16 = 1;

pub struct CapnpReplicationClient {
    reader: Compat<tokio::net::tcp::OwnedReadHalf>,
    writer: Compat<tokio::net::tcp::OwnedWriteHalf>,
    legacy_mode: bool,
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

        Ok(Self {
            reader,
            writer,
            legacy_mode: false,
        })
    }

    pub async fn list_positions(&mut self) -> Result<Vec<AggregatePositionEntry>> {
        let response = self
            .send_request(|mut request| {
                request.set_list_positions(());
                Ok(())
            })
            .await?;

        if !self.legacy_mode {
            let root = response.get_root::<replication_response::Reader>()?;
            match root.which()? {
                replication_response::Which::ListPositions(Ok(list)) => {
                    let positions = list.get_positions()?;
                    let mut entries = Vec::with_capacity(positions.len() as usize);
                    for position in positions.iter() {
                        entries.push(AggregatePositionEntry {
                            aggregate_type: read_text(
                                position.get_aggregate_type(),
                                "aggregate type",
                            )?,
                            aggregate_id: read_text(position.get_aggregate_id(), "aggregate id")?,
                            version: position.get_version(),
                        });
                    }
                    return Ok(entries);
                }
                replication_response::Which::ListPositions(Err(err)) => {
                    return Err(anyhow!("failed to decode list_positions response: {err}"));
                }
                replication_response::Which::Error(Ok(err)) => {
                    let message = read_text(err.get_message(), "error message")?;
                    bail!("remote returned error: {message}");
                }
                replication_response::Which::Error(Err(err)) => {
                    return Err(anyhow!("failed to decode error response: {err}"));
                }
                replication_response::Which::PullSchemas(_)
                | replication_response::Which::ApplySchemas(_) => {
                    bail!("unexpected response variant for list_positions response");
                }
                replication_response::Which::Ok(())
                | replication_response::Which::PullEvents(_)
                | replication_response::Which::ApplyEvents(_) => {
                    // Legacy response shape; fall back below.
                }
            }
        }

        self.parse_list_positions_legacy(&response)
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

        if self.legacy_mode {
            return self.parse_pull_events_legacy(&response);
        }

        {
            let root = response.get_root::<replication_response::Reader>()?;
            match root.which()? {
                replication_response::Which::PullEvents(Ok(list)) => {
                    let events = list.get_events()?;
                    let mut records = Vec::with_capacity(events.len() as usize);
                    for event in events.iter() {
                        records.push(decode_event(event)?);
                    }
                    return Ok(records);
                }
                replication_response::Which::PullEvents(Err(err)) => {
                    return Err(anyhow!("failed to decode pull_events response: {err}"));
                }
                replication_response::Which::Error(Ok(err)) => {
                    let message = read_text(err.get_message(), "error message")?;
                    bail!("remote returned error: {message}");
                }
                replication_response::Which::Error(Err(err)) => {
                    return Err(anyhow!("failed to decode error response: {err}"));
                }
                replication_response::Which::PullSchemas(_)
                | replication_response::Which::ApplySchemas(_) => {
                    bail!("unexpected response variant for pull_events response");
                }
                replication_response::Which::Ok(())
                | replication_response::Which::ListPositions(_)
                | replication_response::Which::ApplyEvents(_) => {
                    // Legacy response shape; fall back below.
                }
            }
        }

        self.parse_pull_events_legacy(&response)
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

        if self.legacy_mode {
            return self.parse_apply_events_legacy(&response);
        }

        {
            let root = response.get_root::<replication_response::Reader>()?;
            match root.which()? {
                replication_response::Which::ApplyEvents(Ok(resp)) => {
                    return Ok(resp.get_applied_sequence());
                }
                replication_response::Which::ApplyEvents(Err(err)) => {
                    return Err(anyhow!("failed to decode apply_events response: {err}"));
                }
                replication_response::Which::Error(Ok(err)) => {
                    let message = read_text(err.get_message(), "error message")?;
                    bail!("remote returned error: {message}");
                }
                replication_response::Which::Error(Err(err)) => {
                    return Err(anyhow!("failed to decode error response: {err}"));
                }
                replication_response::Which::PullSchemas(_)
                | replication_response::Which::ApplySchemas(_) => {
                    bail!("unexpected response variant for apply_events response");
                }
                replication_response::Which::Ok(())
                | replication_response::Which::ListPositions(_)
                | replication_response::Which::PullEvents(_) => {
                    // Legacy response shape; fall back below.
                }
            }
        }

        self.parse_apply_events_legacy(&response)
    }

    pub async fn pull_schemas(&mut self) -> Result<Vec<u8>> {
        if self.legacy_mode {
            bail!(
                "connected remote uses legacy replication protocol that does not support schema synchronization"
            );
        }

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
        if self.legacy_mode {
            bail!(
                "connected remote uses legacy replication protocol that does not support schema synchronization"
            );
        }

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

    fn enable_legacy_mode(&mut self) {
        if !self.legacy_mode {
            self.legacy_mode = true;
            warn!("detected legacy replication protocol; enabling compatibility mode");
        }
    }

    fn parse_list_positions_legacy(
        &mut self,
        response: &capnp::message::Reader<OwnedSegments>,
    ) -> Result<Vec<AggregatePositionEntry>> {
        self.enable_legacy_mode();

        let legacy_root = response.get_root::<legacy_capnp::replication_response::Reader>()?;
        match legacy_root.which()? {
            legacy_capnp::replication_response::Which::ListPositions(Ok(list)) => {
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
            legacy_capnp::replication_response::Which::ListPositions(Err(err)) => Err(anyhow!(
                "failed to decode legacy list_positions response: {err}"
            )),
            legacy_capnp::replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            legacy_capnp::replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode legacy error response: {err}"))
            }
            legacy_capnp::replication_response::Which::PullEvents(_)
            | legacy_capnp::replication_response::Which::ApplyEvents(_) => {
                bail!("legacy remote returned unexpected response variant for list_positions");
            }
        }
    }

    fn parse_pull_events_legacy(
        &mut self,
        response: &capnp::message::Reader<OwnedSegments>,
    ) -> Result<Vec<EventRecord>> {
        self.enable_legacy_mode();

        let legacy_root = response.get_root::<legacy_capnp::replication_response::Reader>()?;
        match legacy_root.which()? {
            legacy_capnp::replication_response::Which::PullEvents(Ok(list)) => {
                let events = list.get_events()?;
                let mut records = Vec::with_capacity(events.len() as usize);
                for event in events.iter() {
                    records.push(decode_event_legacy(event)?);
                }
                Ok(records)
            }
            legacy_capnp::replication_response::Which::PullEvents(Err(err)) => Err(anyhow!(
                "failed to decode legacy pull_events response: {err}"
            )),
            legacy_capnp::replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            legacy_capnp::replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode legacy error response: {err}"))
            }
            legacy_capnp::replication_response::Which::ListPositions(_)
            | legacy_capnp::replication_response::Which::ApplyEvents(_) => {
                bail!("legacy remote returned unexpected response variant for pull_events");
            }
        }
    }

    fn parse_apply_events_legacy(
        &mut self,
        response: &capnp::message::Reader<OwnedSegments>,
    ) -> Result<u64> {
        self.enable_legacy_mode();

        let legacy_root = response.get_root::<legacy_capnp::replication_response::Reader>()?;
        match legacy_root.which()? {
            legacy_capnp::replication_response::Which::ApplyEvents(Ok(resp)) => {
                Ok(resp.get_applied_sequence())
            }
            legacy_capnp::replication_response::Which::ApplyEvents(Err(err)) => Err(anyhow!(
                "failed to decode legacy apply_events response: {err}"
            )),
            legacy_capnp::replication_response::Which::Error(Ok(err)) => {
                let message = read_text(err.get_message(), "error message")?;
                bail!("remote returned error: {message}");
            }
            legacy_capnp::replication_response::Which::Error(Err(err)) => {
                Err(anyhow!("failed to decode legacy error response: {err}"))
            }
            legacy_capnp::replication_response::Which::ListPositions(_)
            | legacy_capnp::replication_response::Which::PullEvents(_) => {
                bail!("legacy remote returned unexpected response variant for apply_events");
            }
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

fn decode_event_legacy(reader: legacy_capnp::event_record::Reader<'_>) -> Result<EventRecord> {
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
