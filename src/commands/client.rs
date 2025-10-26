use std::{
    io::{BufReader, Write},
    net::{IpAddr, SocketAddr, TcpStream},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use capnp::{message::ReaderOptions, serialize};
use eventdbx::{
    config::Config,
    control_capnp::{control_request, control_response},
    store::EventRecord,
};
use serde_json::Value;

#[derive(Clone)]
pub struct ServerClient {
    connect_addr: String,
}

impl ServerClient {
    pub fn new(config: &Config) -> Result<Self> {
        let connect_addr = normalize_connect_addr(&config.socket.bind_addr);

        Ok(Self { connect_addr })
    }

    pub fn append_event(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: Option<&Value>,
        patch: Option<&Value>,
        metadata: Option<&Value>,
        note: Option<&str>,
    ) -> Result<EventRecord> {
        let connect_addr = self.connect_addr.clone();
        let token = token.to_string();
        let aggregate_type = aggregate_type.to_string();
        let aggregate_id = aggregate_id.to_string();
        let event_type = event_type.to_string();
        let payload = payload.cloned();
        let patch = patch.cloned();
        let metadata = metadata.cloned();
        let note = note.map(|value| value.to_string());

        if tokio::runtime::Handle::try_current().is_ok() {
            return tokio::task::block_in_place(move || {
                append_event_blocking(
                    connect_addr,
                    token,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload,
                    patch.clone(),
                    metadata.clone(),
                    note.clone(),
                )
            });
        }

        append_event_blocking(
            connect_addr,
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            patch,
            metadata,
            note,
        )
    }
}

fn append_event_blocking(
    connect_addr: String,
    token: String,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Option<Value>,
    patch: Option<Value>,
    metadata: Option<Value>,
    note: Option<String>,
) -> Result<EventRecord> {
    let mut stream = TcpStream::connect(&connect_addr)
        .with_context(|| format!("failed to connect to CLI proxy at {}", connect_addr))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("failed to configure proxy write timeout")?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("failed to configure proxy read timeout")?;

    let request_id = next_request_id();

    let payload_json = payload
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .context("failed to serialize payload for proxy request")?
        .unwrap_or_default();
    let patch_json = patch
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .context("failed to serialize patch for proxy request")?
        .unwrap_or_default();
    let (metadata_json, has_metadata) = match metadata {
        Some(value) => (
            serde_json::to_string(&value)
                .context("failed to serialize metadata for proxy request")?,
            true,
        ),
        None => (String::new(), false),
    };
    let (note_text, has_note) = match note {
        Some(value) => (value, true),
        None => (String::new(), false),
    };

    let mut message = capnp::message::Builder::new_default();
    {
        let mut request = message.init_root::<control_request::Builder>();
        request.set_id(request_id);
        let payload_builder = request.reborrow().init_payload();
        let mut append = payload_builder.init_append_event();
        append.set_token(&token);
        append.set_aggregate_type(&aggregate_type);
        append.set_aggregate_id(&aggregate_id);
        append.set_event_type(&event_type);
        append.set_payload_json(&payload_json);
        append.set_patch_json(&patch_json);
        append.set_metadata_json(&metadata_json);
        append.set_has_metadata(has_metadata);
        append.set_note(&note_text);
        append.set_has_note(has_note);
    }

    serialize::write_message(&mut stream, &message)
        .context("failed to send append request via CLI proxy")?;
    stream
        .flush()
        .context("failed to flush append request via CLI proxy")?;

    let mut reader = BufReader::new(stream);
    let response_message = serialize::read_message(&mut reader, ReaderOptions::new())
        .context("failed to read append response from CLI proxy")?;
    let response = response_message
        .get_root::<control_response::Reader>()
        .context("failed to decode append response from CLI proxy")?;

    if response.get_id() != request_id {
        bail!(
            "CLI proxy returned response id {} but expected {}",
            response.get_id(),
            request_id
        );
    }

    use control_response::payload;

    match response
        .get_payload()
        .which()
        .context("failed to decode append response payload")?
    {
        payload::AppendEvent(Ok(append)) => {
            let event_json = read_text(append.get_event_json(), "event_json")?;
            let record: EventRecord = serde_json::from_str(&event_json)
                .context("failed to parse append response payload")?;
            Ok(record)
        }
        payload::AppendEvent(Err(err)) => Err(anyhow!(
            "failed to decode append_event payload from CLI proxy: {}",
            err
        )),
        payload::Error(Ok(error)) => {
            let code = read_text(error.get_code(), "code")?;
            let message = read_text(error.get_message(), "message")?;
            Err(anyhow!("server returned {}: {}", code, message))
        }
        payload::Error(Err(err)) => Err(anyhow!(
            "failed to decode error payload from CLI proxy: {}",
            err
        )),
        _ => Err(anyhow!(
            "unexpected payload returned from CLI proxy response"
        )),
    }
}

fn read_text(field: capnp::Result<capnp::text::Reader<'_>>, label: &str) -> Result<String> {
    let reader = field.with_context(|| format!("missing {label} in CLI proxy response"))?;
    reader
        .to_str()
        .map(|value| value.to_string())
        .map_err(|err| anyhow!("invalid utf-8 in {}: {}", label, err))
}

fn normalize_connect_addr(bind_addr: &str) -> String {
    if let Ok(addr) = bind_addr.parse::<SocketAddr>() {
        match addr.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => format!("127.0.0.1:{}", addr.port()),
            IpAddr::V6(ip) if ip.is_unspecified() => format!("[::1]:{}", addr.port()),
            _ => addr.to_string(),
        }
    } else {
        bind_addr.to_string()
    }
}

fn next_request_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}
