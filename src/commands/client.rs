use std::{
    io::Write,
    net::{IpAddr, SocketAddr, TcpStream},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use capnp::{
    message::Builder, message::ReaderOptions, serialize, serialize::write_message_to_words,
};
use eventdbx::replication_noise::{
    FrameTransport, perform_client_handshake_blocking, read_session_frame_blocking,
    write_session_frame_blocking,
};
use eventdbx::{
    config::Config,
    control_capnp::{control_hello, control_hello_response, control_request, control_response},
    store::{AggregateState, EventRecord},
};
use serde_json::{self, Value};

const CONTROL_PROTOCOL_VERSION: u16 = 1;

#[derive(Clone)]
pub struct ServerClient {
    connect_addr: String,
    tenant: Option<String>,
    no_noise: bool,
}

#[derive(Clone)]
struct ControlEndpoint {
    connect_addr: String,
    tenant: Option<String>,
    no_noise: bool,
}

impl ServerClient {
    pub fn new(config: &Config) -> Result<Self> {
        let connect_addr = normalize_connect_addr(&config.socket.bind_addr);
        let no_noise = config.no_noise;

        Ok(Self {
            connect_addr,
            tenant: Some(config.active_domain().to_string()),
            no_noise,
        })
    }

    pub fn with_tenant(mut self, tenant: Option<String>) -> Self {
        self.tenant = tenant;
        self
    }

    fn endpoint(&self) -> ControlEndpoint {
        ControlEndpoint {
            connect_addr: self.connect_addr.clone(),
            tenant: self.tenant.clone(),
            no_noise: self.no_noise,
        }
    }

    fn send_control_request<Build, Handle, T>(
        &self,
        token: &str,
        request_id: u64,
        build: Build,
        handle: Handle,
    ) -> Result<T>
    where
        Build: FnOnce(&mut control_request::Builder<'_>) -> Result<()>,
        Handle: FnOnce(control_response::Reader<'_>) -> Result<T>,
    {
        let endpoint = self.endpoint();
        send_control_request_blocking(&endpoint, token, request_id, build, handle)
    }

    pub fn create_aggregate(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: &Value,
        metadata: Option<&Value>,
        note: Option<&str>,
    ) -> Result<Option<AggregateState>> {
        let endpoint = self.endpoint();
        let token = token.to_string();
        let aggregate_type = aggregate_type.to_string();
        let aggregate_id = aggregate_id.to_string();
        let event_type = event_type.to_string();
        let payload = payload.clone();
        let metadata = metadata.cloned();
        let note = note.map(|value| value.to_string());

        if tokio::runtime::Handle::try_current().is_ok() {
            return tokio::task::block_in_place(move || {
                create_aggregate_blocking(
                    endpoint.clone(),
                    token,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload,
                    metadata.clone(),
                    note.clone(),
                )
            });
        }

        create_aggregate_blocking(
            endpoint,
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            metadata,
            note,
        )
    }

    pub fn append_event(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: Option<&Value>,
        metadata: Option<&Value>,
        note: Option<&str>,
    ) -> Result<Option<EventRecord>> {
        let endpoint = self.endpoint();
        let token = token.to_string();
        let aggregate_type = aggregate_type.to_string();
        let aggregate_id = aggregate_id.to_string();
        let event_type = event_type.to_string();
        let payload = payload.cloned();
        let metadata = metadata.cloned();
        let note = note.map(|value| value.to_string());

        if tokio::runtime::Handle::try_current().is_ok() {
            return tokio::task::block_in_place(move || {
                append_event_blocking(
                    endpoint.clone(),
                    token,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload,
                    metadata.clone(),
                    note.clone(),
                )
            });
        }

        append_event_blocking(
            endpoint,
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            metadata,
            note,
        )
    }

    pub fn reload_tenant(&self, token: &str, tenant: &str) -> Result<bool> {
        let request_id = next_request_id();
        self.send_control_request(
            token,
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut reload = payload_builder.init_tenant_reload();
                reload.set_token(token);
                reload.set_tenant_id(tenant);
                Ok(())
            },
            |response| {
                use control_response::payload;
                match response
                    .get_payload()
                    .which()
                    .context("failed to decode tenant_reload response payload")?
                {
                    payload::TenantReload(Ok(reply)) => Ok(reply.get_reloaded()),
                    payload::TenantReload(Err(err)) => {
                        Err(anyhow!("failed to decode tenant_reload response: {}", err))
                    }
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
            },
        )
    }
}

fn create_aggregate_blocking(
    endpoint: ControlEndpoint,
    token: String,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Value,
    metadata: Option<Value>,
    note: Option<String>,
) -> Result<Option<AggregateState>> {
    let request_id = next_request_id();

    let payload_json = serde_json::to_string(&payload)
        .context("failed to serialize payload for proxy create request")?;
    let (metadata_json, has_metadata) = match metadata {
        Some(value) => (
            serde_json::to_string(&value)
                .context("failed to serialize metadata for proxy create request")?,
            true,
        ),
        None => (String::new(), false),
    };
    let (note_text, has_note) = match note {
        Some(value) => (value, true),
        None => (String::new(), false),
    };
    send_control_request_blocking(
        &endpoint,
        &token,
        request_id,
        |request| {
            let payload_builder = request.reborrow().init_payload();
            let mut create = payload_builder.init_create_aggregate();
            create.set_token(&token);
            create.set_aggregate_type(&aggregate_type);
            create.set_aggregate_id(&aggregate_id);
            create.set_event_type(&event_type);
            create.set_payload_json(&payload_json);
            create.set_metadata_json(&metadata_json);
            create.set_has_metadata(has_metadata);
            create.set_note(&note_text);
            create.set_has_note(has_note);
            Ok(())
        },
        |response| {
            use control_response::payload;

            match response
                .get_payload()
                .which()
                .context("failed to decode create response payload")?
            {
                payload::CreateAggregate(Ok(create)) => {
                    let aggregate_json = read_text(create.get_aggregate_json(), "aggregate_json")?;
                    if aggregate_json.trim().is_empty() {
                        Ok(None)
                    } else {
                        let state: AggregateState = serde_json::from_str(&aggregate_json)
                            .context("failed to parse create response payload")?;
                        Ok(Some(state))
                    }
                }
                payload::CreateAggregate(Err(err)) => Err(anyhow!(
                    "failed to decode create_aggregate payload from CLI proxy: {}",
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
        },
    )
}

fn append_event_blocking(
    endpoint: ControlEndpoint,
    token: String,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Option<Value>,
    metadata: Option<Value>,
    note: Option<String>,
) -> Result<Option<EventRecord>> {
    let request_id = next_request_id();

    let payload_json = payload
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .context("failed to serialize payload for proxy request")?
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
    send_control_request_blocking(
        &endpoint,
        &token,
        request_id,
        |request| {
            let payload_builder = request.reborrow().init_payload();
            let mut append = payload_builder.init_append_event();
            append.set_token(&token);
            append.set_aggregate_type(&aggregate_type);
            append.set_aggregate_id(&aggregate_id);
            append.set_event_type(&event_type);
            append.set_payload_json(&payload_json);
            append.set_metadata_json(&metadata_json);
            append.set_has_metadata(has_metadata);
            append.set_note(&note_text);
            append.set_has_note(has_note);
            Ok(())
        },
        |response| {
            use control_response::payload;

            match response
                .get_payload()
                .which()
                .context("failed to decode append response payload")?
            {
                payload::AppendEvent(Ok(append)) => {
                    let event_json = read_text(append.get_event_json(), "event_json")?;
                    if event_json.trim().is_empty() {
                        Ok(None)
                    } else {
                        let record: EventRecord = serde_json::from_str(&event_json)
                            .context("failed to parse append response payload")?;
                        Ok(Some(record))
                    }
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
        },
    )
}

fn send_control_request_blocking<Build, Handle, T>(
    endpoint: &ControlEndpoint,
    handshake_token: &str,
    request_id: u64,
    build: Build,
    handle: Handle,
) -> Result<T>
where
    Build: FnOnce(&mut control_request::Builder<'_>) -> Result<()>,
    Handle: FnOnce(control_response::Reader<'_>) -> Result<T>,
{
    let mut stream = TcpStream::connect(&endpoint.connect_addr).with_context(|| {
        format!(
            "failed to connect to CLI proxy at {}",
            endpoint.connect_addr
        )
    })?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("failed to configure proxy write timeout")?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("failed to configure proxy read timeout")?;

    let mut hello_message = Builder::new_default();
    {
        let mut hello = hello_message.init_root::<control_hello::Builder>();
        hello.set_protocol_version(CONTROL_PROTOCOL_VERSION);
        hello.set_token(handshake_token);
        hello.set_no_noise(endpoint.no_noise);
        if let Some(tenant) = endpoint.tenant.as_deref() {
            hello.set_tenant_id(tenant);
        } else {
            hello.set_tenant_id("");
        }
    }
    serialize::write_message(&mut stream, &hello_message)
        .context("failed to send control hello")?;
    stream.flush().context("failed to flush control hello")?;

    let response_message = serialize::read_message(&mut stream, ReaderOptions::new())
        .context("failed to read control hello response")?;
    let response = response_message
        .get_root::<control_hello_response::Reader>()
        .context("failed to decode control hello response")?;
    let response_no_noise = response.get_no_noise();
    if !response.get_accepted() {
        let reason = read_text(response.get_message(), "control handshake message")?;
        bail!("control handshake rejected: {}", reason);
    }

    if endpoint.no_noise && !response_no_noise {
        bail!(
            "--no-noise requested but the server refused the plaintext control channel; enable \
             plaintext in server config or omit the flag"
        );
    }

    let mut transport = if response_no_noise {
        FrameTransport::plain()
    } else {
        FrameTransport::from(
            perform_client_handshake_blocking(&mut stream, handshake_token.as_bytes())
                .context("failed to establish Noise control channel")?,
        )
    };

    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<control_request::Builder>();
        request.set_id(request_id);
        build(&mut request)?;
    }
    let request_bytes = write_message_to_words(&message);
    write_session_frame_blocking(&mut stream, &mut transport, &request_bytes)
        .context("failed to send control request")?;

    let response_bytes = read_session_frame_blocking(&mut stream, &mut transport)?
        .ok_or_else(|| anyhow!("CLI proxy closed control channel before response"))?;
    let mut cursor = std::io::Cursor::new(&response_bytes);
    let response_message = capnp::serialize::read_message(&mut cursor, ReaderOptions::new())
        .context("failed to decode control response")?;
    let response = response_message
        .get_root::<control_response::Reader>()
        .context("failed to decode control response")?;

    if response.get_id() != request_id {
        bail!(
            "CLI proxy returned response id {} but expected {}",
            response.get_id(),
            request_id
        );
    }

    handle(response)
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
