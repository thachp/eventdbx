use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result, anyhow};
use capnp::message::ReaderOptions;
use capnp::serialize::{OwnedSegments, write_message_to_words};
use capnp_futures::serialize::{read_message, try_read_message};
use futures::AsyncWriteExt;
use serde_json::{self, Value};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    task::{spawn_blocking, JoinHandle},
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, error, info, warn};

use crate::{
    cli_capnp::{cli_request, cli_response},
    config::Config,
    control_capnp::{control_request, control_response},
    error::EventError,
    plugin::PluginManager,
    replication_capnp::{
        replication_hello, replication_hello_response, replication_request, replication_response,
    },
    replication_capnp_client::REPLICATION_PROTOCOL_VERSION,
    schema::{AggregateSchema, SchemaManager},
    service::{AppendEventInput, CoreContext},
    store::{AggregatePositionEntry, EventMetadata, EventRecord, EventStore},
};

#[derive(Debug, Clone)]
pub struct CliCommandResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

struct SerializedEvent {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    version: u64,
    merkle_root: String,
    hash: String,
    payload: Vec<u8>,
    metadata: Vec<u8>,
}

enum ReplicationReply {
    Positions(Vec<AggregatePositionEntry>),
    Events(Vec<SerializedEvent>),
    ApplyEvents { applied_sequence: u64 },
    PullSchemas { schemas_json: Vec<u8> },
    ApplySchemas { aggregate_count: u32 },
}

enum ControlReply {
    ListAggregates(String),
    GetAggregate { found: bool, aggregate_json: Option<String> },
    ListEvents(String),
    AppendEvent(String),
    VerifyAggregate(String),
}

enum ControlCommand {
    ListAggregates { skip: usize, take: Option<usize> },
    GetAggregate { aggregate_type: String, aggregate_id: String },
    ListEvents {
        aggregate_type: String,
        aggregate_id: String,
        skip: usize,
        take: Option<usize>,
    },
    AppendEvent {
        token: String,
        aggregate_type: String,
        aggregate_id: String,
        event_type: String,
        payload: Option<Value>,
        patch: Option<Value>,
        note: Option<String>,
    },
    VerifyAggregate {
        aggregate_type: String,
        aggregate_id: String,
    },
}

fn control_error_code(err: &EventError) -> &'static str {
    match err {
        EventError::InvalidToken => "invalid_token",
        EventError::TokenExpired => "token_expired",
        EventError::TokenLimitReached => "token_limit_reached",
        EventError::Unauthorized => "unauthorized",
        EventError::AggregateNotFound => "aggregate_not_found",
        EventError::AggregateArchived => "aggregate_archived",
        EventError::SchemaExists => "schema_exists",
        EventError::SchemaNotFound => "schema_not_found",
        EventError::InvalidSchema(_) => "invalid_schema",
        EventError::SchemaViolation(_) => "schema_violation",
        EventError::Config(_) => "config",
        EventError::Storage(_) => "storage",
        EventError::Io(_) => "io",
        EventError::Serialization(_) => "serialization",
    }
}

fn read_control_text(
    field: capnp::Result<capnp::text::Reader<'_>>,
    label: &str,
) -> std::result::Result<String, EventError> {
    let reader = field
        .map_err(|err| EventError::Serialization(format!("failed to read {label}: {err}")))?;
    reader
        .to_string()
        .map_err(|err| EventError::Serialization(format!("invalid utf-8 in {label}: {err}")))
}

pub async fn start(
    bind_addr: &str,
    config_path: Arc<PathBuf>,
    core: CoreContext,
    shared_config: Arc<RwLock<Config>>,
    local_public_key: Arc<Vec<u8>>,
) -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("failed to bind CLI Cap'n Proto listener on {bind_addr}"))?;
    let display_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| bind_addr.to_string());
    info!("CLI Cap'n Proto server listening on {}", display_addr);

    let handle = tokio::spawn({
        let core = core.clone();
        let shared_config = Arc::clone(&shared_config);
        async move {
            if let Err(err) =
                serve(listener, config_path, core, shared_config, local_public_key).await
            {
                warn!("CLI proxy server terminated: {err:?}");
            }
        }
    });
    Ok(handle)
}

async fn serve(
    listener: TcpListener,
    config_path: Arc<PathBuf>,
    core: CoreContext,
    shared_config: Arc<RwLock<Config>>,
    local_public_key: Arc<Vec<u8>>,
) -> Result<()> {
    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("failed to accept CLI proxy connection")?;
        let config_path = Arc::clone(&config_path);
        let core = core.clone();
        let shared_config = Arc::clone(&shared_config);
        let local_public_key = Arc::clone(&local_public_key);
        tokio::spawn(async move {
            if let Err(err) =
                handle_connection(stream, config_path, core, shared_config, local_public_key).await
            {
                warn!(target: "cli_proxy", peer = %peer, "CLI proxy connection error: {err:?}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    config_path: Arc<PathBuf>,
    core: CoreContext,
    shared_config: Arc<RwLock<Config>>,
    local_public_key: Arc<Vec<u8>>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = reader.compat();
    let mut writer = writer.compat_write();

    let first_message = match try_read_message(&mut reader, ReaderOptions::new()).await {
        Ok(Some(message)) => message,
        Ok(None) => return Ok(()),
        Err(err) => {
            return Err(anyhow::Error::new(err).context("failed to read initial message"));
        }
    };

    let is_cli_request = first_message
        .get_root::<cli_request::Reader>()
        .and_then(|request| request.get_args())
        .is_ok();

    if is_cli_request {
        handle_cli_loop(
            Some(first_message),
            &mut reader,
            &mut writer,
            Arc::clone(&config_path),
        )
        .await?;
        return Ok(());
    }

    if is_control_request(&first_message) {
        handle_control_session(
            Some(first_message),
            &mut reader,
            &mut writer,
            core,
            shared_config,
        )
        .await?;
        return Ok(());
    }

    handle_replication_session(
        first_message,
        &mut reader,
        &mut writer,
        core,
        local_public_key,
    )
    .await?;
    Ok(())
}

fn is_control_request(message: &capnp::message::Reader<OwnedSegments>) -> bool {
    match message.get_root::<control_request::Reader>() {
        Ok(reader) => reader.get_payload().which().is_ok(),
        Err(_) => false,
    }
}

async fn handle_cli_loop<R, W>(
    mut pending: Option<capnp::message::Reader<OwnedSegments>>,
    reader: &mut R,
    writer: &mut W,
    config_path: Arc<PathBuf>,
) -> Result<()>
where
    R: futures::AsyncRead + Unpin,
    W: futures::AsyncWrite + Unpin,
{
    loop {
        let message = if let Some(message) = pending.take() {
            message
        } else {
            match try_read_message(&mut *reader, ReaderOptions::new()).await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(err) => {
                    return Err(anyhow::Error::new(err).context("failed to read CLI request"));
                }
            }
        };

        let response_result = process_request(message, Arc::clone(&config_path)).await;

        let response_bytes = {
            let mut response_message = capnp::message::Builder::new_default();
            {
                let mut response = response_message.init_root::<cli_response::Builder>();
                match response_result {
                    Ok(result) => {
                        response.set_exit_code(result.exit_code);
                        response.set_stdout(&result.stdout);
                        response.set_stderr(&result.stderr);
                    }
                    Err(err) => {
                        response.set_exit_code(-1);
                        response.set_stdout("");
                        response.set_stderr(&err.to_string());
                    }
                }
            }
            write_message_to_words(&response_message)
        };

        writer
            .write_all(&response_bytes)
            .await
            .context("failed to write CLI response")?;
        writer
            .flush()
            .await
            .context("failed to flush CLI response")?;
    }

    Ok(())
}

async fn handle_control_session<R, W>(
    mut pending: Option<capnp::message::Reader<OwnedSegments>>,
    reader: &mut R,
    writer: &mut W,
    core: CoreContext,
    shared_config: Arc<RwLock<Config>>,
) -> Result<()>
where
    R: futures::AsyncRead + Unpin,
    W: futures::AsyncWrite + Unpin,
{
    loop {
        let message = if let Some(message) = pending.take() {
            message
        } else {
            match try_read_message(&mut *reader, ReaderOptions::new()).await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(err) => {
                    return Err(anyhow::Error::new(err).context("failed to read control request"));
                }
            }
        };

        let request = message
            .get_root::<control_request::Reader>()
            .context("failed to decode control request")?;
        let request_id = request.get_id();

        let response_result = match parse_control_command(request) {
            Ok(command) => {
                execute_control_command(command, core.clone(), Arc::clone(&shared_config)).await
            }
            Err(err) => Err(err),
        };

        let response_bytes = {
            let mut response_message = capnp::message::Builder::new_default();
            {
                let mut response = response_message.init_root::<control_response::Builder>();
                response.set_id(request_id);
                match response_result {
                    Ok(reply) => {
                        let payload = response.reborrow().init_payload();
                        match reply {
                            ControlReply::ListAggregates(json) => {
                                let mut builder = payload.init_list_aggregates();
                                builder.set_aggregates_json(&json);
                            }
                            ControlReply::GetAggregate { found, aggregate_json } => {
                                let mut builder = payload.init_get_aggregate();
                                builder.set_found(found);
                                if let Some(json) = aggregate_json {
                                    builder.set_aggregate_json(&json);
                                } else {
                                    builder.set_aggregate_json("");
                                }
                            }
                            ControlReply::ListEvents(json) => {
                                let mut builder = payload.init_list_events();
                                builder.set_events_json(&json);
                            }
                            ControlReply::AppendEvent(json) => {
                                let mut builder = payload.init_append_event();
                                builder.set_event_json(&json);
                            }
                            ControlReply::VerifyAggregate(merkle_root) => {
                                let mut builder = payload.init_verify_aggregate();
                                builder.set_merkle_root(&merkle_root);
                            }
                        }
                    }
                    Err(err) => {
                        let payload = response.reborrow().init_payload();
                        let mut builder = payload.init_error();
                        builder.set_code(control_error_code(&err));
                        builder.set_message(&err.to_string());
                    }
                }
            }
            write_message_to_words(&response_message)
        };

        writer
            .write_all(&response_bytes)
            .await
            .context("failed to write control response")?;
        writer
            .flush()
            .await
            .context("failed to flush control response")?;
    }

    Ok(())
}

fn parse_control_command(
    request: control_request::Reader<'_>,
) -> std::result::Result<ControlCommand, EventError> {
    use control_request::payload;

    match request
        .get_payload()
        .which()
        .map_err(|err| EventError::Serialization(err.to_string()))?
    {
        payload::ListAggregates(req) => {
            let req = req.map_err(|err| EventError::Serialization(err.to_string()))?;
            let skip = usize::try_from(req.get_skip())
                .map_err(|_| EventError::InvalidSchema("skip exceeds platform limits".into()))?;
            let take = if req.get_has_take() {
                Some(
                    usize::try_from(req.get_take()).map_err(|_| {
                        EventError::InvalidSchema("take exceeds platform limits".into())
                    })?,
                )
            } else {
                None
            };

            Ok(ControlCommand::ListAggregates { skip, take })
        }
        payload::GetAggregate(req) => {
            let req = req.map_err(|err| EventError::Serialization(err.to_string()))?;
            let aggregate_type = read_control_text(req.get_aggregate_type(), "aggregate_type")?;
            let aggregate_id = read_control_text(req.get_aggregate_id(), "aggregate_id")?;

            Ok(ControlCommand::GetAggregate {
                aggregate_type,
                aggregate_id,
            })
        }
        payload::ListEvents(req) => {
            let req = req.map_err(|err| EventError::Serialization(err.to_string()))?;
            let aggregate_type = read_control_text(req.get_aggregate_type(), "aggregate_type")?;
            let aggregate_id = read_control_text(req.get_aggregate_id(), "aggregate_id")?;
            let skip = usize::try_from(req.get_skip())
                .map_err(|_| EventError::InvalidSchema("skip exceeds platform limits".into()))?;
            let take = if req.get_has_take() {
                Some(
                    usize::try_from(req.get_take()).map_err(|_| {
                        EventError::InvalidSchema("take exceeds platform limits".into())
                    })?,
                )
            } else {
                None
            };

            Ok(ControlCommand::ListEvents {
                aggregate_type,
                aggregate_id,
                skip,
                take,
            })
        }
        payload::AppendEvent(req) => {
            let req = req.map_err(|err| EventError::Serialization(err.to_string()))?;
            let token = read_control_text(req.get_token(), "token")?
                .trim()
                .to_string();
            let aggregate_type = read_control_text(req.get_aggregate_type(), "aggregate_type")?;
            let aggregate_id = read_control_text(req.get_aggregate_id(), "aggregate_id")?;
            let event_type = read_control_text(req.get_event_type(), "event_type")?;

            let payload_raw = read_control_text(req.get_payload_json(), "payload_json")?;
            let payload_trimmed = payload_raw.trim();
            let payload = if payload_trimmed.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<Value>(payload_trimmed).map_err(|err| {
                        EventError::InvalidSchema(format!("invalid payload_json: {err}"))
                    })?,
                )
            };

            let patch_raw = read_control_text(req.get_patch_json(), "patch_json")?;
            let patch_trimmed = patch_raw.trim();
            let patch = if patch_trimmed.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<Value>(patch_trimmed).map_err(|err| {
                        EventError::InvalidSchema(format!("invalid patch_json: {err}"))
                    })?,
                )
            };

            let note = if req.get_has_note() {
                Some(read_control_text(req.get_note(), "note")?)
            } else {
                None
            };

            Ok(ControlCommand::AppendEvent {
                token,
                aggregate_type,
                aggregate_id,
                event_type,
                payload,
                patch,
                note,
            })
        }
        payload::VerifyAggregate(req) => {
            let req = req.map_err(|err| EventError::Serialization(err.to_string()))?;
            let aggregate_type = read_control_text(req.get_aggregate_type(), "aggregate_type")?;
            let aggregate_id = read_control_text(req.get_aggregate_id(), "aggregate_id")?;

            Ok(ControlCommand::VerifyAggregate {
                aggregate_type,
                aggregate_id,
            })
        }
    }
}

async fn execute_control_command(
    command: ControlCommand,
    core: CoreContext,
    shared_config: Arc<RwLock<Config>>,
) -> std::result::Result<ControlReply, EventError> {
    match command {
        ControlCommand::ListAggregates { skip, take } => {
            let aggregates = spawn_blocking({
                let core = core.clone();
                move || core.list_aggregates(skip, take)
            })
            .await
            .map_err(|err| EventError::Storage(format!("list aggregates task failed: {err}")))?;
            let json = serde_json::to_string(&aggregates)?;
            Ok(ControlReply::ListAggregates(json))
        }
        ControlCommand::GetAggregate {
            aggregate_type,
            aggregate_id,
        } => {
            let aggregate = spawn_blocking({
                let core = core.clone();
                let aggregate_type = aggregate_type.clone();
                let aggregate_id = aggregate_id.clone();
              move || core.get_aggregate(&aggregate_type, &aggregate_id)
            })
            .await
            .map_err(|err| EventError::Storage(format!("get aggregate task failed: {err}")))??;

            let json = aggregate
                .as_ref()
                .map(|aggregate| serde_json::to_string(aggregate))
                .transpose()?;

            Ok(ControlReply::GetAggregate {
                found: aggregate.is_some(),
                aggregate_json: json,
            })
        }
        ControlCommand::ListEvents {
            aggregate_type,
            aggregate_id,
            skip,
            take,
        } => {
            let events = spawn_blocking({
                let core = core.clone();
                let aggregate_type = aggregate_type.clone();
                let aggregate_id = aggregate_id.clone();
                move || core.list_events(&aggregate_type, &aggregate_id, skip, take)
            })
            .await
            .map_err(|err| EventError::Storage(format!("list events task failed: {err}")))??;

            let json = serde_json::to_string(&events)?;
            Ok(ControlReply::ListEvents(json))
        }
        ControlCommand::AppendEvent {
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            patch,
            note,
        } => {
            let record = spawn_blocking({
                let core = core.clone();
                let input = AppendEventInput {
                    token,
                    aggregate_type: aggregate_type.clone(),
                    aggregate_id: aggregate_id.clone(),
                    event_type: event_type.clone(),
                    payload,
                    patch,
                    note,
                };
                move || core.append_event(input)
            })
            .await
            .map_err(|err| EventError::Storage(format!("append event task failed: {err}")))??;

            let record_json = serde_json::to_string(&record)?;

            let schemas = core.schemas();
            if schemas.should_snapshot(&record.aggregate_type, record.version) {
                let aggregate_type = record.aggregate_type.clone();
                let aggregate_id = record.aggregate_id.clone();
                let version = record.version;
                let snapshot_result = spawn_blocking({
                    let store = core.store();
                    let aggregate_type_key = aggregate_type.clone();
                    let aggregate_id_key = aggregate_id.clone();
                    move || {
                        store.create_snapshot(
                            &aggregate_type_key,
                            &aggregate_id_key,
                            Some(format!("auto snapshot v{}", version)),
                        )
                    }
                })
                .await
                .map_err(|err| {
                    EventError::Storage(format!("failed to create auto snapshot: {err}"))
                })?;
                if let Err(err) = snapshot_result {
                    warn!(
                        target: "server",
                        "failed to create auto snapshot for {}::{} v{}: {}",
                        aggregate_type,
                        aggregate_id,
                        version,
                        err
                    );
                }
            }

            let plugins = {
                let guard = shared_config
                    .read()
                    .map_err(|_| EventError::Storage("failed to acquire config lock".into()))?;
                PluginManager::from_config(&*guard)?
            };

            if !plugins.is_empty() {
                let schemas = core.schemas();
                let schema = schemas.get(&record.aggregate_type).ok();
                let aggregate_type = record.aggregate_type.clone();
                let aggregate_id = record.aggregate_id.clone();
                let state_result = spawn_blocking({
                    let store = core.store();
                    let aggregate_type_key = aggregate_type.clone();
                    let aggregate_id_key = aggregate_id.clone();
                    move || store.get_aggregate_state(&aggregate_type_key, &aggregate_id_key)
                })
                .await
                .map_err(|err| {
                    EventError::Storage(format!("failed to load aggregate state: {err}"))
                })?;

                match state_result {
                    Ok(current_state) => {
                        if let Err(err) =
                            plugins.notify_event(&record, &current_state, schema.as_ref())
                        {
                            error!("plugin notification failed: {}", err);
                        }
                    }
                    Err(err) => {
                        warn!(
                            "plugin notification skipped (failed to load state for {}::{}): {}",
                            aggregate_type, aggregate_id, err
                        );
                    }
                }
            }

            Ok(ControlReply::AppendEvent(record_json))
        }
        ControlCommand::VerifyAggregate {
            aggregate_type,
            aggregate_id,
        } => {
            let merkle = spawn_blocking({
                let core = core.clone();
                let aggregate_type = aggregate_type.clone();
                let aggregate_id = aggregate_id.clone();
                move || core.verify_aggregate(&aggregate_type, &aggregate_id)
            })
            .await
            .map_err(|err| EventError::Storage(format!("verify aggregate task failed: {err}")))??;

            Ok(ControlReply::VerifyAggregate(merkle))
        }
    }
}

async fn handle_replication_session<R, W>(
    first_message: capnp::message::Reader<OwnedSegments>,
    reader: &mut R,
    writer: &mut W,
    core: CoreContext,
    local_public_key: Arc<Vec<u8>>,
) -> Result<()>
where
    R: futures::AsyncRead + Unpin,
    W: futures::AsyncWrite + Unpin,
{
    let hello = first_message
        .get_root::<replication_hello::Reader>()
        .context("failed to decode replication hello")?;
    let protocol_version = hello.get_protocol_version();
    let expected_key = hello
        .get_expected_public_key()
        .map_err(|err| anyhow!("failed to read handshake public key: {err}"))?
        .to_vec();

    let (accepted, response_text) = if protocol_version != REPLICATION_PROTOCOL_VERSION {
        (
            false,
            format!(
                "unsupported replication protocol version {}",
                protocol_version
            ),
        )
    } else if expected_key.is_empty() {
        (
            false,
            "missing expected public key in replication handshake".to_string(),
        )
    } else if expected_key != *local_public_key {
        warn!("replication handshake rejected due to pinned key mismatch");
        (false, "pinned public key mismatch".to_string())
    } else {
        (true, "ok".to_string())
    };

    let handshake_bytes = {
        let mut response_message = capnp::message::Builder::new_default();
        {
            let mut response = response_message.init_root::<replication_hello_response::Builder>();
            response.set_accepted(accepted);
            response.set_message(&response_text);
        }
        write_message_to_words(&response_message)
    };

    writer
        .write_all(&handshake_bytes)
        .await
        .context("failed to write replication hello response")?;
    writer
        .flush()
        .await
        .context("failed to flush replication hello response")?;

    if !accepted {
        return Ok(());
    }

    let store = core.store();
    let schemas = core.schemas();
    let store = Arc::clone(&store);
    let schemas = Arc::clone(&schemas);
    let mut last_sequence = 0u64;

    loop {
        let message = match try_read_message(&mut *reader, ReaderOptions::new()).await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(err) => {
                return Err(anyhow::Error::new(err).context("failed to read replication request"));
            }
        };

        let response_bytes = {
            let mut response_message = capnp::message::Builder::new_default();
            let mut response_root = response_message.init_root::<replication_response::Builder>();

            let result = message
                .get_root::<replication_request::Reader>()
                .map_err(|err| anyhow!("failed to decode replication request: {err}"))
                .and_then(|request| {
                    process_replication_request(request, &store, &schemas, &mut last_sequence)
                })
                .and_then(|reply| populate_replication_response(&mut response_root, reply));

            if let Err(err) = result {
                let mut error = response_root.init_error();
                error.set_message(&err.to_string());
            }

            write_message_to_words(&response_message)
        };
        writer
            .write_all(&response_bytes)
            .await
            .context("failed to write replication response")?;
        writer
            .flush()
            .await
            .context("failed to flush replication response")?;
    }

    Ok(())
}

fn process_replication_request(
    request: replication_request::Reader<'_>,
    store: &EventStore,
    schemas: &SchemaManager,
    last_sequence: &mut u64,
) -> Result<ReplicationReply> {
    use replication_request::Which;

    match request.which()? {
        Which::ListPositions(()) => {
            let positions = store.aggregate_positions()?;
            Ok(ReplicationReply::Positions(positions))
        }
        Which::PullEvents(req) => {
            let req = req.map_err(|err| anyhow!("failed to read pullEvents request: {err}"))?;
            let aggregate_type = read_text_field(req.get_aggregate_type(), "aggregate type")?;
            let aggregate_id = read_text_field(req.get_aggregate_id(), "aggregate id")?;
            let from_version = req.get_from_version();
            let limit = req.get_limit();
            let limit = if limit == 0 {
                None
            } else {
                Some(limit as usize)
            };

            let events = store.events_after(&aggregate_type, &aggregate_id, from_version, limit)?;
            let mut serialized = Vec::with_capacity(events.len());
            for event in events {
                let payload = serde_json::to_vec(&event.payload)
                    .map_err(|err| anyhow!("failed to encode event payload: {err}"))?;
                let metadata = serde_json::to_vec(&event.metadata)
                    .map_err(|err| anyhow!("failed to encode event metadata: {err}"))?;
                serialized.push(SerializedEvent {
                    aggregate_type: event.aggregate_type,
                    aggregate_id: event.aggregate_id,
                    event_type: event.event_type,
                    version: event.version,
                    merkle_root: event.merkle_root,
                    hash: event.hash,
                    payload,
                    metadata,
                });
            }

            Ok(ReplicationReply::Events(serialized))
        }
        Which::ApplyEvents(req) => {
            let req = req.map_err(|err| anyhow!("failed to read applyEvents request: {err}"))?;
            let sequence = req.get_sequence();
            let events = req
                .get_events()
                .map_err(|err| anyhow!("failed to access applyEvents list: {err}"))?;

            for event_reader in events.iter() {
                let record = decode_capnp_event(event_reader)?;
                store.append_replica(record)?;
            }

            *last_sequence = sequence;
            Ok(ReplicationReply::ApplyEvents {
                applied_sequence: *last_sequence,
            })
        }
        Which::PullSchemas(()) => {
            let snapshot = schemas.snapshot();
            let payload = serde_json::to_vec(&snapshot)
                .map_err(|err| anyhow!("failed to encode schema snapshot: {err}"))?;
            Ok(ReplicationReply::PullSchemas {
                schemas_json: payload,
            })
        }
        Which::ApplySchemas(req) => {
            let req = req.map_err(|err| anyhow!("failed to read applySchemas request: {err}"))?;
            let data = req
                .get_schemas_json()
                .map_err(|err| anyhow!("failed to read applySchemas payload: {err}"))?;
            let map: BTreeMap<String, AggregateSchema> = if data.is_empty() {
                BTreeMap::new()
            } else {
                serde_json::from_slice(data)
                    .map_err(|err| anyhow!("failed to decode schema payload: {err}"))?
            };

            let aggregate_count = map.len() as u32;
            schemas
                .replace_all(map)
                .map_err(|err| anyhow!("failed to apply schema updates: {err}"))?;
            Ok(ReplicationReply::ApplySchemas { aggregate_count })
        }
    }
}

fn populate_replication_response(
    response: &mut replication_response::Builder<'_>,
    payload: ReplicationReply,
) -> Result<()> {
    match payload {
        ReplicationReply::Positions(positions) => {
            let mut list = response.reborrow().init_list_positions();
            let mut builder = list.reborrow().init_positions(positions.len() as u32);
            for (idx, entry) in positions.into_iter().enumerate() {
                let mut position = builder.reborrow().get(idx as u32);
                position.set_aggregate_type(&entry.aggregate_type);
                position.set_aggregate_id(&entry.aggregate_id);
                position.set_version(entry.version);
            }
        }
        ReplicationReply::Events(events) => {
            let mut pull = response.reborrow().init_pull_events();
            let mut builder = pull.reborrow().init_events(events.len() as u32);
            for (idx, event) in events.into_iter().enumerate() {
                let mut record = builder.reborrow().get(idx as u32);
                record.set_aggregate_type(&event.aggregate_type);
                record.set_aggregate_id(&event.aggregate_id);
                record.set_event_type(&event.event_type);
                record.set_version(event.version);
                record.set_merkle_root(&event.merkle_root);
                record.set_hash(&event.hash);
                record.set_payload(&event.payload);
                record.set_metadata(&event.metadata);
            }
        }
        ReplicationReply::ApplyEvents { applied_sequence } => {
            let mut apply = response.reborrow().init_apply_events();
            apply.set_applied_sequence(applied_sequence);
        }
        ReplicationReply::PullSchemas { schemas_json } => {
            let mut pull = response.reborrow().init_pull_schemas();
            pull.set_schemas_json(&schemas_json);
        }
        ReplicationReply::ApplySchemas { aggregate_count } => {
            let mut apply = response.reborrow().init_apply_schemas();
            apply.set_aggregate_count(aggregate_count);
        }
    }
    Ok(())
}

fn decode_capnp_event(
    reader: crate::replication_capnp::event_record::Reader<'_>,
) -> Result<EventRecord> {
    let aggregate_type = read_text_field(reader.get_aggregate_type(), "event aggregate type")?;
    let aggregate_id = read_text_field(reader.get_aggregate_id(), "event aggregate id")?;
    let event_type = read_text_field(reader.get_event_type(), "event type")?;
    let version = reader.get_version();
    let merkle_root = read_text_field(reader.get_merkle_root(), "event merkle root")?;
    let hash = read_text_field(reader.get_hash(), "event hash")?;
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

fn read_text_field(value: capnp::Result<capnp::text::Reader<'_>>, field: &str) -> Result<String> {
    let reader = value.map_err(|err| anyhow!("failed to read {field}: {err}"))?;
    reader
        .to_str()
        .map_err(|err| anyhow!("invalid UTF-8 in {field}: {err}"))
        .map(|s| s.to_string())
}

async fn process_request(
    message: capnp::message::Reader<OwnedSegments>,
    config_path: Arc<PathBuf>,
) -> Result<CliCommandResult> {
    let request = message
        .get_root::<cli_request::Reader>()
        .context("failed to decode CLI request")?;
    let args = {
        let args_reader = request
            .get_args()
            .context("failed to read CLI request arguments")?;

        let mut collected = Vec::with_capacity(args_reader.len() as usize);
        for arg in args_reader.iter() {
            let value = arg.context("failed to read CLI argument")?;
            collected.push(value.to_string()?);
        }
        collected
    };

    execute_cli_command(args, &config_path).await
}

async fn execute_cli_command(args: Vec<String>, config_path: &PathBuf) -> Result<CliCommandResult> {
    let exe = resolve_cli_executable().context("failed to resolve CLI executable")?;

    let final_args = augment_args_with_config(args, config_path);

    let mut command = Command::new(exe);
    command.args(&final_args);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let output = command
        .output()
        .await
        .context("failed to execute CLI command")?;

    let exit_code = output
        .status
        .code()
        .unwrap_or_else(|| if output.status.success() { 0 } else { -1 });

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    debug!(
        target: "cli_proxy",
        args = ?final_args,
        exit_code,
        "CLI command executed"
    );

    Ok(CliCommandResult {
        exit_code,
        stdout,
        stderr,
    })
}

fn resolve_cli_executable() -> Result<PathBuf> {
    if let Ok(path) = env::var("EVENTDBX_CLI") {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = env::var("CARGO_BIN_EXE_eventdbx") {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = env::var("CARGO_BIN_EXE_dbx") {
        return Ok(PathBuf::from(path));
    }
    let current = std::env::current_exe().context("failed to resolve current executable")?;
    if let Some(dir) = current.parent() {
        if let Some(candidate) = probe_dir_for_cli(dir) {
            return Ok(candidate);
        }
        if let Some(parent) = dir.parent() {
            if let Some(candidate) = probe_dir_for_cli(parent) {
                return Ok(candidate);
            }
        }
    }
    Ok(current)
}

fn probe_dir_for_cli(dir: &Path) -> Option<PathBuf> {
    let candidates = ["eventdbx", "dbx"];
    for candidate in candidates {
        let unix_path = dir.join(candidate);
        if unix_path.exists() {
            return Some(unix_path);
        }
    }
    #[cfg(windows)]
    {
        let candidates = ["eventdbx.exe", "dbx.exe"];
        for candidate in candidates {
            let windows_path = dir.join(candidate);
            if windows_path.exists() {
                return Some(windows_path);
            }
        }
    }
    None
}

fn augment_args_with_config(mut args: Vec<String>, config_path: &PathBuf) -> Vec<String> {
    if has_config_arg(&args) {
        return args;
    }

    let mut final_args = Vec::with_capacity(args.len() + 2);
    final_args.push("--config".to_string());
    final_args.push(config_path.to_string_lossy().into_owned());
    final_args.extend(args.drain(..));
    final_args
}

fn has_config_arg(args: &[String]) -> bool {
    args.iter()
        .any(|arg| arg == "--config" || arg.starts_with("--config="))
}

pub async fn invoke(args: &[String], addr: &str) -> Result<CliCommandResult> {
    let stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("failed to connect to CLI proxy at {addr}"))?;
    let (reader, writer) = stream.into_split();
    let mut writer = writer.compat_write();
    let message_bytes = {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut request = message.init_root::<cli_request::Builder>();
            let mut list = request.init_args(args.len() as u32);
            for (idx, arg) in args.iter().enumerate() {
                list.set(idx as u32, arg);
            }
        }
        write_message_to_words(&message)
    };

    writer
        .write_all(&message_bytes)
        .await
        .context("failed to send CLI request")?;
    writer
        .flush()
        .await
        .context("failed to flush CLI request")?;

    let mut reader = reader.compat();
    let response_message = read_message(&mut reader, ReaderOptions::new())
        .await
        .context("failed to read CLI response")?;
    let response = response_message
        .get_root::<cli_response::Reader>()
        .context("failed to decode CLI response")?;

    let stdout = response
        .get_stdout()
        .context("missing stdout field in CLI response")?
        .to_string()
        .map_err(|err| anyhow::Error::new(err).context("invalid UTF-8 in CLI stdout"))?;
    let stderr = response
        .get_stderr()
        .context("missing stderr field in CLI response")?
        .to_string()
        .map_err(|err| anyhow::Error::new(err).context("invalid UTF-8 in CLI stderr"))?;

    Ok(CliCommandResult {
        exit_code: response.get_exit_code(),
        stdout,
        stderr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn probe_dir_prefers_eventdbx() {
        let dir = TempDir::new().unwrap();
        let eventdbx = dir.path().join("eventdbx");
        let dbx = dir.path().join("dbx");
        std::fs::write(&eventdbx, b"").unwrap();
        std::fs::write(&dbx, b"").unwrap();

        let resolved = probe_dir_for_cli(dir.path()).expect("expected CLI path");
        assert_eq!(resolved, eventdbx);
    }

    #[test]
    fn probe_dir_falls_back_to_dbx() {
        let dir = TempDir::new().unwrap();
        let dbx = dir.path().join("dbx");
        std::fs::write(&dbx, b"").unwrap();

        let resolved = probe_dir_for_cli(dir.path()).expect("expected CLI path");
        assert_eq!(resolved, dbx);
    }
}
