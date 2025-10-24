use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, anyhow};
use capnp::{message::Builder, message::ReaderOptions, serialize::write_message_to_words};
use capnp_futures::serialize::read_message;
use eventdbx::{
    cli_proxy::test_support::spawn_control_session,
    config::Config,
    control_capnp::{control_request, control_response},
    schema::SchemaManager,
    service::CoreContext,
    store::EventStore,
    token::{IssueTokenInput, JwtLimits, TokenManager},
};
use futures::AsyncWriteExt;
use serde_json::Value;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn control_capnp_regression_flows() -> Result<()> {
    let tempdir = tempdir().context("failed to create temp dir")?;
    let mut config = Config::default();
    config.data_dir = tempdir.path().to_path_buf();
    config.data_encryption_key = None;

    let tokens = Arc::new(TokenManager::load(
        config.jwt_manager_config()?,
        config.tokens_path(),
        config.jwt_revocations_path(),
        None,
    )?);
    let schemas = Arc::new(SchemaManager::load(config.schema_store_path())?);
    let store = Arc::new(EventStore::open(config.event_store_path(), None)?);

    let core = CoreContext::new(
        Arc::clone(&tokens),
        Arc::clone(&schemas),
        Arc::clone(&store),
        config.restrict,
        config.list_page_size,
        config.page_limit,
    );
    let shared_config = Arc::new(RwLock::new(config.clone()));

    let (mut writer, mut reader, server_task) =
        spawn_control_session(core.clone(), Arc::clone(&shared_config));

    let mut next_request_id: u64 = 1;

    let aggregates: Vec<Value> = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut list = payload.init_list_aggregates();
            list.set_skip(0);
            list.set_take(0);
            list.set_has_take(false);
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::ListAggregates(Ok(list)) => {
                let json = list
                    .get_aggregates_json()
                    .context("missing aggregates_json")?
                    .to_str()
                    .context("aggregates_json utf-8 error")?;
                let parsed: Vec<Value> =
                    serde_json::from_str(json).context("failed to parse aggregates_json")?;
                Ok(parsed)
            }
            control_response::payload::ListAggregates(Err(err)) => {
                Err(anyhow!("failed to decode list_aggregates response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert!(aggregates.is_empty());
    next_request_id += 1;

    let token_record = tokens.issue(IssueTokenInput {
        subject: "system:admin".into(),
        group: "system".into(),
        user: "admin".into(),
        root: true,
        actions: Vec::new(),
        resources: Vec::new(),
        ttl_secs: Some(3_600),
        not_before: None,
        issued_by: "control-test".into(),
        limits: JwtLimits::default(),
    })?;

    let event_record: Value = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut append = payload.init_append_event();
            append.set_token(&token_record.token);
            append.set_aggregate_type("order");
            append.set_aggregate_id("order-1");
            append.set_event_type("OrderCreated");
            append.set_payload_json(r#"{"status":"created"}"#);
            append.set_patch_json("");
            append.set_note("");
            append.set_has_note(false);
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::AppendEvent(Ok(append)) => {
                let json = append
                    .get_event_json()
                    .context("missing event_json")?
                    .to_str()
                    .context("event_json utf-8 error")?;
                let parsed: Value =
                    serde_json::from_str(json).context("failed to parse event_json")?;
                Ok(parsed)
            }
            control_response::payload::AppendEvent(Err(err)) => {
                Err(anyhow!("failed to decode append_event response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert_eq!(event_record["aggregate_type"], "order");
    assert_eq!(event_record["aggregate_id"], "order-1");
    assert_eq!(event_record["version"], 1);
    next_request_id += 1;

    let aggregates_after: Vec<Value> = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut list = payload.init_list_aggregates();
            list.set_skip(0);
            list.set_take(10);
            list.set_has_take(true);
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::ListAggregates(Ok(list)) => {
                let json = list
                    .get_aggregates_json()
                    .context("missing aggregates_json")?
                    .to_str()
                    .context("aggregates_json utf-8 error")?;
                let parsed: Vec<Value> =
                    serde_json::from_str(json).context("failed to parse aggregates_json")?;
                Ok(parsed)
            }
            control_response::payload::ListAggregates(Err(err)) => {
                Err(anyhow!("failed to decode list_aggregates response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert_eq!(aggregates_after.len(), 1);
    assert_eq!(aggregates_after[0]["aggregate_type"], "order");
    assert_eq!(aggregates_after[0]["version"], 1);
    next_request_id += 1;

    let aggregate_detail = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut get = payload.init_get_aggregate();
            get.set_aggregate_type("order");
            get.set_aggregate_id("order-1");
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::GetAggregate(Ok(get)) => {
                let found = get.get_found();
                let json = get
                    .get_aggregate_json()
                    .context("missing aggregate_json")?
                    .to_str()
                    .context("aggregate_json utf-8 error")?;
                let parsed: Option<Value> = if json.trim().is_empty() {
                    None
                } else {
                    Some(serde_json::from_str(json).context("failed to parse aggregate_json")?)
                };
                Ok((found, parsed))
            }
            control_response::payload::GetAggregate(Err(err)) => {
                Err(anyhow!("failed to decode get_aggregate response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert!(aggregate_detail.0);
    let aggregate_body = aggregate_detail
        .1
        .expect("aggregate payload expected when found");
    assert_eq!(aggregate_body["aggregate_type"], "order");
    assert_eq!(aggregate_body["state"]["status"], "created");
    next_request_id += 1;

    let events: Vec<Value> = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut list = payload.init_list_events();
            list.set_aggregate_type("order");
            list.set_aggregate_id("order-1");
            list.set_skip(0);
            list.set_take(25);
            list.set_has_take(true);
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::ListEvents(Ok(list)) => {
                let json = list
                    .get_events_json()
                    .context("missing events_json")?
                    .to_str()
                    .context("events_json utf-8 error")?;
                let parsed: Vec<Value> =
                    serde_json::from_str(json).context("failed to parse events_json")?;
                Ok(parsed)
            }
            control_response::payload::ListEvents(Err(err)) => {
                Err(anyhow!("failed to decode list_events response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event_type"], "OrderCreated");
    next_request_id += 1;

    let merkle = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut verify = payload.init_verify_aggregate();
            verify.set_aggregate_type("order");
            verify.set_aggregate_id("order-1");
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::VerifyAggregate(Ok(verify)) => {
                let root = verify
                    .get_merkle_root()
                    .context("missing merkle_root")?
                    .to_str()
                    .context("merkle_root utf-8 error")?
                    .to_string();
                Ok(root)
            }
            control_response::payload::VerifyAggregate(Err(err)) => {
                Err(anyhow!("failed to decode verify_aggregate response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert!(!merkle.trim().is_empty());
    assert_eq!(merkle, event_record["merkle_root"]);
    next_request_id += 1;

    let missing = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut get = payload.init_get_aggregate();
            get.set_aggregate_type("inventory");
            get.set_aggregate_id("sku-1");
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::GetAggregate(Ok(get)) => {
                let found = get.get_found();
                Ok(found)
            }
            control_response::payload::GetAggregate(Err(err)) => {
                Err(anyhow!("failed to decode get_aggregate response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert!(!missing);
    next_request_id += 1;

    let error_response = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut append = payload.init_append_event();
            append.set_token("invalid-token");
            append.set_aggregate_type("order");
            append.set_aggregate_id("order-1");
            append.set_event_type("OrderCreated");
            append.set_payload_json(r#"{"status":"ignored"}"#);
            append.set_patch_json("");
            append.set_note("");
            append.set_has_note(false);
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::Error(Ok(error)) => {
                let code = error
                    .get_code()
                    .context("missing error code")?
                    .to_str()
                    .context("code utf-8 error")?
                    .to_string();
                let message = error
                    .get_message()
                    .context("missing error message")?
                    .to_str()
                    .context("message utf-8 error")?
                    .to_string();
                Ok((code, message))
            }
            control_response::payload::Error(Err(err)) => {
                Err(anyhow!("failed to decode control error payload: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert_eq!(error_response.0, "invalid_token");
    assert!(!error_response.1.is_empty());

    drop(writer);
    drop(reader);

    let outcome = server_task.await.context("control handler task panicked")?;
    outcome.context("control handler returned error")?;

    Ok(())
}

async fn send_control_request<W, R, Build, Handle, T>(
    writer: &mut W,
    reader: &mut R,
    request_id: u64,
    build: Build,
    handle: Handle,
) -> Result<T>
where
    W: futures::AsyncWrite + Unpin,
    R: futures::AsyncRead + Unpin,
    Build: FnOnce(&mut control_request::Builder<'_>),
    Handle: FnOnce(control_response::Reader<'_>) -> Result<T>,
{
    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<control_request::Builder>();
        request.set_id(request_id);
        build(&mut request);
    }
    let bytes = write_message_to_words(&message);
    writer
        .write_all(&bytes)
        .await
        .context("failed to send control request")?;
    writer
        .flush()
        .await
        .context("failed to flush control request")?;

    let response_message = read_message(reader, ReaderOptions::new())
        .await
        .context("failed to read control response")?;
    let response = response_message
        .get_root::<control_response::Reader>()
        .context("failed to decode control response")?;

    if response.get_id() != request_id {
        return Err(anyhow!(
            "response id {} did not match request {}",
            response.get_id(),
            request_id
        ));
    }

    handle(response)
}
