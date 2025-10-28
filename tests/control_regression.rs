use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, anyhow};
use capnp::{message::Builder, message::ReaderOptions, serialize::write_message_to_words};
use capnp_futures::serialize::read_message;
use eventdbx::{
    cli_proxy::test_support::spawn_control_session,
    config::Config,
    control_capnp::{control_request, control_response},
    schema::{CreateSchemaInput, SchemaManager},
    service::CoreContext,
    store::EventStore,
    token::{IssueTokenInput, JwtLimits, TokenManager},
};
use futures::AsyncWriteExt;
use serde_json::{Value, json};
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
    schemas.create(CreateSchemaInput {
        aggregate: "order".into(),
        events: vec!["order_created".into(), "order_status_updated".into()],
        snapshot_threshold: None,
    })?;
    let store = Arc::new(EventStore::open(
        config.event_store_path(),
        None,
        config.snowflake_worker_id,
    )?);

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
            list.set_has_sort(false);
            list.set_include_archived(false);
            list.set_archived_only(false);
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
            append.set_event_type("order_created");
            append.set_payload_json(
                r#"{"status":"created","firstName":"John","address":{"street":"123 Main","zipCode":"94107"}}"#,
            );
            append.set_metadata_json("");
            append.set_note("");
            append.set_has_note(false);
            append.set_has_metadata(false);
        },
        |response| {
            use control_response::payload;

            match response
                .get_payload()
                .which()
                .context("payload decode failed")?
            {
                payload::AppendEvent(Ok(append)) => {
                    let json = append
                        .get_event_json()
                        .context("missing event_json")?
                        .to_str()
                        .context("event_json utf-8 error")?;
                    let parsed: Value =
                        serde_json::from_str(json).context("failed to parse event_json")?;
                    Ok(parsed)
                }
                payload::AppendEvent(Err(err)) => {
                    Err(anyhow!("failed to decode append_event response: {err}"))
                }
                payload::Error(Ok(error)) => {
                    let code = error
                        .get_code()
                        .context("missing error code")?
                        .to_str()
                        .context("error code utf-8 error")?
                        .to_string();
                    let message = error
                        .get_message()
                        .context("missing error message")?
                        .to_str()
                        .context("error message utf-8 error")?
                        .to_string();
                    Err(anyhow!("patch_event returned {}: {}", code, message))
                }
                payload::Error(Err(err)) => Err(anyhow!("failed to decode error payload: {err}")),
                payload::GetAggregate(_) => {
                    Err(anyhow!("unexpected get_aggregate response to patch_event"))
                }
                payload::ListAggregates(_) => Err(anyhow!(
                    "unexpected list_aggregates response to patch_event"
                )),
                payload::ListEvents(_) => {
                    Err(anyhow!("unexpected list_events response to patch_event"))
                }
                payload::VerifyAggregate(_) => Err(anyhow!(
                    "unexpected verify_aggregate response to patch_event"
                )),
                payload::CreateAggregate(_) => Err(anyhow!(
                    "unexpected create_aggregate response to patch_event"
                )),
                payload::SelectAggregate(_) => Err(anyhow!(
                    "unexpected select_aggregate response to patch_event"
                )),
            }
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
            list.set_has_sort(false);
            list.set_include_archived(false);
            list.set_archived_only(false);
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
        |response| {
            use control_response::payload;

            match response
                .get_payload()
                .which()
                .context("payload decode failed")?
            {
                payload::GetAggregate(Ok(get)) => {
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
                payload::GetAggregate(Err(err)) => {
                    Err(anyhow!("failed to decode get_aggregate response: {err}"))
                }
                payload::Error(Ok(error)) => {
                    let code = error
                        .get_code()
                        .context("missing error code")?
                        .to_str()
                        .context("error code utf-8 error")?
                        .to_string();
                    let message = error
                        .get_message()
                        .context("missing error message")?
                        .to_str()
                        .context("error message utf-8 error")?
                        .to_string();
                    Err(anyhow!("get_aggregate returned {}: {}", code, message))
                }
                payload::Error(Err(err)) => Err(anyhow!("failed to decode error payload: {err}")),
                payload::AppendEvent(_) => {
                    Err(anyhow!("unexpected append_event response to get_aggregate"))
                }
                payload::ListAggregates(_) => Err(anyhow!(
                    "unexpected list_aggregates response to get_aggregate"
                )),
                payload::ListEvents(_) => {
                    Err(anyhow!("unexpected list_events response to get_aggregate"))
                }
                payload::VerifyAggregate(_) => Err(anyhow!(
                    "unexpected verify_aggregate response to get_aggregate"
                )),
                payload::CreateAggregate(_) => Err(anyhow!(
                    "unexpected create_aggregate response to get_aggregate"
                )),
                payload::SelectAggregate(_) => Err(anyhow!(
                    "unexpected select_aggregate response to get_aggregate"
                )),
            }
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
    assert_eq!(events[0]["event_type"], "order_created");
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
            append.set_event_type("order_created");
            append.set_payload_json(r#"{"status":"ignored"}"#);
            append.set_metadata_json("");
            append.set_note("");
            append.set_has_note(false);
            append.set_has_metadata(false);
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

#[tokio::test(flavor = "multi_thread")]
async fn control_capnp_patch_requires_existing() -> Result<()> {
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
    schemas.create(CreateSchemaInput {
        aggregate: "order".into(),
        events: vec!["order_created".into(), "order_status_updated".into()],
        snapshot_threshold: None,
    })?;
    let store = Arc::new(EventStore::open(
        config.event_store_path(),
        None,
        config.snowflake_worker_id,
    )?);

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

    let token_record = tokens.issue(IssueTokenInput {
        subject: "system:admin".into(),
        group: "system".into(),
        user: "admin".into(),
        root: true,
        actions: Vec::new(),
        resources: Vec::new(),
        ttl_secs: Some(3_600),
        not_before: None,
        issued_by: "control-batch-test".into(),
        limits: JwtLimits::default(),
    })?;

    let missing_patch_error = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut patch = payload.init_patch_event();
            patch.set_token(&token_record.token);
            patch.set_aggregate_type("order");
            patch.set_aggregate_id("order-unknown");
            patch.set_event_type("order_status_updated");
            patch.set_patch_json(
                r#"[
                    {"op":"replace","path":"/firstName","value":"Jane"},
                    {"op":"add","path":"/age","value":30},
                    {"op":"remove","path":"/address/zipCode"}
                ]"#,
            );
            patch.set_note("");
            patch.set_has_note(false);
            patch.set_metadata_json("");
            patch.set_has_metadata(false);
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
    assert_eq!(missing_patch_error.0, "aggregate_not_found");
    next_request_id += 1;

    let append_record: Value = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut append = payload.init_append_event();
            append.set_token(&token_record.token);
            append.set_aggregate_type("order");
            append.set_aggregate_id("order-1");
            append.set_event_type("order_created");
            append.set_payload_json(
                r#"{"status":"created","firstName":"John","address":{"street":"123 Main","zipCode":"94107"}}"#,
            );
            append.set_metadata_json("");
            append.set_note("");
            append.set_has_note(false);
            append.set_has_metadata(false);
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
    assert_eq!(append_record["version"], 1);
    assert_eq!(append_record["payload"]["firstName"], "John");
    let initial_address = append_record["payload"]["address"]
        .as_object()
        .expect("initial address object");
    assert_eq!(
        initial_address.get("zipCode").and_then(Value::as_str),
        Some("94107")
    );
    next_request_id += 1;

    let patch_record: Value = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut patch = payload.init_patch_event();
            patch.set_token(&token_record.token);
            patch.set_aggregate_type("order");
            patch.set_aggregate_id("order-1");
            patch.set_event_type("order_status_updated");
            patch.set_patch_json(
                r#"[
                    {"op":"replace","path":"/firstName","value":"Jane"},
                    {"op":"add","path":"/age","value":30},
                    {"op":"remove","path":"/address/zipCode"}
                ]"#,
            );
            patch.set_note("");
            patch.set_has_note(false);
            patch.set_metadata_json("");
            patch.set_has_metadata(false);
        },
        |response| {
            use control_response::payload;

            match response
                .get_payload()
                .which()
                .context("payload decode failed")?
            {
                payload::AppendEvent(Ok(append)) => {
                    let json = append
                        .get_event_json()
                        .context("missing event_json")?
                        .to_str()
                        .context("event_json utf-8 error")?;
                    let parsed: Value =
                        serde_json::from_str(json).context("failed to parse event_json")?;
                    Ok(parsed)
                }
                payload::AppendEvent(Err(err)) => {
                    Err(anyhow!("failed to decode append_event response: {err}"))
                }
                payload::Error(Ok(error)) => {
                    let code = error
                        .get_code()
                        .context("missing error code")?
                        .to_str()
                        .context("error code utf-8 error")?
                        .to_string();
                    let message = error
                        .get_message()
                        .context("missing error message")?
                        .to_str()
                        .context("error message utf-8 error")?
                        .to_string();
                    Err(anyhow!("patch_event returned {}: {}", code, message))
                }
                payload::Error(Err(err)) => Err(anyhow!("failed to decode error payload: {err}")),
                payload::GetAggregate(_) => {
                    Err(anyhow!("unexpected get_aggregate response to patch_event"))
                }
                payload::ListAggregates(_) => Err(anyhow!(
                    "unexpected list_aggregates response to patch_event"
                )),
                payload::ListEvents(_) => {
                    Err(anyhow!("unexpected list_events response to patch_event"))
                }
                payload::VerifyAggregate(_) => Err(anyhow!(
                    "unexpected verify_aggregate response to patch_event"
                )),
                payload::CreateAggregate(_) => Err(anyhow!(
                    "unexpected create_aggregate response to patch_event"
                )),
                payload::SelectAggregate(_) => Err(anyhow!(
                    "unexpected select_aggregate response to patch_event"
                )),
            }
        },
    )
    .await?;
    assert_eq!(patch_record["version"], 2);
    assert_eq!(patch_record["event_type"], "order_status_updated");
    assert_eq!(patch_record["payload"]["firstName"], "Jane");
    assert_eq!(patch_record["payload"]["age"], 30);
    let patch_address = patch_record["payload"]["address"]
        .as_object()
        .expect("address payload object");
    assert!(
        !patch_address.contains_key("zipCode"),
        "zipCode should be removed from address payload"
    );
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
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["event_type"], "order_status_updated");
    assert_eq!(events[1]["payload"]["firstName"], "Jane");
    assert_eq!(events[1]["payload"]["age"], 30);
    let patched_address = events[1]["payload"]["address"]
        .as_object()
        .expect("patched address payload object");
    assert!(
        !patched_address.contains_key("zipCode"),
        "zipCode should be removed in event payload"
    );

    next_request_id += 1;

    let aggregate_after_patch = send_control_request(
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
    assert!(aggregate_after_patch.0);
    let aggregate_body = aggregate_after_patch.1.expect("aggregate body expected");
    let state = aggregate_body["state"].as_object().expect("state object");
    assert_eq!(state.get("firstName").and_then(Value::as_str), Some("Jane"));
    assert_eq!(
        state.get("age").and_then(Value::as_str),
        Some("30"),
        "age should be stored as stringified value"
    );
    let address_state = state
        .get("address")
        .and_then(Value::as_str)
        .expect("address state entry");
    let parsed_address: Value =
        serde_json::from_str(address_state).context("failed to parse address state JSON")?;
    assert!(
        !parsed_address.get("zipCode").is_some(),
        "address state should not include zipCode"
    );

    next_request_id += 1;

    let selection = send_control_request(
        &mut writer,
        &mut reader,
        next_request_id,
        |request| {
            let payload = request.reborrow().init_payload();
            let mut select = payload.init_select_aggregate();
            select.set_aggregate_type("order");
            select.set_aggregate_id("order-1");
            let mut fields = select.reborrow().init_fields(3);
            fields.set(0, "firstName");
            fields.set(1, "address.street");
            fields.set(2, "address.zipCode");
        },
        |response| match response
            .get_payload()
            .which()
            .context("payload decode failed")?
        {
            control_response::payload::SelectAggregate(Ok(select)) => {
                let found = select.get_found();
                let json = select
                    .get_selection_json()
                    .context("missing selection_json")?
                    .to_str()
                    .context("selection_json utf-8 error")?;
                let parsed: Option<Value> = if json.trim().is_empty() {
                    None
                } else {
                    Some(serde_json::from_str(json).context("failed to parse selection_json")?)
                };
                Ok((found, parsed))
            }
            control_response::payload::SelectAggregate(Err(err)) => {
                Err(anyhow!("failed to decode select_aggregate response: {err}"))
            }
            _ => Err(anyhow!("unexpected response variant")),
        },
    )
    .await?;
    assert!(selection.0, "selection should report aggregate found");
    let selection_body = selection
        .1
        .expect("selection body expected when aggregate found");
    assert_eq!(selection_body["firstName"], json!("Jane"));
    assert_eq!(selection_body["address.street"], json!("123 Main"));
    assert!(selection_body["address.zipCode"].is_null());

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
