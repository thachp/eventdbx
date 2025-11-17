use std::{
    collections::BTreeMap,
    io::Write,
    net::{IpAddr, SocketAddr, TcpStream},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use capnp::{
    message::Builder, message::ReaderOptions, serialize, serialize::write_message_to_words,
};
use eventdbx::replication_noise::{
    perform_client_handshake_blocking, read_encrypted_frame_blocking,
    write_encrypted_frame_blocking,
};
use eventdbx::{
    config::Config,
    control_capnp::{control_hello, control_hello_response, control_request, control_response},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};
use serde_json::{self, Value};

const CONTROL_PROTOCOL_VERSION: u16 = 1;
const DEFAULT_PAGE_SIZE: usize = 256;

#[derive(Clone)]
pub struct ServerClient {
    connect_addr: String,
    tenant: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TenantSchemaPublishResult {
    pub version_id: String,
    pub activated: bool,
    pub skipped: bool,
}

impl ServerClient {
    pub fn new(config: &Config) -> Result<Self> {
        let connect_addr = normalize_connect_addr(&config.socket.bind_addr);

        Ok(Self {
            connect_addr,
            tenant: Some(config.active_domain().to_string()),
        })
    }

    #[allow(dead_code)]
    pub fn with_addr<S: Into<String>>(connect_addr: S) -> Self {
        Self::with_addr_and_tenant(connect_addr, None)
    }

    pub fn with_addr_and_tenant<S: Into<String>>(connect_addr: S, tenant: Option<String>) -> Self {
        Self {
            connect_addr: connect_addr.into(),
            tenant,
        }
    }

    #[allow(dead_code)]
    pub fn with_tenant(mut self, tenant: Option<String>) -> Self {
        self.tenant = tenant;
        self
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
        let connect_addr = self.connect_addr.clone();
        let tenant = self.tenant.clone();
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
                    connect_addr,
                    tenant.clone(),
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
            connect_addr,
            tenant,
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
        let connect_addr = self.connect_addr.clone();
        let tenant = self.tenant.clone();
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
                    connect_addr,
                    tenant.clone(),
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
            connect_addr,
            tenant,
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            metadata,
            note,
        )
    }

    pub fn patch_event(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        patch: &Value,
        metadata: Option<&Value>,
        note: Option<&str>,
    ) -> Result<Option<EventRecord>> {
        let connect_addr = self.connect_addr.clone();
        let tenant = self.tenant.clone();
        let token = token.to_string();
        let aggregate_type = aggregate_type.to_string();
        let aggregate_id = aggregate_id.to_string();
        let event_type = event_type.to_string();
        let patch = patch.clone();
        let metadata = metadata.cloned();
        let note = note.map(|value| value.to_string());

        if tokio::runtime::Handle::try_current().is_ok() {
            return tokio::task::block_in_place(move || {
                patch_event_blocking(
                    connect_addr,
                    tenant.clone(),
                    token,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    patch.clone(),
                    metadata.clone(),
                    note.clone(),
                )
            });
        }

        patch_event_blocking(
            connect_addr,
            tenant,
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            patch,
            metadata,
            note,
        )
    }

    pub fn get_aggregate(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<AggregateState>> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut get = payload_builder.init_get_aggregate();
                get.set_token(token);
                get.set_aggregate_type(aggregate_type);
                get.set_aggregate_id(aggregate_id);
                Ok(())
            },
            |response| {
                use control_response::payload;

                match response
                    .get_payload()
                    .which()
                    .context("failed to decode get_aggregate response payload")?
                {
                    payload::GetAggregate(Ok(result)) => {
                        if !result.get_found() {
                            return Ok(None);
                        }
                        let json = read_text(result.get_aggregate_json(), "aggregate_json")?;
                        if json.trim().is_empty() {
                            Ok(None)
                        } else {
                            let state: AggregateState = serde_json::from_str(&json)
                                .context("failed to parse get_aggregate response payload")?;
                            Ok(Some(state))
                        }
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

    pub fn list_aggregates(
        &self,
        token: &str,
        filter: Option<&str>,
        include_archived: bool,
        archived_only: bool,
    ) -> Result<Vec<AggregateState>> {
        let mut cursor: Option<String> = None;
        let mut results = Vec::new();
        loop {
            let (page, next_cursor) = self.list_aggregates_page(
                token,
                cursor.as_deref(),
                DEFAULT_PAGE_SIZE,
                filter,
                include_archived,
                archived_only,
            )?;
            results.extend(page);
            cursor = next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(results)
    }

    fn list_aggregates_page(
        &self,
        token: &str,
        cursor: Option<&str>,
        take: usize,
        filter: Option<&str>,
        include_archived: bool,
        archived_only: bool,
    ) -> Result<(Vec<AggregateState>, Option<String>)> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut list = payload_builder.init_list_aggregates();
                list.set_token(token);
                if let Some(cursor) = cursor {
                    list.set_has_cursor(true);
                    list.set_cursor(cursor);
                } else {
                    list.set_has_cursor(false);
                    list.set_cursor("");
                }
                list.set_has_take(true);
                list.set_take(take as u64);
                list.set_include_archived(include_archived);
                list.set_archived_only(archived_only);
                if let Some(filter) = filter {
                    list.set_has_filter(true);
                    list.set_filter(filter);
                } else {
                    list.set_has_filter(false);
                }
                list.set_has_sort(false);
                Ok(())
            },
            |response| {
                use control_response::payload;

                match response
                    .get_payload()
                    .which()
                    .context("failed to decode list_aggregates response payload")?
                {
                    payload::ListAggregates(Ok(result)) => {
                        let json = read_text(result.get_aggregates_json(), "aggregates_json")?;
                        let aggregates = if json.trim().is_empty() {
                            Vec::new()
                        } else {
                            serde_json::from_str(&json)
                                .context("failed to parse list_aggregates payload")?
                        };
                        let next_cursor = if result.get_has_next_cursor() {
                            Some(read_text(result.get_next_cursor(), "next_cursor")?)
                        } else {
                            None
                        };
                        Ok((aggregates, next_cursor))
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

    #[allow(dead_code)]
    pub fn list_events(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<EventRecord>> {
        let mut cursor: Option<String> = None;
        let mut results = Vec::new();
        loop {
            let (page, next_cursor) = self.list_events_page(
                token,
                aggregate_type,
                aggregate_id,
                cursor.as_deref(),
                DEFAULT_PAGE_SIZE,
            )?;
            results.extend(page);
            cursor = next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(results)
    }

    pub fn list_events_since(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        start_version: u64,
        archived: bool,
    ) -> Result<Vec<EventRecord>> {
        let mut cursor = if start_version == 0 {
            None
        } else {
            let prefix = if archived { 'r' } else { 'a' };
            Some(format!(
                "{}:{}:{}:{}",
                prefix, aggregate_type, aggregate_id, start_version
            ))
        };
        let mut results = Vec::new();
        loop {
            let (page, next_cursor) = self.list_events_page(
                token,
                aggregate_type,
                aggregate_id,
                cursor.as_deref(),
                DEFAULT_PAGE_SIZE,
            )?;
            results.extend(page);
            cursor = next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(results)
    }

    fn list_events_page(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        cursor: Option<&str>,
        take: usize,
    ) -> Result<(Vec<EventRecord>, Option<String>)> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut list = payload_builder.init_list_events();
                list.set_token(token);
                list.set_aggregate_type(aggregate_type);
                list.set_aggregate_id(aggregate_id);
                if let Some(cursor) = cursor {
                    list.set_has_cursor(true);
                    list.set_cursor(cursor);
                } else {
                    list.set_has_cursor(false);
                    list.set_cursor("");
                }
                list.set_has_take(true);
                list.set_take(take as u64);
                list.set_has_filter(false);
                Ok(())
            },
            |response| {
                use control_response::payload;

                match response
                    .get_payload()
                    .which()
                    .context("failed to decode list_events response payload")?
                {
                    payload::ListEvents(Ok(result)) => {
                        let json = read_text(result.get_events_json(), "events_json")?;
                        let events = if json.trim().is_empty() {
                            Vec::new()
                        } else {
                            serde_json::from_str(&json)
                                .context("failed to parse list_events payload")?
                        };
                        let next_cursor = if result.get_has_next_cursor() {
                            Some(read_text(result.get_next_cursor(), "next_cursor")?)
                        } else {
                            None
                        };
                        Ok((events, next_cursor))
                    }
                    payload::Error(Ok(error)) => {
                        let code = read_text(error.get_code(), "code")?;
                        if code == "aggregate_not_found" {
                            return Ok((Vec::new(), None));
                        }
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

    pub fn set_aggregate_archive(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        archived: bool,
    ) -> Result<()> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut set = payload_builder.init_set_aggregate_archive();
                set.set_token(token);
                set.set_aggregate_type(aggregate_type);
                set.set_aggregate_id(aggregate_id);
                set.set_archived(archived);
                set.set_has_comment(false);
                Ok(())
            },
            |response| {
                use control_response::payload;

                match response
                    .get_payload()
                    .which()
                    .context("failed to decode set_aggregate_archive response payload")?
                {
                    payload::SetAggregateArchive(Ok(_)) => Ok(()),
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

    pub fn list_schemas(&self, token: &str) -> Result<BTreeMap<String, AggregateSchema>> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut list = payload_builder.init_list_schemas();
                list.set_token(token);
                Ok(())
            },
            |response| {
                use control_response::payload;

                match response
                    .get_payload()
                    .which()
                    .context("failed to decode list_schemas response payload")?
                {
                    payload::ListSchemas(Ok(result)) => {
                        let json = read_text(result.get_schemas_json(), "schemas_json")?;
                        if json.trim().is_empty() {
                            Ok(BTreeMap::new())
                        } else {
                            let schemas = serde_json::from_str(&json)
                                .context("failed to parse list_schemas payload")?;
                            Ok(schemas)
                        }
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

    pub fn replace_schemas(
        &self,
        token: &str,
        schemas: &BTreeMap<String, AggregateSchema>,
    ) -> Result<()> {
        let payload_json = serde_json::to_string(schemas)
            .context("failed to serialize schema snapshot for remote replace")?;
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut replace = payload_builder.init_replace_schemas();
                replace.set_token(token);
                replace.set_schemas_json(&payload_json);
                Ok(())
            },
            |response| {
                use control_response::payload;

                match response
                    .get_payload()
                    .which()
                    .context("failed to decode replace_schemas response payload")?
                {
                    payload::ReplaceSchemas(Ok(_)) => Ok(()),
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

    pub fn assign_tenant(&self, token: &str, tenant: &str, shard: &str) -> Result<bool> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut assign = payload_builder.init_tenant_assign();
                assign.set_token(token);
                assign.set_tenant_id(tenant);
                assign.set_shard_id(shard);
                Ok(())
            },
            |response| {
                use control_response::payload;
                match response
                    .get_payload()
                    .which()
                    .context("failed to decode tenant_assign response payload")?
                {
                    payload::TenantAssign(Ok(reply)) => Ok(reply.get_changed()),
                    payload::TenantAssign(Err(err)) => {
                        Err(anyhow!("failed to decode tenant_assign response: {}", err))
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

    pub fn unassign_tenant(&self, token: &str, tenant: &str) -> Result<bool> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut unassign = payload_builder.init_tenant_unassign();
                unassign.set_token(token);
                unassign.set_tenant_id(tenant);
                Ok(())
            },
            |response| {
                use control_response::payload;
                match response
                    .get_payload()
                    .which()
                    .context("failed to decode tenant_unassign response payload")?
                {
                    payload::TenantUnassign(Ok(reply)) => Ok(reply.get_changed()),
                    payload::TenantUnassign(Err(err)) => Err(anyhow!(
                        "failed to decode tenant_unassign response: {}",
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

    pub fn set_tenant_quota(
        &self,
        token: &str,
        tenant: &str,
        max_megabytes: u64,
    ) -> Result<bool> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut quota = payload_builder.init_tenant_quota_set();
                quota.set_token(token);
                quota.set_tenant_id(tenant);
                quota.set_max_storage_mb(max_megabytes);
                Ok(())
            },
            |response| {
                use control_response::payload;
                match response
                    .get_payload()
                    .which()
                    .context("failed to decode tenant_quota_set response payload")?
                {
                    payload::TenantQuotaSet(Ok(reply)) => Ok(reply.get_changed()),
                    payload::TenantQuotaSet(Err(err)) => Err(anyhow!(
                        "failed to decode tenant_quota_set response: {}",
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

    pub fn clear_tenant_quota(&self, token: &str, tenant: &str) -> Result<bool> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut quota = payload_builder.init_tenant_quota_clear();
                quota.set_token(token);
                quota.set_tenant_id(tenant);
                Ok(())
            },
            |response| {
                use control_response::payload;
                match response
                    .get_payload()
                    .which()
                    .context("failed to decode tenant_quota_clear response payload")?
                {
                    payload::TenantQuotaClear(Ok(reply)) => Ok(reply.get_changed()),
                    payload::TenantQuotaClear(Err(err)) => Err(anyhow!(
                        "failed to decode tenant_quota_clear response: {}",
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

    pub fn recalc_tenant_storage(&self, token: &str, tenant: &str) -> Result<u64> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut quota = payload_builder.init_tenant_quota_recalc();
                quota.set_token(token);
                quota.set_tenant_id(tenant);
                Ok(())
            },
            |response| {
                use control_response::payload;
                match response
                    .get_payload()
                    .which()
                    .context("failed to decode tenant_quota_recalc response payload")?
                {
                    payload::TenantQuotaRecalc(Ok(reply)) => Ok(reply.get_storage_bytes()),
                    payload::TenantQuotaRecalc(Err(err)) => Err(anyhow!(
                        "failed to decode tenant_quota_recalc response: {}",
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

    pub fn reload_tenant(&self, token: &str, tenant: &str) -> Result<bool> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
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

    pub fn publish_tenant_schemas(
        &self,
        token: &str,
        tenant: &str,
        reason: Option<&str>,
        actor: Option<&str>,
        labels: &[String],
        activate: bool,
        force: bool,
        reload: bool,
    ) -> Result<TenantSchemaPublishResult> {
        let request_id = next_request_id();
        send_control_request_blocking(
            &self.connect_addr,
            token,
            self.tenant.as_deref(),
            request_id,
            |request| {
                let payload_builder = request.reborrow().init_payload();
                let mut publish = payload_builder.init_tenant_schema_publish();
                publish.set_token(token);
                publish.set_tenant_id(tenant);
                if let Some(text) = reason {
                    publish.set_has_reason(true);
                    publish.set_reason(text);
                } else {
                    publish.set_has_reason(false);
                    publish.set_reason("");
                }
                if let Some(text) = actor {
                    publish.set_has_actor(true);
                    publish.set_actor(text);
                } else {
                    publish.set_has_actor(false);
                    publish.set_actor("");
                }
                publish.set_activate(activate);
                publish.set_force(force);
                publish.set_reload(reload);
                let mut labels_builder = publish.init_labels(labels.len() as u32);
                for (idx, label) in labels.iter().enumerate() {
                    labels_builder.set(idx as u32, label);
                }
                Ok(())
            },
            |response| {
                use control_response::payload;
                match response
                    .get_payload()
                    .which()
                    .context("failed to decode tenant_schema_publish response payload")?
                {
                    payload::TenantSchemaPublish(Ok(reply)) => {
                        let version_id = read_text(reply.get_version_id(), "version_id")?;
                        Ok(TenantSchemaPublishResult {
                            version_id,
                            activated: reply.get_activated(),
                            skipped: reply.get_skipped(),
                        })
                    }
                    payload::TenantSchemaPublish(Err(err)) => Err(anyhow!(
                        "failed to decode tenant_schema_publish response: {}",
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
}

fn create_aggregate_blocking(
    connect_addr: String,
    tenant: Option<String>,
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
        &connect_addr,
        &token,
        tenant.as_deref(),
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
    connect_addr: String,
    tenant: Option<String>,
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
        &connect_addr,
        &token,
        tenant.as_deref(),
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

fn patch_event_blocking(
    connect_addr: String,
    tenant: Option<String>,
    token: String,
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    patch: Value,
    metadata: Option<Value>,
    note: Option<String>,
) -> Result<Option<EventRecord>> {
    let request_id = next_request_id();

    let patch_json =
        serde_json::to_string(&patch).context("failed to serialize patch for proxy request")?;
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
        &connect_addr,
        &token,
        tenant.as_deref(),
        request_id,
        |request| {
            let payload_builder = request.reborrow().init_payload();
            let mut patch_builder = payload_builder.init_patch_event();
            patch_builder.set_token(&token);
            patch_builder.set_aggregate_type(&aggregate_type);
            patch_builder.set_aggregate_id(&aggregate_id);
            patch_builder.set_event_type(&event_type);
            patch_builder.set_patch_json(&patch_json);
            patch_builder.set_metadata_json(&metadata_json);
            patch_builder.set_has_metadata(has_metadata);
            patch_builder.set_note(&note_text);
            patch_builder.set_has_note(has_note);
            Ok(())
        },
        |response| {
            use control_response::payload;

            match response
                .get_payload()
                .which()
                .context("failed to decode patch response payload")?
            {
                payload::AppendEvent(Ok(append)) => {
                    let event_json = read_text(append.get_event_json(), "event_json")?;
                    if event_json.trim().is_empty() {
                        Ok(None)
                    } else {
                        let record: EventRecord = serde_json::from_str(&event_json)
                            .context("failed to parse patch response payload")?;
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
    connect_addr: &str,
    handshake_token: &str,
    handshake_tenant: Option<&str>,
    request_id: u64,
    build: Build,
    handle: Handle,
) -> Result<T>
where
    Build: FnOnce(&mut control_request::Builder<'_>) -> Result<()>,
    Handle: FnOnce(control_response::Reader<'_>) -> Result<T>,
{
    let mut stream = TcpStream::connect(connect_addr)
        .with_context(|| format!("failed to connect to CLI proxy at {}", connect_addr))?;
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
        if let Some(tenant) = handshake_tenant {
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
    if !response.get_accepted() {
        let reason = read_text(response.get_message(), "control handshake message")?;
        bail!("control handshake rejected: {}", reason);
    }

    let mut noise = perform_client_handshake_blocking(&mut stream, handshake_token.as_bytes())
        .context("failed to establish encrypted control channel")?;

    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<control_request::Builder>();
        request.set_id(request_id);
        build(&mut request)?;
    }
    let request_bytes = write_message_to_words(&message);
    write_encrypted_frame_blocking(&mut stream, &mut noise, &request_bytes)
        .context("failed to send control request")?;

    let response_bytes = read_encrypted_frame_blocking(&mut stream, &mut noise)?
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
