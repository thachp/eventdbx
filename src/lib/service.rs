use std::{collections::BTreeMap, sync::Arc};

use crate::{
    error::{EventError, Result},
    filter::FilterExpr,
    restrict::{self, RestrictMode},
    schema::SchemaManager,
    store::{
        ActorClaims, AggregateCursor, AggregateQueryScope, AggregateSort, AggregateState,
        AppendEvent, EventArchiveScope, EventCursor, EventQueryScope, EventRecord, EventStore,
        select_state_field,
    },
    tenant_store::TenantAssignmentStore,
    token::{JwtClaims, TokenManager},
    validation::{
        ensure_aggregate_id, ensure_first_event_rule, ensure_metadata_extensions,
        ensure_payload_size, ensure_snake_case,
    },
};
use serde_json::Value;
/// Shared context exposing the core store, schema, and token managers so
/// higher-level surfaces (REST, GraphQL, gRPC, plugins) can be layered on
/// top of a consistent API.
#[derive(Clone)]
pub struct CoreContext {
    tokens: Arc<TokenManager>,
    schemas: Arc<SchemaManager>,
    store: Arc<EventStore>,
    restrict: RestrictMode,
    list_page_size: usize,
    page_limit: usize,
    tenant_id: String,
    aggregate_quota: Option<u64>,
    assignments: Arc<TenantAssignmentStore>,
}

impl CoreContext {
    pub fn new(
        tokens: Arc<TokenManager>,
        schemas: Arc<SchemaManager>,
        store: Arc<EventStore>,
        restrict: RestrictMode,
        list_page_size: usize,
        page_limit: usize,
        tenant_id: impl Into<String>,
        aggregate_quota: Option<u64>,
        assignments: Arc<TenantAssignmentStore>,
    ) -> Self {
        Self {
            tokens,
            schemas,
            store,
            restrict,
            list_page_size,
            page_limit,
            tenant_id: tenant_id.into(),
            aggregate_quota,
            assignments,
        }
    }

    pub fn tokens(&self) -> Arc<TokenManager> {
        Arc::clone(&self.tokens)
    }

    pub fn schemas(&self) -> Arc<SchemaManager> {
        Arc::clone(&self.schemas)
    }

    pub fn store(&self) -> Arc<EventStore> {
        Arc::clone(&self.store)
    }

    pub fn assignments(&self) -> Arc<TenantAssignmentStore> {
        Arc::clone(&self.assignments)
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn restrict(&self) -> RestrictMode {
        self.restrict
    }

    pub fn list_page_size(&self) -> usize {
        self.list_page_size
    }

    pub fn page_limit(&self) -> usize {
        self.page_limit
    }

    pub fn is_hidden_aggregate(&self, aggregate_type: &str) -> bool {
        self.schemas
            .get(aggregate_type)
            .map(|schema| schema.hidden)
            .unwrap_or(false)
    }

    pub fn sanitize_aggregate(&self, mut aggregate: AggregateState) -> AggregateState {
        aggregate.state = self.filter_state_map(&aggregate.aggregate_type, aggregate.state);
        aggregate
    }

    fn ensure_tenant_count_initialized(&self) -> Result<u64> {
        let store = Arc::clone(&self.store);
        self.assignments
            .ensure_aggregate_count(&self.tenant_id, || {
                store
                    .counts()
                    .map(|counts| counts.total_aggregates() as u64)
            })
    }

    fn enforce_aggregate_quota(&self) -> Result<()> {
        let current = self.ensure_tenant_count_initialized()?;
        if let Some(limit) = self.aggregate_quota {
            if current >= limit {
                return Err(EventError::TenantQuotaExceeded(format!(
                    "tenant '{}' reached aggregate quota ({limit})",
                    self.tenant_id
                )));
            }
        }
        Ok(())
    }

    pub fn list_aggregates(
        &self,
        token: &str,
        cursor: Option<&AggregateCursor>,
        take: Option<usize>,
        filter: Option<FilterExpr>,
        sort: Option<&[AggregateSort]>,
        scope: AggregateQueryScope,
    ) -> Result<(Vec<AggregateState>, Option<AggregateCursor>)> {
        let mut effective_take = take.unwrap_or(self.list_page_size);
        if effective_take == 0 {
            return Ok((Vec::new(), None));
        }
        if effective_take > self.page_limit {
            effective_take = self.page_limit;
        }

        let claims: JwtClaims = self
            .tokens()
            .authorize_action(token, "aggregate.read", None)?;
        let filter_ref = filter.as_ref();

        let mut transform = |aggregate: AggregateState| {
            if self.is_hidden_aggregate(&aggregate.aggregate_type) {
                return None;
            }
            let resource =
                Self::aggregate_resource(&aggregate.aggregate_type, &aggregate.aggregate_id);
            if !claims.allows_resource(Some(resource.as_str())) {
                return None;
            }
            let sanitized = self.sanitize_aggregate(aggregate);
            if let Some(expr) = filter_ref {
                if !expr.matches_aggregate(&sanitized) {
                    return None;
                }
            }
            Some(sanitized)
        };

        if cursor.is_some() && sort.is_some() {
            return Err(EventError::InvalidCursor(
                "cursor cannot be combined with sort directives".into(),
            ));
        }

        if let Some(keys) = sort {
            let aggregates = self.store.aggregates_paginated_with_transform(
                0,
                Some(effective_take),
                Some(keys),
                scope,
                &mut transform,
            );
            return Ok((aggregates, None));
        }

        self.store
            .aggregates_page_with_transform(cursor, effective_take, scope, &mut transform)
    }

    pub fn get_aggregate(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<AggregateState>> {
        let resource = Self::aggregate_resource(aggregate_type, aggregate_id);
        self.tokens()
            .authorize_action(token, "aggregate.read", Some(resource.as_str()))?;

        if self.is_hidden_aggregate(aggregate_type) {
            return Ok(None);
        }
        match self.store.get_aggregate_state(aggregate_type, aggregate_id) {
            Ok(aggregate) => Ok(Some(self.sanitize_aggregate(aggregate))),
            Err(EventError::AggregateNotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn select_aggregate_fields(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        fields: &[String],
    ) -> Result<Option<BTreeMap<String, Value>>> {
        let resource = Self::aggregate_resource(aggregate_type, aggregate_id);
        self.tokens()
            .authorize_action(token, "aggregate.read", Some(resource.as_str()))?;

        if self.is_hidden_aggregate(aggregate_type) {
            return Ok(None);
        }

        let aggregate = match self.store.get_aggregate_state(aggregate_type, aggregate_id) {
            Ok(aggregate) => self.sanitize_aggregate(aggregate),
            Err(EventError::AggregateNotFound) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut selections = BTreeMap::new();
        for field in fields {
            let trimmed = field.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value = select_state_field(&aggregate.state, trimmed).unwrap_or(Value::Null);
            selections.insert(trimmed.to_string(), value);
        }

        Ok(Some(selections))
    }

    pub fn list_events(
        &self,
        token: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        cursor: Option<&EventCursor>,
        take: Option<usize>,
    ) -> Result<(Vec<EventRecord>, Option<EventCursor>)> {
        let resource = Self::aggregate_resource(aggregate_type, aggregate_id);
        self.tokens()
            .authorize_action(token, "aggregate.read", Some(resource.as_str()))?;

        if self.is_hidden_aggregate(aggregate_type) {
            return Err(EventError::AggregateNotFound);
        }

        let mut effective_take = take.unwrap_or(self.page_limit);
        if effective_take == 0 {
            return Ok((Vec::new(), None));
        }
        if effective_take > self.page_limit {
            effective_take = self.page_limit;
        }

        self.store.events_page(
            EventQueryScope::Aggregate {
                aggregate_type,
                aggregate_id,
            },
            EventArchiveScope::ActiveOnly,
            cursor,
            effective_take,
            None,
        )
    }

    pub fn verify_aggregate(&self, aggregate_type: &str, aggregate_id: &str) -> Result<String> {
        if self.is_hidden_aggregate(aggregate_type) {
            return Err(EventError::AggregateNotFound);
        }
        self.store.verify(aggregate_type, aggregate_id)
    }

    pub fn create_aggregate(&self, input: CreateAggregateInput) -> Result<AggregateState> {
        let CreateAggregateInput {
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            metadata,
            note,
        } = input;

        ensure_snake_case("aggregate_type", &aggregate_type)?;
        ensure_snake_case("event_type", &event_type)?;
        ensure_aggregate_id(&aggregate_id)?;
        if let Some(ref metadata) = metadata {
            ensure_metadata_extensions(metadata)?;
        }
        ensure_payload_size(&payload)?;

        let store = self.store();
        if store
            .aggregate_version(&aggregate_type, &aggregate_id)?
            .is_some()
        {
            return Err(EventError::SchemaViolation(format!(
                "aggregate {}::{} already exists",
                aggregate_type, aggregate_id
            )));
        }

        self.enforce_aggregate_quota()?;

        let resource = format!("aggregate:{}:{}", aggregate_type, aggregate_id);
        let claims =
            self.tokens()
                .authorize_action(&token, "aggregate.create", Some(resource.as_str()))?;

        if claims.actor_claims().is_none() {
            return Err(EventError::Unauthorized);
        }

        let append_input = AppendEventInput {
            token,
            aggregate_type: aggregate_type.clone(),
            aggregate_id: aggregate_id.clone(),
            event_type,
            payload: Some(payload),
            patch: None,
            metadata,
            note,
        };
        let _record = self.append_event_internal(append_input, true)?;
        self.assignments
            .increment_aggregate_count(&self.tenant_id, 1)?;

        let aggregate = store.get_aggregate_state(&aggregate_type, &aggregate_id)?;
        Ok(self.sanitize_aggregate(aggregate))
    }

    pub fn set_aggregate_archive(&self, input: SetAggregateArchiveInput) -> Result<AggregateState> {
        let SetAggregateArchiveInput {
            token,
            aggregate_type,
            aggregate_id,
            archived,
            comment,
        } = input;

        ensure_snake_case("aggregate_type", &aggregate_type)?;
        ensure_aggregate_id(&aggregate_id)?;

        let resource = format!("aggregate:{}:{}", aggregate_type, aggregate_id);
        let claims =
            self.tokens()
                .authorize_action(&token, "aggregate.archive", Some(resource.as_str()))?;

        if claims.actor_claims().is_none() {
            return Err(EventError::Unauthorized);
        }

        let comment = normalize_optional_comment(comment);

        let store = self.store();
        let state = store.set_archive(&aggregate_type, &aggregate_id, archived, comment)?;
        Ok(self.sanitize_aggregate(state))
    }

    pub fn append_event(&self, input: AppendEventInput) -> Result<EventRecord> {
        let allow_new = input.payload.is_some();
        self.append_event_internal(input, allow_new)
    }

    fn append_event_internal(
        &self,
        input: AppendEventInput,
        allow_new_aggregate: bool,
    ) -> Result<EventRecord> {
        let AppendEventInput {
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            patch,
            metadata,
            note,
        } = input;

        enum EventBody {
            Payload(Value),
            Patch(Value),
        }

        let event_body = match (payload, patch) {
            (Some(payload), None) => EventBody::Payload(payload),
            (None, Some(patch_spec)) => EventBody::Patch(patch_spec),
            (Some(_), Some(_)) => {
                return Err(EventError::InvalidSchema(
                    "payload_json and patch_json cannot both be provided".into(),
                ));
            }
            (None, None) => {
                return Err(EventError::InvalidSchema(
                    "payload_json must be provided when patch_json is absent".into(),
                ));
            }
        };

        ensure_snake_case("aggregate_type", &aggregate_type)?;
        ensure_snake_case("event_type", &event_type)?;
        ensure_aggregate_id(&aggregate_id)?;
        if let Some(ref metadata) = metadata {
            ensure_metadata_extensions(metadata)?;
        }

        let aggregate_version = self
            .store
            .aggregate_version(&aggregate_type, &aggregate_id)?;
        let is_new_aggregate = match aggregate_version {
            Some(version) if version > 0 => false,
            Some(_) | None => true,
        };
        let is_patch_event = matches!(&event_body, EventBody::Patch(_));
        if (!allow_new_aggregate || is_patch_event) && is_new_aggregate {
            return Err(EventError::AggregateNotFound);
        }
        ensure_first_event_rule(is_new_aggregate, &event_type)?;

        let resource = format!("aggregate:{}:{}", aggregate_type, aggregate_id);
        let claims =
            self.tokens()
                .authorize_action(&token, "aggregate.append", Some(resource.as_str()))?;

        let effective_payload = match event_body {
            EventBody::Payload(payload) => {
                ensure_payload_size(&payload)?;
                payload
            }
            EventBody::Patch(patch_spec) => {
                if !patch_spec.is_array() {
                    return Err(EventError::InvalidSchema(
                        "patch_json must be an array of operations".into(),
                    ));
                }
                let payload = self.store.prepare_payload_from_patch(
                    &aggregate_type,
                    &aggregate_id,
                    &patch_spec,
                )?;
                ensure_payload_size(&payload)?;
                payload
            }
        };

        let schemas = self.schemas();
        let schema_present = match schemas.get(&aggregate_type) {
            Ok(_) => true,
            Err(EventError::SchemaNotFound) => false,
            Err(err) => return Err(err),
        };

        if !schema_present && self.restrict.requires_declared_schema() {
            return Err(EventError::SchemaViolation(
                restrict::strict_mode_missing_schema_message(&aggregate_type),
            ));
        }

        if schema_present {
            schemas.validate_event(&aggregate_type, &event_type, &effective_payload)?;
        }

        let issued_by: Option<ActorClaims> = claims.actor_claims();
        if issued_by.is_none() {
            return Err(EventError::Unauthorized);
        }

        let record = self.store.append(AppendEvent {
            aggregate_type,
            aggregate_id,
            event_type,
            payload: effective_payload,
            metadata,
            issued_by,
            note,
        })?;
        Ok(record)
    }

    fn aggregate_resource(aggregate_type: &str, aggregate_id: &str) -> String {
        format!("aggregate:{}:{}", aggregate_type, aggregate_id)
    }

    fn filter_state_map(
        &self,
        aggregate_type: &str,
        mut state: BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        if let Ok(schema) = self.schemas.get(aggregate_type) {
            if !schema.hidden_fields.is_empty() {
                state.retain(|key, _| !schema.hidden_fields.contains(key));
            }
        }
        state
    }
}

#[derive(Debug, Clone)]
pub struct AppendEventInput {
    pub token: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: Option<Value>,
    pub patch: Option<Value>,
    pub metadata: Option<Value>,
    pub note: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateAggregateInput {
    pub token: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: Value,
    pub metadata: Option<Value>,
    pub note: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SetAggregateArchiveInput {
    pub token: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub archived: bool,
    pub comment: Option<String>,
}

pub(crate) fn normalize_optional_comment(comment: Option<String>) -> Option<String> {
    comment.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}
