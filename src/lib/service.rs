use std::{collections::BTreeMap, sync::Arc};

use crate::{
    error::{EventError, Result},
    filter::FilterExpr,
    restrict::{self, RestrictMode},
    schema::SchemaManager,
    store::{
        ActorClaims, AggregateState, AppendEvent, EventRecord, EventStore, select_state_field,
    },
    token::TokenManager,
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
}

impl CoreContext {
    pub fn new(
        tokens: Arc<TokenManager>,
        schemas: Arc<SchemaManager>,
        store: Arc<EventStore>,
        restrict: RestrictMode,
        list_page_size: usize,
        page_limit: usize,
    ) -> Self {
        Self {
            tokens,
            schemas,
            store,
            restrict,
            list_page_size,
            page_limit,
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

    pub fn list_aggregates(
        &self,
        skip: usize,
        take: Option<usize>,
        filter: Option<FilterExpr>,
    ) -> Vec<AggregateState> {
        let mut effective_take = take.unwrap_or(self.list_page_size);
        if effective_take == 0 {
            return Vec::new();
        }
        if effective_take > self.page_limit {
            effective_take = self.page_limit;
        }

        let filter_ref = filter.as_ref();
        self.store
            .aggregates_paginated_with_transform(skip, Some(effective_take), |aggregate| {
                if self.is_hidden_aggregate(&aggregate.aggregate_type) {
                    return None;
                }
                let sanitized = self.sanitize_aggregate(aggregate);
                if let Some(expr) = filter_ref {
                    if !expr.matches_aggregate(&sanitized) {
                        return None;
                    }
                }
                Some(sanitized)
            })
    }

    pub fn get_aggregate(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<AggregateState>> {
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
        aggregate_type: &str,
        aggregate_id: &str,
        fields: &[String],
    ) -> Result<Option<BTreeMap<String, Value>>> {
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
        aggregate_type: &str,
        aggregate_id: &str,
        skip: usize,
        take: Option<usize>,
    ) -> Result<Vec<EventRecord>> {
        if self.is_hidden_aggregate(aggregate_type) {
            return Err(EventError::AggregateNotFound);
        }

        let mut effective_take = take.unwrap_or(self.page_limit);
        if effective_take == 0 {
            return Ok(Vec::new());
        }
        if effective_take > self.page_limit {
            effective_take = self.page_limit;
        }

        let events = self.store.list_events_paginated(
            aggregate_type,
            aggregate_id,
            skip,
            Some(effective_take),
        )?;
        Ok(events)
    }

    pub fn verify_aggregate(&self, aggregate_type: &str, aggregate_id: &str) -> Result<String> {
        if self.is_hidden_aggregate(aggregate_type) {
            return Err(EventError::AggregateNotFound);
        }
        self.store.verify(aggregate_type, aggregate_id)
    }

    pub fn append_event(&self, input: AppendEventInput) -> Result<EventRecord> {
        let AppendEventInput {
            token,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            patch,
            metadata,
            note,
            require_existing,
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
        if (require_existing || is_patch_event) && is_new_aggregate {
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
    pub require_existing: bool,
}
