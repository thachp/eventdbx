use std::{collections::BTreeMap, sync::Arc};

use crate::{
    error::{EventError, Result},
    restrict::RestrictMode,
    schema::SchemaManager,
    store::{ActorClaims, AggregateState, AppendEvent, EventRecord, EventStore},
    token::TokenManager,
    validation::{
        ensure_aggregate_id, ensure_first_event_rule, ensure_metadata_extensions,
        ensure_payload_size, ensure_schema_declared, ensure_snake_case,
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

    pub fn list_aggregates(&self, skip: usize, take: Option<usize>) -> Vec<AggregateState> {
        let mut effective_take = take.unwrap_or(self.list_page_size);
        if effective_take == 0 {
            return Vec::new();
        }
        if effective_take > self.page_limit {
            effective_take = self.page_limit;
        }

        self.store
            .aggregates_paginated(skip, Some(effective_take))
            .into_iter()
            .filter(|aggregate| !self.is_hidden_aggregate(&aggregate.aggregate_type))
            .map(|aggregate| self.sanitize_aggregate(aggregate))
            .collect()
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

        let events = self
            .store
            .list_events(aggregate_type, aggregate_id)?
            .into_iter()
            .skip(skip)
            .take(effective_take)
            .collect();
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
        } = input;

        ensure_snake_case("aggregate_type", &aggregate_type)?;
        ensure_snake_case("event_type", &event_type)?;
        ensure_aggregate_id(&aggregate_id)?;
        if let Some(ref metadata) = metadata {
            ensure_metadata_extensions(metadata)?;
        }

        let is_new_aggregate = match self
            .store
            .aggregate_version(&aggregate_type, &aggregate_id)?
        {
            Some(version) if version > 0 => false,
            Some(_) | None => true,
        };
        ensure_first_event_rule(is_new_aggregate, &event_type)?;

        if payload.is_some() && patch.is_some() {
            return Err(EventError::InvalidSchema(
                "payload_json must be empty when patch_json is provided".into(),
            ));
        }
        if payload.is_none() && patch.is_none() {
            return Err(EventError::InvalidSchema(
                "payload_json must be provided when patch_json is absent".into(),
            ));
        }

        let resource = format!("aggregate:{}:{}", aggregate_type, aggregate_id);
        let claims =
            self.tokens()
                .authorize_action(&token, "aggregate.append", Some(resource.as_str()))?;

        let effective_payload = match (payload, patch) {
            (Some(payload), None) => payload,
            (None, Some(patch_ops)) => {
                self.store
                    .prepare_payload_from_patch(&aggregate_type, &aggregate_id, &patch_ops)?
            }
            (Some(_), Some(_)) => unreachable!(),
            (None, None) => unreachable!(),
        };

        ensure_payload_size(&effective_payload)?;

        let schemas = self.schemas();
        ensure_schema_declared(schemas.as_ref(), &aggregate_type)?;
        schemas.validate_event(&aggregate_type, &event_type, &effective_payload)?;

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
}
