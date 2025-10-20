use std::collections::BTreeMap;

use async_graphql::Result as GqlResult;
use async_graphql::{Context, EmptySubscription, Json, Object, Schema, SimpleObject};
use axum::http::HeaderMap;
use serde_json::Value;

use crate::error::EventError;
use crate::server::{AppState, extract_bearer_token, notify_plugins, replicate_events};
use crate::store::{AggregateState, AppendEvent, EventMetadata, EventRecord, payload_to_map};
use crate::token::AccessKind;

#[derive(Clone)]
pub struct GraphqlState {
    app: AppState,
}

impl GraphqlState {
    pub(crate) fn new(app_state: AppState) -> Self {
        Self { app: app_state }
    }
}

pub type EventSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn build_schema(state: GraphqlState) -> EventSchema {
    Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        EmptySubscription,
    )
    .data(state)
    .finish()
}

#[derive(Default)]
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn aggregates(
        &self,
        ctx: &Context<'_>,
        skip: Option<usize>,
        take: Option<usize>,
    ) -> GqlResult<Vec<Aggregate>> {
        let state = ctx.data::<GraphqlState>()?;
        let app = &state.app;
        let skip = skip.unwrap_or(0);
        let mut take = take.unwrap_or(app.list_page_size);
        if take == 0 {
            return Ok(Vec::new());
        }
        if take > app.page_limit {
            take = app.page_limit;
        }
        let aggregates = app.store.aggregates();
        Ok(aggregates
            .into_iter()
            .filter(|aggregate| !app.is_hidden_aggregate(&aggregate.aggregate_type))
            .skip(skip)
            .take(take)
            .map(|aggregate| {
                app.cache_store(&aggregate);
                app.sanitize_aggregate(aggregate).into()
            })
            .collect())
    }

    async fn aggregate(
        &self,
        ctx: &Context<'_>,
        aggregate_type: String,
        aggregate_id: String,
    ) -> GqlResult<Option<Aggregate>> {
        let state = ctx.data::<GraphqlState>()?;
        let app = &state.app;
        if app.is_hidden_aggregate(&aggregate_type) {
            return Ok(None);
        }
        match app.load_aggregate(&aggregate_type, &aggregate_id) {
            Ok(state) => Ok(Some(state.into())),
            Err(crate::error::EventError::AggregateNotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn aggregate_events(
        &self,
        ctx: &Context<'_>,
        aggregate_type: String,
        aggregate_id: String,
        skip: Option<usize>,
        take: Option<usize>,
    ) -> GqlResult<Vec<Event>> {
        let state = ctx.data::<GraphqlState>()?;
        let app = &state.app;
        if app.is_hidden_aggregate(&aggregate_type) {
            return Err(EventError::AggregateNotFound.into());
        }
        let skip = skip.unwrap_or(0);
        let mut take = take.unwrap_or(app.page_limit);
        if take == 0 {
            return Ok(Vec::new());
        }
        if take > app.page_limit {
            take = app.page_limit;
        }

        let events = app.store.list_events(&aggregate_type, &aggregate_id)?;
        let events = events
            .into_iter()
            .skip(skip)
            .take(take)
            .map(Into::into)
            .collect();
        Ok(events)
    }

    async fn verify_aggregate(
        &self,
        ctx: &Context<'_>,
        aggregate_type: String,
        aggregate_id: String,
    ) -> GqlResult<VerifyResult> {
        let state = ctx.data::<GraphqlState>()?;
        let app = &state.app;
        if app.is_hidden_aggregate(&aggregate_type) {
            return Err(EventError::AggregateNotFound.into());
        }
        let merkle = app.store.verify(&aggregate_type, &aggregate_id)?;
        Ok(VerifyResult {
            merkle_root: merkle,
        })
    }
}

#[derive(Default)]
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn append_event(&self, ctx: &Context<'_>, input: AppendEventInput) -> GqlResult<Event> {
        let state = ctx.data::<GraphqlState>()?;
        let app = &state.app;
        let headers = ctx
            .data::<HeaderMap>()
            .map_err(|_| async_graphql::Error::from(EventError::Unauthorized))?;
        let token = extract_bearer_token(headers)
            .ok_or_else(|| async_graphql::Error::from(EventError::Unauthorized))?;
        let claims = app.tokens.authorize(&token, AccessKind::Write)?.into();

        let payload_map = payload_to_map(&input.payload);
        if app.restrict {
            app.schemas
                .validate_event(&input.aggregate_type, &input.event_type, &payload_map)?;
        }

        let aggregate_type = input.aggregate_type.clone();
        let aggregate_id = input.aggregate_id.clone();
        let record = app.store.append(AppendEvent {
            aggregate_type: input.aggregate_type.clone(),
            aggregate_id: input.aggregate_id.clone(),
            event_type: input.event_type.clone(),
            payload: input.payload.clone(),
            issued_by: Some(claims),
        })?;

        notify_plugins(app, &record);
        replicate_events(app, std::slice::from_ref(&record));
        app.refresh_cached_aggregate(&aggregate_type, &aggregate_id);

        Ok(record.into())
    }
}

#[derive(async_graphql::InputObject)]
struct AppendEventInput {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: serde_json::Value,
}

#[derive(async_graphql::SimpleObject)]
struct VerifyResult {
    merkle_root: String,
}

#[derive(SimpleObject)]
struct Aggregate {
    aggregate_type: String,
    aggregate_id: String,
    version: u64,
    state: Json<BTreeMap<String, String>>,
    merkle_root: String,
    archived: bool,
}

impl From<AggregateState> for Aggregate {
    fn from(value: AggregateState) -> Self {
        Self {
            aggregate_type: value.aggregate_type,
            aggregate_id: value.aggregate_id,
            version: value.version,
            state: Json(value.state),
            merkle_root: value.merkle_root,
            archived: value.archived,
        }
    }
}

#[derive(SimpleObject)]
struct Event {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    version: u64,
    payload: Json<Value>,
    metadata: EventMetadataObject,
    hash: String,
    merkle_root: String,
}

impl From<EventRecord> for Event {
    fn from(value: EventRecord) -> Self {
        Self {
            aggregate_type: value.aggregate_type,
            aggregate_id: value.aggregate_id,
            event_type: value.event_type,
            version: value.version,
            payload: Json(value.payload),
            metadata: value.metadata.into(),
            hash: value.hash,
            merkle_root: value.merkle_root,
        }
    }
}

#[derive(SimpleObject)]
struct EventMetadataObject {
    event_id: String,
    created_at: String,
    issued_by: Option<ActorClaimsObject>,
}

impl From<EventMetadata> for EventMetadataObject {
    fn from(value: EventMetadata) -> Self {
        Self {
            event_id: value.event_id.to_string(),
            created_at: value.created_at.to_rfc3339(),
            issued_by: value.issued_by.map(Into::into),
        }
    }
}

#[derive(SimpleObject)]
struct ActorClaimsObject {
    group: String,
    user: String,
}

impl From<crate::store::ActorClaims> for ActorClaimsObject {
    fn from(value: crate::store::ActorClaims) -> Self {
        Self {
            group: value.group,
            user: value.user,
        }
    }
}
