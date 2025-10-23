use std::collections::BTreeMap;

use async_graphql::Result as GqlResult;
use async_graphql::{Context, EmptySubscription, Json, Object, Schema, SimpleObject};
use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::Value;

use crate::error::EventError;
use crate::restrict;
use crate::server::{AppState, extract_bearer_token, run_cli_json};
use crate::store::{AggregateState, EventMetadata, EventRecord};

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
        let mut take = take.unwrap_or(app.list_page_size());
        if take == 0 {
            return Ok(Vec::new());
        }
        if take > app.page_limit() {
            take = app.page_limit();
        }
        let args = vec![
            "aggregate".to_string(),
            "list".to_string(),
            "--skip".to_string(),
            skip.to_string(),
            "--take".to_string(),
            take.to_string(),
            "--json".to_string(),
        ];
        let aggregates: Vec<AggregateState> = run_cli_json(args)
            .await
            .map_err(async_graphql::Error::from)?;
        Ok(aggregates
            .into_iter()
            .filter(|aggregate| !app.is_hidden_aggregate(&aggregate.aggregate_type))
            .map(|aggregate| app.sanitize_aggregate(aggregate).into())
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
        let args = vec![
            "aggregate".to_string(),
            "get".to_string(),
            aggregate_type.clone(),
            aggregate_id.clone(),
        ];
        match run_cli_json::<AggregateState>(args).await {
            Ok(state) => Ok(Some(app.sanitize_aggregate(state).into())),
            Err(EventError::AggregateNotFound) => Ok(None),
            Err(err) => Err(async_graphql::Error::from(err)),
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
        let mut take = take.unwrap_or(app.page_limit());
        if take == 0 {
            return Ok(Vec::new());
        }
        if take > app.page_limit() {
            take = app.page_limit();
        }

        let args = vec![
            "aggregate".to_string(),
            "replay".to_string(),
            aggregate_type.clone(),
            aggregate_id.clone(),
            "--skip".to_string(),
            skip.to_string(),
            "--take".to_string(),
            take.to_string(),
            "--json".to_string(),
        ];
        let events: Vec<EventRecord> = run_cli_json(args)
            .await
            .map_err(async_graphql::Error::from)?;
        let events = events.into_iter().map(Into::into).collect();
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
        let args = vec![
            "aggregate".to_string(),
            "verify".to_string(),
            aggregate_type,
            aggregate_id,
            "--json".to_string(),
        ];
        let response: VerifyPayload = run_cli_json(args)
            .await
            .map_err(async_graphql::Error::from)?;
        Ok(VerifyResult {
            merkle_root: response.merkle_root,
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
        let resource = format!("aggregate:{}:{}", input.aggregate_type, input.aggregate_id);
        let _claims = app
            .tokens()
            .authorize_action(&token, "aggregate.append", Some(resource.as_str()))
            .map_err(async_graphql::Error::from)?;

        let mode = app.restrict();
        if mode.enforces_validation() {
            let schemas = app.schemas();
            if mode.requires_declared_schema() {
                if let Err(_) = schemas.get(&input.aggregate_type) {
                    return Err(async_graphql::Error::from(EventError::SchemaViolation(
                        restrict::strict_mode_missing_schema_message(&input.aggregate_type),
                    )));
                }
            }
            if let Some(payload) = &input.payload {
                schemas.validate_event(&input.aggregate_type, &input.event_type, payload)?;
            }
        }

        let mut args = vec![
            "aggregate".to_string(),
            "apply".to_string(),
            input.aggregate_type.clone(),
            input.aggregate_id.clone(),
            input.event_type.clone(),
        ];
        match (&input.payload, &input.patch) {
            (Some(payload), None) => {
                let payload_json = serde_json::to_string(payload).map_err(|err| {
                    async_graphql::Error::from(EventError::Serialization(err.to_string()))
                })?;
                args.push("--payload".to_string());
                args.push(payload_json);
            }
            (None, Some(patch)) => {
                let patch_json = serde_json::to_string(patch).map_err(|err| {
                    async_graphql::Error::from(EventError::Serialization(err.to_string()))
                })?;
                args.push("--patch".to_string());
                args.push(patch_json);
            }
            (Some(_), Some(_)) => {
                return Err(async_graphql::Error::from(EventError::InvalidSchema(
                    "payload and patch cannot both be provided".into(),
                )));
            }
            (None, None) => {
                return Err(async_graphql::Error::from(EventError::InvalidSchema(
                    "either payload or patch must be provided".into(),
                )));
            }
        }
        if let Some(note) = input.note.as_ref() {
            args.push("--note".to_string());
            args.push(note.clone());
        }
        let record: EventRecord = run_cli_json(args)
            .await
            .map_err(async_graphql::Error::from)?;

        Ok(record.into())
    }
}

#[derive(async_graphql::InputObject)]
struct AppendEventInput {
    aggregate_type: String,
    aggregate_id: String,
    event_type: String,
    payload: Option<serde_json::Value>,
    patch: Option<serde_json::Value>,
    note: Option<String>,
}

#[derive(async_graphql::SimpleObject)]
struct VerifyResult {
    merkle_root: String,
}

#[derive(Deserialize)]
struct VerifyPayload {
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
    note: Option<String>,
}

impl From<EventMetadata> for EventMetadataObject {
    fn from(value: EventMetadata) -> Self {
        Self {
            event_id: value.event_id.to_string(),
            created_at: value.created_at.to_rfc3339(),
            issued_by: value.issued_by.map(Into::into),
            note: value.note,
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
