use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;
use tonic::{Request, Response, Status, metadata::MetadataMap};

use crate::{
    error::EventError,
    restrict,
    server::{AppState, run_cli_json},
    store::{AggregateState, EventRecord},
    token::AccessKind,
};

pub mod proto {
    tonic::include_proto!("eventdbx.api");
}

use proto::event_service_server::{EventService, EventServiceServer};
use proto::{
    ActorClaims, Aggregate, AppendEventRequest, AppendEventResponse, Event, EventMetadata,
    GetAggregateRequest, GetAggregateResponse, HealthRequest, HealthResponse,
    ListAggregatesRequest, ListAggregatesResponse, ListEventsRequest, ListEventsResponse,
    VerifyAggregateRequest, VerifyAggregateResponse,
};

#[derive(Clone)]
pub struct GrpcApi {
    state: AppState,
}

impl GrpcApi {
    pub(crate) fn new(state: AppState) -> Self {
        Self { state }
    }

    pub(crate) fn into_server(self) -> EventServiceServer<Self> {
        EventServiceServer::new(self)
    }

    fn map_error(err: EventError) -> Status {
        match err {
            EventError::InvalidToken | EventError::Unauthorized => {
                Status::unauthenticated(err.to_string())
            }
            EventError::TokenExpired | EventError::TokenLimitReached => {
                Status::permission_denied(err.to_string())
            }
            EventError::AggregateNotFound | EventError::SchemaNotFound => {
                Status::not_found(err.to_string())
            }
            EventError::SchemaViolation(_)
            | EventError::InvalidSchema(_)
            | EventError::Config(_) => Status::invalid_argument(err.to_string()),
            EventError::SchemaExists | EventError::AggregateArchived => {
                Status::failed_precondition(err.to_string())
            }
            EventError::Storage(_) | EventError::Serialization(_) | EventError::Io(_) => {
                Status::internal(err.to_string())
            }
        }
    }

    fn extract_token(metadata: &MetadataMap) -> Result<String, Status> {
        let value = metadata
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?;
        let value = value
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid authorization metadata"))?;
        let prefix = "Bearer ";
        if let Some(rest) = value.strip_prefix(prefix) {
            Ok(rest.to_string())
        } else {
            Err(Status::unauthenticated(
                "authorization metadata must use Bearer token",
            ))
        }
    }

    fn convert_event(record: EventRecord) -> Result<Event, Status> {
        let metadata = record.metadata.clone();
        let issued_by = metadata.issued_by.map(|claims| ActorClaims {
            group: claims.group,
            user: claims.user,
        });

        let event = Event {
            aggregate_type: record.aggregate_type,
            aggregate_id: record.aggregate_id,
            event_type: record.event_type,
            version: record.version,
            payload_json: serde_json::to_string(&record.payload)
                .map_err(|err| Status::internal(err.to_string()))?,
            metadata: Some(EventMetadata {
                event_id: metadata.event_id.to_string(),
                created_at: metadata.created_at.to_rfc3339(),
                issued_by,
            }),
            hash: record.hash,
            merkle_root: record.merkle_root,
        };
        Ok(event)
    }

    fn sanitize_aggregate(&self, aggregate: AggregateState) -> Aggregate {
        let filtered = self.state.sanitize_aggregate(aggregate);
        Aggregate {
            aggregate_type: filtered.aggregate_type,
            aggregate_id: filtered.aggregate_id,
            version: filtered.version,
            state: filtered.state.into_iter().collect::<HashMap<_, _>>(),
            merkle_root: filtered.merkle_root,
            archived: filtered.archived,
        }
    }
}

#[derive(Deserialize)]
struct VerifyPayload {
    merkle_root: String,
}

#[tonic::async_trait]
impl EventService for GrpcApi {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".to_string(),
        }))
    }

    async fn append_event(
        &self,
        request: Request<AppendEventRequest>,
    ) -> Result<Response<AppendEventResponse>, Status> {
        let metadata = request.metadata();
        let token = Self::extract_token(metadata)?;
        let payload = request.into_inner();

        let payload_value: Value = serde_json::from_str(&payload.payload_json)
            .map_err(|err| Status::invalid_argument(format!("invalid payload_json: {}", err)))?;

        self.state
            .tokens()
            .authorize(&token, AccessKind::Write)
            .map_err(Self::map_error)?;

        let mode = self.state.restrict();
        if mode.enforces_validation() {
            let schemas = self.state.schemas();
            if mode.requires_declared_schema() {
                if let Err(_) = schemas.get(&payload.aggregate_type) {
                    return Err(Self::map_error(EventError::SchemaViolation(
                        restrict::strict_mode_missing_schema_message(&payload.aggregate_type),
                    )));
                }
            }
            if let Err(err) =
                schemas.validate_event(&payload.aggregate_type, &payload.event_type, &payload_value)
            {
                return Err(Self::map_error(err));
            }
        }

        let payload_json = serde_json::to_string(&payload_value)
            .map_err(|err| Status::internal(err.to_string()))?;
        let args = vec![
            "aggregate".to_string(),
            "apply".to_string(),
            payload.aggregate_type.clone(),
            payload.aggregate_id.clone(),
            payload.event_type.clone(),
            "--payload".to_string(),
            payload_json,
        ];

        let record: EventRecord = run_cli_json(args).await.map_err(Self::map_error)?;

        let event = Self::convert_event(record)?;
        Ok(Response::new(AppendEventResponse { event: Some(event) }))
    }

    async fn list_aggregates(
        &self,
        request: Request<ListAggregatesRequest>,
    ) -> Result<Response<ListAggregatesResponse>, Status> {
        let params = request.into_inner();
        let skip = params.skip as usize;
        let mut take = if params.take == 0 {
            self.state.list_page_size()
        } else {
            params.take as usize
        };
        if take == 0 {
            return Ok(Response::new(ListAggregatesResponse {
                aggregates: Vec::new(),
            }));
        }
        if take > self.state.page_limit() {
            take = self.state.page_limit();
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

        let aggregates: Vec<AggregateState> = run_cli_json(args).await.map_err(Self::map_error)?;
        let page = aggregates
            .into_iter()
            .filter(|aggregate| !self.state.is_hidden_aggregate(&aggregate.aggregate_type))
            .map(|aggregate| self.sanitize_aggregate(aggregate))
            .collect();

        Ok(Response::new(ListAggregatesResponse { aggregates: page }))
    }

    async fn get_aggregate(
        &self,
        request: Request<GetAggregateRequest>,
    ) -> Result<Response<GetAggregateResponse>, Status> {
        let params = request.into_inner();
        if self.state.is_hidden_aggregate(&params.aggregate_type) {
            return Err(Status::not_found("aggregate not found"));
        }
        let args = vec![
            "aggregate".to_string(),
            "get".to_string(),
            params.aggregate_type.clone(),
            params.aggregate_id.clone(),
        ];
        let aggregate = match run_cli_json::<AggregateState>(args).await {
            Ok(aggregate) => aggregate,
            Err(EventError::AggregateNotFound) => {
                return Err(Status::not_found("aggregate not found"));
            }
            Err(err) => return Err(Self::map_error(err)),
        };
        let aggregate = self.sanitize_aggregate(aggregate);

        Ok(Response::new(GetAggregateResponse {
            aggregate: Some(aggregate),
        }))
    }

    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsResponse>, Status> {
        let params = request.into_inner();
        if self.state.is_hidden_aggregate(&params.aggregate_type) {
            return Err(Status::not_found("aggregate not found"));
        }
        let skip = params.skip as usize;
        let mut take = if params.take == 0 {
            self.state.page_limit()
        } else {
            params.take as usize
        };
        if take == 0 {
            return Ok(Response::new(ListEventsResponse { events: Vec::new() }));
        }
        if take > self.state.page_limit() {
            take = self.state.page_limit();
        }

        let args = vec![
            "aggregate".to_string(),
            "replay".to_string(),
            params.aggregate_type.clone(),
            params.aggregate_id.clone(),
            "--skip".to_string(),
            skip.to_string(),
            "--take".to_string(),
            take.to_string(),
            "--json".to_string(),
        ];

        let events: Vec<EventRecord> = run_cli_json(args).await.map_err(Self::map_error)?;
        let page = events
            .into_iter()
            .map(Self::convert_event)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListEventsResponse { events: page }))
    }

    async fn verify_aggregate(
        &self,
        request: Request<VerifyAggregateRequest>,
    ) -> Result<Response<VerifyAggregateResponse>, Status> {
        let params = request.into_inner();
        if self.state.is_hidden_aggregate(&params.aggregate_type) {
            return Err(Status::not_found("aggregate not found"));
        }

        let args = vec![
            "aggregate".to_string(),
            "verify".to_string(),
            params.aggregate_type.clone(),
            params.aggregate_id.clone(),
            "--json".to_string(),
        ];
        let response: VerifyPayload = run_cli_json(args).await.map_err(Self::map_error)?;

        Ok(Response::new(VerifyAggregateResponse {
            merkle_root: response.merkle_root,
        }))
    }
}

impl From<EventError> for Status {
    fn from(value: EventError) -> Self {
        GrpcApi::map_error(value)
    }
}
