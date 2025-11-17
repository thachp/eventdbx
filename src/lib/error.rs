use std::io;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, EventError>;

#[derive(Debug, Error)]
pub enum EventError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("token not found or inactive")]
    InvalidToken,
    #[error("token has expired")]
    TokenExpired,
    #[error("token write limit reached")]
    TokenLimitReached,
    #[error("unauthorized")]
    Unauthorized,
    #[error("aggregate not found")]
    AggregateNotFound,
    #[error("aggregate is archived and cannot accept new events")]
    AggregateArchived,
    #[error("schema already exists")]
    SchemaExists,
    #[error("schema not found")]
    SchemaNotFound,
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    #[error("schema violation: {0}")]
    SchemaViolation(String),
    #[error("invalid cursor: {0}")]
    InvalidCursor(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("tenant quota exceeded: {0}")]
    TenantQuotaExceeded(String),
}

impl From<toml::de::Error> for EventError {
    fn from(err: toml::de::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<toml::ser::Error> for EventError {
    fn from(err: toml::ser::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<serde_json::Error> for EventError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    message: &'a str,
}

impl IntoResponse for EventError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Config(_) => StatusCode::BAD_REQUEST,
            Self::InvalidToken | Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::TokenExpired => StatusCode::UNAUTHORIZED,
            Self::TokenLimitReached | Self::AggregateArchived | Self::TenantQuotaExceeded(_) => {
                StatusCode::FORBIDDEN
            }
            Self::AggregateNotFound | Self::SchemaNotFound => StatusCode::NOT_FOUND,
            Self::SchemaExists => StatusCode::CONFLICT,
            Self::SchemaViolation(_) => StatusCode::BAD_REQUEST,
            Self::InvalidCursor(_) => StatusCode::BAD_REQUEST,
            Self::Storage(_) | Self::Serialization(_) | Self::Io(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::InvalidSchema(_) => StatusCode::BAD_REQUEST,
        };

        let message = self.to_string();
        (status, Json(ErrorBody { message: &message })).into_response()
    }
}
