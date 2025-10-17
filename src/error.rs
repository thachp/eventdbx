use std::io;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, EventfulError>;

#[derive(Debug, Error)]
pub enum EventfulError {
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
    #[error("schema already exists")]
    SchemaExists,
    #[error("schema not found")]
    SchemaNotFound,
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<toml::de::Error> for EventfulError {
    fn from(err: toml::de::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<toml::ser::Error> for EventfulError {
    fn from(err: toml::ser::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<serde_json::Error> for EventfulError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    message: &'a str,
}

impl IntoResponse for EventfulError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Config(_) => StatusCode::BAD_REQUEST,
            Self::InvalidToken | Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::TokenExpired => StatusCode::UNAUTHORIZED,
            Self::TokenLimitReached => StatusCode::FORBIDDEN,
            Self::AggregateNotFound | Self::SchemaNotFound => StatusCode::NOT_FOUND,
            Self::SchemaExists => StatusCode::CONFLICT,
            Self::Storage(_) | Self::Serialization(_) | Self::Io(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::InvalidSchema(_) => StatusCode::BAD_REQUEST,
        };

        let message = self.to_string();
        (status, Json(ErrorBody { message: &message })).into_response()
    }
}
