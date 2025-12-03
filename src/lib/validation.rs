use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;

use crate::{
    error::{EventError, Result},
    schema::SchemaManager,
};

pub const MAX_AGGREGATE_ID_LENGTH: usize = 128;
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
pub const MAX_EVENT_METADATA_BYTES: usize = 64 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedEventType {
    pub normalized: String,
    pub original: Option<String>,
}

static SNAKE_CASE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z][a-z0-9_]*$").expect("valid snake_case regex"));
static EVENT_TYPE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*$").expect("valid event_type regex")
});
static AGGREGATE_ID_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[A-Za-z0-9](?:[A-Za-z0-9_-]{0,127})?$").expect("valid aggregate_id regex")
});
static METADATA_KEY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^@[A-Za-z0-9][A-Za-z0-9_-]*$").expect("valid metadata key regex"));

pub fn ensure_snake_case(label: &str, value: &str) -> Result<()> {
    if SNAKE_CASE_RE.is_match(value) {
        Ok(())
    } else {
        Err(EventError::InvalidSchema(format!(
            "{label} must be lowercase snake_case"
        )))
    }
}

pub fn normalize_event_type(value: &str) -> Result<NormalizedEventType> {
    if EVENT_TYPE_RE.is_match(value) {
        let normalized = value.replace('.', "_");
        let original = if normalized != value {
            Some(value.to_string())
        } else {
            None
        };
        Ok(NormalizedEventType {
            normalized,
            original,
        })
    } else {
        Err(EventError::InvalidSchema(
            "event_type must be lowercase snake_case; dots may be used as separators".into(),
        ))
    }
}

pub fn ensure_aggregate_id(value: &str) -> Result<()> {
    if value.trim() != value {
        return Err(EventError::InvalidSchema(
            "aggregate_id cannot include leading or trailing whitespace".into(),
        ));
    }
    if value.is_empty() {
        return Err(EventError::InvalidSchema(
            "aggregate_id must not be empty".into(),
        ));
    }
    if value.len() > MAX_AGGREGATE_ID_LENGTH {
        return Err(EventError::InvalidSchema(format!(
            "aggregate_id cannot exceed {} characters",
            MAX_AGGREGATE_ID_LENGTH
        )));
    }
    if !AGGREGATE_ID_RE.is_match(value) {
        return Err(EventError::InvalidSchema(
            "aggregate_id may only contain letters, numbers, underscores, or hyphens".into(),
        ));
    }
    Ok(())
}

pub fn ensure_payload_size(payload: &Value) -> Result<()> {
    let size = serde_json::to_vec(payload)
        .map_err(|err| EventError::Serialization(err.to_string()))?
        .len();
    if size > MAX_EVENT_PAYLOAD_BYTES {
        return Err(EventError::InvalidSchema(format!(
            "payload_json exceeds maximum size of {} bytes",
            MAX_EVENT_PAYLOAD_BYTES
        )));
    }
    Ok(())
}

pub fn ensure_first_event_rule(_is_new_aggregate: bool, _event_type: &str) -> Result<()> {
    Ok(())
}

pub fn ensure_schema_declared(schemas: &SchemaManager, aggregate_type: &str) -> Result<()> {
    match schemas.get(aggregate_type) {
        Ok(_) => Ok(()),
        Err(EventError::SchemaNotFound) => Err(EventError::SchemaViolation(
            missing_schema_message(aggregate_type),
        )),
        Err(err) => Err(err),
    }
}

pub fn missing_schema_message(aggregate: &str) -> String {
    format!(
        "aggregate {} requires a schema before events can be appended",
        aggregate
    )
}

pub fn ensure_metadata_extensions(metadata: &Value) -> Result<()> {
    let object = metadata
        .as_object()
        .ok_or_else(|| EventError::InvalidSchema("metadata must be a JSON object".into()))?;

    for key in object.keys() {
        if !METADATA_KEY_RE.is_match(key) {
            return Err(EventError::InvalidSchema(format!(
                "metadata key '{}' must start with '@' followed by letters, numbers, underscores, or hyphens",
                key
            )));
        }
    }

    let size = serde_json::to_vec(metadata)
        .map_err(|err| EventError::Serialization(err.to_string()))?
        .len();
    if size > MAX_EVENT_METADATA_BYTES {
        return Err(EventError::InvalidSchema(format!(
            "metadata exceeds maximum size of {} bytes",
            MAX_EVENT_METADATA_BYTES
        )));
    }

    Ok(())
}

/// Convert a dot-separated reference path into a JSON Pointer string.
pub fn json_pointer_from_path(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path.replace('.', "/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snake_case_validation_allows_valid_names() {
        ensure_snake_case("aggregate_type", "order_created").expect("valid snake case");
    }

    #[test]
    fn snake_case_validation_rejects_invalid_names() {
        let err = ensure_snake_case("event_type", "OrderCreated").unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
    }

    #[test]
    fn event_type_normalization_accepts_dots() {
        let normalized = normalize_event_type("person.created").expect("dot segments normalize");
        assert_eq!(normalized.normalized, "person_created");
        assert_eq!(normalized.original.as_deref(), Some("person.created"));
    }

    #[test]
    fn event_type_normalization_rejects_uppercase() {
        let err = normalize_event_type("Person.Created").unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
    }

    #[test]
    fn event_type_normalization_rejects_mixed_case_with_underscore() {
        let err = normalize_event_type("Patient_Created").unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
    }

    #[test]
    fn event_type_normalization_rejects_hyphenated_segments() {
        let err = normalize_event_type("patient-Created").unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
    }

    #[test]
    fn event_type_normalization_passes_through_snake_case() {
        let normalized = normalize_event_type("order_created").expect("snake_case passes through");
        assert_eq!(normalized.normalized, "order_created");
        assert_eq!(normalized.original, None);
    }

    #[test]
    fn aggregate_id_validation_rejects_whitespace() {
        let err = ensure_aggregate_id(" bad").unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
    }

    #[test]
    fn payload_size_enforces_limit() {
        let oversized = "x".repeat(MAX_EVENT_PAYLOAD_BYTES + 1);
        let err = ensure_payload_size(&json!(oversized)).unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
    }

    #[test]
    fn metadata_extensions_validate_keys_and_size() {
        ensure_metadata_extensions(&json!({"@plugin": {"enabled": true}})).expect("valid metadata");

        let err = ensure_metadata_extensions(&json!({"plugin": {"enabled": true}})).unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));

        let huge_value = "x".repeat(MAX_EVENT_METADATA_BYTES + 1);
        let err = ensure_metadata_extensions(&json!({"@plugin": huge_value})).unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
    }
}
