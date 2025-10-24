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

static SNAKE_CASE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z][a-z0-9_]*$").expect("valid snake_case regex"));
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

pub fn ensure_first_event_rule(is_new_aggregate: bool, event_type: &str) -> Result<()> {
    if is_new_aggregate && !event_type.ends_with("_created") {
        return Err(EventError::InvalidSchema(
            "first event for a new aggregate must end with '_created'".into(),
        ));
    }
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
    fn first_event_rule_requires_created_suffix() {
        let err = ensure_first_event_rule(true, "order_updated").unwrap_err();
        assert!(matches!(err, EventError::InvalidSchema(_)));
        ensure_first_event_rule(true, "order_created").expect("created suffix required");
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
