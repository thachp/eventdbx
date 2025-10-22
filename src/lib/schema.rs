use std::{
    collections::BTreeMap,
    fmt,
    fs,
    path::PathBuf,
    str::FromStr,
};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum ColumnType {
    Integer,
    Float,
    Decimal { precision: u8, scale: u8 },
    Boolean,
    Text,
    Timestamp,
    Date,
    Json,
    Binary,
}

impl ColumnType {
    fn as_str(&self) -> String {
        match self {
            ColumnType::Integer => "integer".to_string(),
            ColumnType::Float => "float".to_string(),
            ColumnType::Decimal { precision, scale } => {
                format!("decimal({},{})", precision, scale)
            }
            ColumnType::Boolean => "boolean".to_string(),
            ColumnType::Text => "text".to_string(),
            ColumnType::Timestamp => "timestamp".to_string(),
            ColumnType::Date => "date".to_string(),
            ColumnType::Json => "json".to_string(),
            ColumnType::Binary => "binary".to_string(),
        }
    }

    fn parse_decimal(value: &str) -> Result<Self, ColumnTypeParseError> {
        let start = value.find('(').ok_or_else(|| ColumnTypeParseError(value.to_string()))?;
        let end = value
            .rfind(')')
            .filter(|pos| *pos > start)
            .ok_or_else(|| ColumnTypeParseError(value.to_string()))?;
        let inner = &value[start + 1..end];
        let mut parts = inner.split(',').map(|part| part.trim());
        let precision = parts
            .next()
            .ok_or_else(|| ColumnTypeParseError(value.to_string()))?
            .parse()
            .map_err(|_| ColumnTypeParseError(value.to_string()))?;
        let scale = parts
            .next()
            .ok_or_else(|| ColumnTypeParseError(value.to_string()))?
            .parse()
            .map_err(|_| ColumnTypeParseError(value.to_string()))?;

        if parts.next().is_some() {
            return Err(ColumnTypeParseError(value.to_string()));
        }

        Ok(ColumnType::Decimal { precision, scale })
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<ColumnType> for String {
    fn from(value: ColumnType) -> Self {
        value.as_str()
    }
}

impl TryFrom<String> for ColumnType {
    type Error = ColumnTypeParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ColumnType::from_str(&value)
    }
}

impl FromStr for ColumnType {
    type Err = ColumnTypeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(ColumnTypeParseError(value.to_string()));
        }
        let lower = trimmed.to_ascii_lowercase();
        match lower.as_str() {
            "integer" | "int" => Ok(ColumnType::Integer),
            "float" | "double" => Ok(ColumnType::Float),
            "boolean" | "bool" => Ok(ColumnType::Boolean),
            "text" | "string" => Ok(ColumnType::Text),
            "timestamp" => Ok(ColumnType::Timestamp),
            "date" => Ok(ColumnType::Date),
            "json" => Ok(ColumnType::Json),
            "binary" | "bytes" => Ok(ColumnType::Binary),
            _ if lower.starts_with("decimal(") || lower.starts_with("numeric(") => {
                ColumnType::parse_decimal(trimmed)
            }
            _ => Err(ColumnTypeParseError(value.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnTypeParseError(String);

impl fmt::Display for ColumnTypeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid column type '{}'", self.0)
    }
}

impl std::error::Error for ColumnTypeParseError {}

use super::error::{EventError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventSchema {
    pub fields: Vec<String>,
}

impl EventSchema {
    fn ensure_sorted(&mut self) {
        self.fields.sort();
        self.fields.dedup();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AggregateSchema {
    pub aggregate: String,
    pub snapshot_threshold: Option<u64>,
    pub locked: bool,
    pub field_locks: Vec<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub hidden_fields: Vec<String>,
    #[serde(default)]
    pub column_types: BTreeMap<String, ColumnType>,
    pub events: BTreeMap<String, EventSchema>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AggregateSchema {
    fn ensure_sorted(&mut self) {
        self.field_locks.sort();
        self.field_locks.dedup();
        self.hidden_fields.sort();
        self.hidden_fields.dedup();
        for schema in self.events.values_mut() {
            schema.ensure_sorted();
        }
    }
}

#[derive(Debug)]
pub struct SchemaManager {
    path: PathBuf,
    items: RwLock<BTreeMap<String, AggregateSchema>>,
}

#[derive(Debug)]
pub struct CreateSchemaInput {
    pub aggregate: String,
    pub events: Vec<String>,
    pub snapshot_threshold: Option<u64>,
}

#[derive(Debug, Default)]
pub struct SchemaUpdate {
    pub snapshot_threshold: Option<Option<u64>>,
    pub locked: Option<bool>,
    pub field_lock: Option<(String, bool)>,
    pub event_add_fields: BTreeMap<String, Vec<String>>,
    pub event_remove_fields: BTreeMap<String, Vec<String>>,
    pub hidden: Option<bool>,
    pub hidden_field: Option<(String, bool)>,
    pub column_type: Option<(String, Option<ColumnType>)>,
}

impl SchemaManager {
    pub fn load(path: PathBuf) -> Result<Self> {
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&path, "{}")?;
        }

        let contents = fs::read_to_string(&path)?;
        let map: BTreeMap<String, AggregateSchema> = if contents.trim().is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_str(&contents)?
        };

        Ok(Self {
            path,
            items: RwLock::new(map),
        })
    }

    pub fn create(&self, input: CreateSchemaInput) -> Result<AggregateSchema> {
        if input.aggregate.trim().is_empty() {
            return Err(EventError::InvalidSchema(
                "aggregate name must be provided".into(),
            ));
        }
        if input.events.is_empty() {
            return Err(EventError::InvalidSchema(
                "at least one event must be provided".into(),
            ));
        }

        let mut items = self.items.write();
        let aggregate_key = input.aggregate.clone();
        if items.contains_key(&aggregate_key) {
            return Err(EventError::SchemaExists);
        }

        let now = Utc::now();
        let mut events = BTreeMap::new();
        for event in input.events {
            if event.trim().is_empty() {
                return Err(EventError::InvalidSchema(
                    "event names cannot be empty".into(),
                ));
            }
            events.insert(event, EventSchema { fields: Vec::new() });
        }

        let mut schema = AggregateSchema {
            aggregate: aggregate_key.clone(),
            snapshot_threshold: input.snapshot_threshold,
            locked: false,
            field_locks: Vec::new(),
            hidden: false,
            hidden_fields: Vec::new(),
            column_types: BTreeMap::new(),
            events,
            created_at: now,
            updated_at: now,
        };
        schema.ensure_sorted();

        items.insert(aggregate_key.clone(), schema.clone());
        self.persist(&items)?;

        Ok(schema)
    }

    pub fn update(&self, aggregate: &str, update: SchemaUpdate) -> Result<AggregateSchema> {
        let mut items = self.items.write();
        {
            let schema = items.get_mut(aggregate).ok_or(EventError::SchemaNotFound)?;

            if let Some(snapshot) = update.snapshot_threshold {
                schema.snapshot_threshold = snapshot;
            }
            if let Some(locked) = update.locked {
                schema.locked = locked;
            }
            if let Some((field, lock)) = update.field_lock {
                if field.trim().is_empty() {
                    return Err(EventError::InvalidSchema(
                        "field name cannot be empty".into(),
                    ));
                }
                if lock {
                    if !schema.field_locks.contains(&field) {
                        schema.field_locks.push(field);
                    }
                } else {
                    schema.field_locks.retain(|item| item != &field);
                }
            }

            if let Some(hidden) = update.hidden {
                schema.hidden = hidden;
            }

            if let Some((field, hide)) = update.hidden_field {
                if field.trim().is_empty() {
                    return Err(EventError::InvalidSchema(
                        "field name cannot be empty".into(),
                    ));
                }
                if hide {
                    if !schema.hidden_fields.contains(&field) {
                        schema.hidden_fields.push(field);
                    }
                } else {
                    schema.hidden_fields.retain(|item| item != &field);
                }
            }

            if let Some((field, data_type)) = update.column_type {
                if field.trim().is_empty() {
                    return Err(EventError::InvalidSchema(
                        "field name cannot be empty".into(),
                    ));
                }
                match data_type {
                    Some(value) => {
                        schema.column_types.insert(field, value);
                    }
                    None => {
                        schema.column_types.remove(&field);
                    }
                }
            }

            for (event, fields) in update.event_add_fields {
                let schema_event = schema
                    .events
                    .entry(event.clone())
                    .or_insert(EventSchema { fields: Vec::new() });
                for field in fields {
                    if field.trim().is_empty() {
                        return Err(EventError::InvalidSchema(
                            "field names cannot be empty".into(),
                        ));
                    }
                    schema_event.fields.push(field);
                }
            }

            for (event, fields) in update.event_remove_fields {
                if let Some(schema_event) = schema.events.get_mut(&event) {
                    schema_event
                        .fields
                        .retain(|existing| !fields.contains(existing));
                }
            }

            schema.ensure_sorted();
            schema.updated_at = Utc::now();
        }

        let result = items
            .get(aggregate)
            .cloned()
            .ok_or(EventError::SchemaNotFound)?;
        self.persist(&items)?;

        Ok(result)
    }

    pub fn list(&self) -> Vec<AggregateSchema> {
        self.items.read().values().cloned().collect()
    }

    pub fn get(&self, aggregate: &str) -> Result<AggregateSchema> {
        self.items
            .read()
            .get(aggregate)
            .cloned()
            .ok_or(EventError::SchemaNotFound)
    }

    pub fn should_snapshot(&self, aggregate: &str, version: u64) -> bool {
        if version == 0 {
            return false;
        }
        let items = self.items.read();
        items
            .get(aggregate)
            .and_then(|schema| schema.snapshot_threshold)
            .map(|threshold| threshold > 0 && version % threshold == 0)
            .unwrap_or(false)
    }

    pub fn snapshot(&self) -> BTreeMap<String, AggregateSchema> {
        self.items.read().clone()
    }

    pub fn replace_all(&self, mut items: BTreeMap<String, AggregateSchema>) -> Result<()> {
        for schema in items.values_mut() {
            schema.ensure_sorted();
        }

        {
            let mut guard = self.items.write();
            *guard = items.clone();
        }

        self.persist(&items)?;
        Ok(())
    }

    pub fn validate_event(
        &self,
        aggregate: &str,
        event_type: &str,
        payload: &BTreeMap<String, String>,
    ) -> Result<()> {
        let items = self.items.read();
        let Some(schema) = items.get(aggregate) else {
            return Ok(());
        };

        if schema.locked {
            return Err(EventError::SchemaViolation(format!(
                "aggregate {} is locked for updates",
                aggregate
            )));
        }

        for key in payload.keys() {
            if schema.field_locks.contains(key) {
                return Err(EventError::SchemaViolation(format!(
                    "field {} is locked for aggregate {}",
                    key, aggregate
                )));
            }
        }

        let event_schema = schema.events.get(event_type).ok_or_else(|| {
            EventError::SchemaViolation(format!(
                "event {} is not defined for aggregate {}",
                event_type, aggregate
            ))
        })?;

        if !event_schema.fields.is_empty() {
            for required in &event_schema.fields {
                if !payload.contains_key(required) {
                    return Err(EventError::SchemaViolation(format!(
                        "missing required field {} for event {}",
                        required, event_type
                    )));
                }
            }

            for key in payload.keys() {
                if !event_schema.fields.contains(key) {
                    return Err(EventError::SchemaViolation(format!(
                        "field {} is not permitted for event {}",
                        key, event_type
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn remove_event(&self, aggregate: &str, event: &str) -> Result<AggregateSchema> {
        let mut items = self.items.write();
        let result = {
            let schema = items.get_mut(aggregate).ok_or(EventError::SchemaNotFound)?;

            if !schema.events.contains_key(event) {
                return Err(EventError::SchemaViolation(format!(
                    "event {} is not defined for aggregate {}",
                    event, aggregate
                )));
            }

            if schema.events.len() == 1 {
                return Err(EventError::SchemaViolation(format!(
                    "aggregate {} must define at least one event",
                    aggregate
                )));
            }

            schema.events.remove(event);
            schema.updated_at = Utc::now();
            schema.clone()
        };

        self.persist(&items)?;
        Ok(result)
    }

    fn persist(&self, items: &BTreeMap<String, AggregateSchema>) -> Result<()> {
        let payload = serde_json::to_string_pretty(items)?;
        fs::write(&self.path, payload)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, str::FromStr};

    #[test]
    fn create_and_update_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        let manager = SchemaManager::load(path).unwrap();

        let schema = manager
            .create(CreateSchemaInput {
                aggregate: "patient".into(),
                events: vec!["patient-created".into(), "patient-updated".into()],
                snapshot_threshold: Some(10),
            })
            .unwrap();
        assert_eq!(schema.aggregate, "patient");
        assert_eq!(schema.events.len(), 2);

        let updated = manager
            .update(
                "patient",
                SchemaUpdate {
                    locked: Some(true),
                    field_lock: Some(("birthdate".into(), true)),
                    event_add_fields: {
                        let mut map = BTreeMap::new();
                        map.insert(
                            "patient-created".into(),
                            vec!["name".into(), "birthdate".into()],
                        );
                        map
                    },
                    ..SchemaUpdate::default()
                },
            )
            .unwrap();

        assert!(updated.locked);
        assert!(updated.field_locks.contains(&"birthdate".to_string()));
        assert!(
            updated
                .events
                .get("patient-created")
                .unwrap()
                .fields
                .contains(&"name".to_string())
        );
    }

    #[test]
    fn remove_event_from_aggregate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        let manager = SchemaManager::load(path).unwrap();

        manager
            .create(CreateSchemaInput {
                aggregate: "person".into(),
                events: vec!["person_created".into(), "person_updated".into()],
                snapshot_threshold: None,
            })
            .unwrap();

        let updated = manager.remove_event("person", "person_updated").unwrap();
        assert!(updated.events.contains_key("person_created"));
        assert!(!updated.events.contains_key("person_updated"));

        let err = manager.remove_event("person", "missing").unwrap_err();
        assert!(matches!(err, EventError::SchemaViolation(_)));

        let err = manager
            .remove_event("person", "person_created")
            .unwrap_err();
        assert!(matches!(err, EventError::SchemaViolation(_)));
    }

    #[test]
    fn validates_required_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        let manager = SchemaManager::load(path).unwrap();

        manager
            .create(CreateSchemaInput {
                aggregate: "person".into(),
                events: vec!["person_created".into()],
                snapshot_threshold: None,
            })
            .unwrap();

        let mut update = SchemaUpdate::default();
        update
            .event_add_fields
            .insert("person_created".into(), vec!["first_name".into()]);
        manager.update("person", update).unwrap();

        let mut payload = BTreeMap::new();
        payload.insert("first_name".into(), "Alice".into());
        manager
            .validate_event("person", "person_created", &payload)
            .unwrap();

        payload.remove("first_name");
        let err = manager
            .validate_event("person", "person_created", &payload)
            .unwrap_err();
        assert!(matches!(err, EventError::SchemaViolation(_)));
    }

    #[test]
    fn snapshot_threshold_triggers_on_expected_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        let manager = SchemaManager::load(path).unwrap();

        manager
            .create(CreateSchemaInput {
                aggregate: "order".into(),
                events: vec!["order-created".into()],
                snapshot_threshold: Some(3),
            })
            .unwrap();

        assert!(!manager.should_snapshot("order", 1));
        assert!(manager.should_snapshot("order", 3));
        assert!(!manager.should_snapshot("order", 4));

        // Unknown aggregate returns false.
        assert!(!manager.should_snapshot("missing", 3));
    }

    #[test]
    fn column_type_from_str_parses_decimal() {
        let ty = ColumnType::from_str("decimal(12, 4)").unwrap();
        assert_eq!(
            ty,
            ColumnType::Decimal {
                precision: 12,
                scale: 4
            }
        );
    }

    #[test]
    fn column_type_round_trip_serialization() {
        let mut map = BTreeMap::new();
        map.insert(
            "amount".to_string(),
            ColumnType::Decimal {
                precision: 8,
                scale: 2,
            },
        );
        map.insert("flag".to_string(), ColumnType::Boolean);

        let json = serde_json::to_string(&map).unwrap();
        assert_eq!(json, r#"{"amount":"decimal(8,2)","flag":"boolean"}"#);

        let decoded: BTreeMap<String, ColumnType> = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn update_schema_column_type() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        let manager = SchemaManager::load(path).unwrap();

        manager
            .create(CreateSchemaInput {
                aggregate: "order".into(),
                events: vec!["order_created".into()],
                snapshot_threshold: None,
            })
            .unwrap();

        let mut update = SchemaUpdate::default();
        update.column_type = Some((
            "total".to_string(),
            Some(ColumnType::Decimal {
                precision: 12,
                scale: 2,
            }),
        ));

        let schema = manager.update("order", update).unwrap();
        let column_type = schema
            .column_types
            .get("total")
            .expect("column type should be recorded");
        assert_eq!(
            column_type,
            &ColumnType::Decimal {
                precision: 12,
                scale: 2
            }
        );

        let mut removal = SchemaUpdate::default();
        removal.column_type = Some(("total".to_string(), None));
        let schema = manager.update("order", removal).unwrap();
        assert!(!schema.column_types.contains_key("total"));
    }
}
