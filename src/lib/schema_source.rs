use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{
    error::{EventError, Result},
    reference::ReferenceRules,
    schema::{
        AggregateSchema, ColumnSettings, ColumnType, EventSchema, FieldFormat, FieldRules,
        MAX_EVENT_NOTE_LENGTH,
    },
};

pub const DEFAULT_SCHEMA_FILE_NAME: &str = "schema.dbx";

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    pub schemas: BTreeMap<String, AggregateSchema>,
}

impl CompiledSchema {
    pub fn aggregate_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }
}

pub fn find_schema_file(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }

    let current = env::current_dir()
        .map_err(|err| EventError::Config(format!("failed to resolve current directory: {err}")))?;
    find_schema_file_from(&current)
}

pub fn load_schema_file(explicit: Option<&Path>) -> Result<(PathBuf, String)> {
    let path = find_schema_file(explicit)?;
    let contents = fs::read_to_string(&path).map_err(|err| {
        EventError::InvalidSchema(format!(
            "failed to read schema source {}: {err}",
            path.display()
        ))
    })?;
    Ok((path, contents))
}

fn find_schema_file_from(start: &Path) -> Result<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let candidate = current.join(DEFAULT_SCHEMA_FILE_NAME);
        if candidate.exists() {
            return Ok(candidate);
        }
        if !current.pop() {
            break;
        }
    }

    Err(EventError::InvalidSchema(format!(
        "could not find {} in the current directory or any parent directory",
        DEFAULT_SCHEMA_FILE_NAME
    )))
}

pub fn compile_schema_source(
    input: &str,
    existing: Option<&BTreeMap<String, AggregateSchema>>,
) -> Result<CompiledSchema> {
    let document = Parser::new(input).parse_document()?;
    let now = Utc::now();
    let mut schemas = BTreeMap::new();

    for aggregate in document.aggregates {
        let name = aggregate.name.clone();
        if schemas.contains_key(&name) {
            return Err(EventError::InvalidSchema(format!(
                "aggregate {name} is defined more than once"
            )));
        }
        let schema =
            compile_aggregate(aggregate, existing.and_then(|items| items.get(&name)), now)?;
        schemas.insert(name, schema);
    }

    Ok(CompiledSchema { schemas })
}

fn compile_aggregate(
    aggregate: AggregateDecl,
    existing: Option<&AggregateSchema>,
    now: DateTime<Utc>,
) -> Result<AggregateSchema> {
    if aggregate.name.trim().is_empty() {
        return Err(EventError::InvalidSchema(
            "aggregate name cannot be empty".to_string(),
        ));
    }
    if aggregate.events.is_empty() {
        return Err(EventError::InvalidSchema(format!(
            "aggregate {} must define at least one event",
            aggregate.name
        )));
    }

    let snapshot_threshold = match aggregate.snapshot_threshold {
        Some(value) => parse_optional_u64(&value, "snapshot_threshold")?,
        None => None,
    };
    let locked = match aggregate.locked {
        Some(value) => parse_bool(&value, "locked")?,
        None => false,
    };
    let hidden = match aggregate.hidden {
        Some(value) => parse_bool(&value, "hidden")?,
        None => false,
    };

    let mut hidden_fields = match aggregate.hidden_fields {
        Some(value) => parse_string_list(&value, "hidden_fields")?,
        None => Vec::new(),
    };
    let mut field_locks = match aggregate.field_locks {
        Some(value) => parse_string_list(&value, "field_locks")?,
        None => Vec::new(),
    };

    let mut column_types = BTreeMap::new();
    let mut seen_fields = BTreeSet::new();
    for field in aggregate.fields {
        if !seen_fields.insert(field.name.clone()) {
            return Err(EventError::InvalidSchema(format!(
                "field {} is defined more than once in aggregate {}",
                field.name, aggregate.name
            )));
        }

        let hidden_field = match field.hidden {
            Some(value) => parse_bool(&value, "field.hidden")?,
            None => false,
        };
        if hidden_field {
            hidden_fields.push(field.name.clone());
        }

        let locked_field = match field.locked {
            Some(value) => parse_bool(&value, "field.locked")?,
            None => false,
        };
        if locked_field {
            field_locks.push(field.name.clone());
        }

        let type_value = field.column_type.ok_or_else(|| {
            EventError::InvalidSchema(format!(
                "field {} in aggregate {} must define type",
                field.name, aggregate.name
            ))
        })?;
        let type_name = parse_string(&type_value, "field.type")?;
        let mut rules = match field.rules {
            Some(value) => parse_rules(&value)?,
            None => FieldRules::default(),
        };

        let column_type = parse_column_type_alias(type_name.as_str(), &mut rules)?;
        if rules.reference.is_some() && rules.format != Some(FieldFormat::Reference) {
            rules.format = Some(FieldFormat::Reference);
        }

        column_types.insert(field.name, ColumnSettings { column_type, rules });
    }

    let mut events = BTreeMap::new();
    let mut seen_events = BTreeSet::new();
    for event in aggregate.events {
        if !seen_events.insert(event.name.clone()) {
            return Err(EventError::InvalidSchema(format!(
                "event {} is defined more than once in aggregate {}",
                event.name, aggregate.name
            )));
        }

        let fields = match event.fields {
            Some(value) => parse_string_list(&value, "event.fields")?,
            None => Vec::new(),
        };
        let notes = match event.note {
            Some(value) => parse_optional_string(&value, "event.note")?,
            None => None,
        };
        if let Some(ref note) = notes {
            if note.chars().count() > MAX_EVENT_NOTE_LENGTH {
                return Err(EventError::InvalidSchema(format!(
                    "default note for event {} cannot exceed {} characters",
                    event.name, MAX_EVENT_NOTE_LENGTH
                )));
            }
        }

        let mut fields = fields;
        fields.sort();
        fields.dedup();
        events.insert(event.name, EventSchema { fields, notes });
    }

    hidden_fields.sort();
    hidden_fields.dedup();
    field_locks.sort();
    field_locks.dedup();

    let mut schema = AggregateSchema {
        aggregate: aggregate.name.clone(),
        snapshot_threshold,
        locked,
        field_locks,
        hidden,
        hidden_fields,
        column_types,
        events,
        created_at: now,
        updated_at: now,
    };

    if let Some(existing) = existing {
        schema.created_at = existing.created_at;
        let mut candidate = schema.clone();
        candidate.updated_at = existing.updated_at;
        if &candidate == existing {
            schema.updated_at = existing.updated_at;
        }
    }

    Ok(schema)
}

fn parse_rules(value: &Value) -> Result<FieldRules> {
    if !value.is_object() {
        return Err(EventError::InvalidSchema(
            "field.rules must be a JSON object".to_string(),
        ));
    }
    let rules: FieldRules = serde_json::from_value(value.clone())
        .map_err(|err| EventError::InvalidSchema(format!("failed to parse field.rules: {err}")))?;
    Ok(rules)
}

fn parse_column_type_alias(input: &str, rules: &mut FieldRules) -> Result<ColumnType> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(EventError::InvalidSchema(
            "field.type cannot be empty".to_string(),
        ));
    }

    if is_reference_type_alias(trimmed) {
        if rules.format.is_none() {
            rules.format = Some(FieldFormat::Reference);
        }
        if rules.reference.is_none() {
            rules.reference = Some(ReferenceRules::default());
        }
        return Ok(ColumnType::Text);
    }

    ColumnType::from_str(trimmed).map_err(|err| EventError::InvalidSchema(err.to_string()))
}

fn is_reference_type_alias(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "ref" | "reference" | "aggregate_ref" | "aggregate-reference"
    )
}

fn parse_bool(value: &Value, label: &str) -> Result<bool> {
    value
        .as_bool()
        .ok_or_else(|| EventError::InvalidSchema(format!("{label} must be a JSON boolean")))
}

fn parse_string(value: &Value, label: &str) -> Result<String> {
    value
        .as_str()
        .map(|value| value.to_string())
        .ok_or_else(|| EventError::InvalidSchema(format!("{label} must be a JSON string")))
}

fn parse_optional_string(value: &Value, label: &str) -> Result<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::String(text) => Ok(Some(text.clone())),
        _ => Err(EventError::InvalidSchema(format!(
            "{label} must be a JSON string or null"
        ))),
    }
}

fn parse_optional_u64(value: &Value, label: &str) -> Result<Option<u64>> {
    match value {
        Value::Null => Ok(None),
        Value::Number(number) => number.as_u64().map(Some).ok_or_else(|| {
            EventError::InvalidSchema(format!("{label} must be a non-negative integer or null"))
        }),
        _ => Err(EventError::InvalidSchema(format!(
            "{label} must be a non-negative integer or null"
        ))),
    }
}

fn parse_string_list(value: &Value, label: &str) -> Result<Vec<String>> {
    let items = value.as_array().ok_or_else(|| {
        EventError::InvalidSchema(format!("{label} must be a JSON array of strings"))
    })?;
    let mut output = Vec::with_capacity(items.len());
    for item in items {
        let text = item.as_str().ok_or_else(|| {
            EventError::InvalidSchema(format!("{label} must be a JSON array of strings"))
        })?;
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(EventError::InvalidSchema(format!(
                "{label} entries cannot be empty"
            )));
        }
        output.push(trimmed.to_string());
    }
    Ok(output)
}

#[derive(Debug)]
struct SchemaDocument {
    aggregates: Vec<AggregateDecl>,
}

#[derive(Debug)]
struct AggregateDecl {
    name: String,
    snapshot_threshold: Option<Value>,
    locked: Option<Value>,
    hidden: Option<Value>,
    hidden_fields: Option<Value>,
    field_locks: Option<Value>,
    fields: Vec<FieldDecl>,
    events: Vec<EventDecl>,
}

#[derive(Debug)]
struct FieldDecl {
    name: String,
    column_type: Option<Value>,
    rules: Option<Value>,
    hidden: Option<Value>,
    locked: Option<Value>,
}

#[derive(Debug)]
struct EventDecl {
    name: String,
    fields: Option<Value>,
    note: Option<Value>,
}

struct Parser<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn parse_document(&mut self) -> Result<SchemaDocument> {
        let mut aggregates = Vec::new();
        self.skip_ws_and_comments();
        while !self.is_eof() {
            let keyword = self.parse_word()?;
            if keyword != "aggregate" {
                return Err(self.error(format!("expected `aggregate`, found `{keyword}`")));
            }
            let name = self.parse_name()?;
            self.expect_char('{')?;
            let aggregate = self.parse_aggregate(name)?;
            aggregates.push(aggregate);
            self.skip_ws_and_comments();
        }
        Ok(SchemaDocument { aggregates })
    }

    fn parse_aggregate(&mut self, name: String) -> Result<AggregateDecl> {
        let mut aggregate = AggregateDecl {
            name,
            snapshot_threshold: None,
            locked: None,
            hidden: None,
            hidden_fields: None,
            field_locks: None,
            fields: Vec::new(),
            events: Vec::new(),
        };

        loop {
            self.skip_ws_and_comments();
            if self.try_consume_char('}') {
                break;
            }

            let token = self.parse_word()?;
            match token.as_str() {
                "field" => {
                    let name = self.parse_name()?;
                    self.expect_char('{')?;
                    aggregate.fields.push(self.parse_field(name)?);
                }
                "event" => {
                    let name = self.parse_name()?;
                    self.expect_char('{')?;
                    aggregate.events.push(self.parse_event(name)?);
                }
                "snapshot_threshold" => {
                    set_once(
                        &mut aggregate.snapshot_threshold,
                        "snapshot_threshold",
                        self.parse_assignment_value()?,
                    )?;
                }
                "locked" => {
                    set_once(
                        &mut aggregate.locked,
                        "locked",
                        self.parse_assignment_value()?,
                    )?;
                }
                "hidden" => {
                    set_once(
                        &mut aggregate.hidden,
                        "hidden",
                        self.parse_assignment_value()?,
                    )?;
                }
                "hidden_fields" => {
                    set_once(
                        &mut aggregate.hidden_fields,
                        "hidden_fields",
                        self.parse_assignment_value()?,
                    )?;
                }
                "field_locks" => {
                    set_once(
                        &mut aggregate.field_locks,
                        "field_locks",
                        self.parse_assignment_value()?,
                    )?;
                }
                other => {
                    return Err(self.error(format!("unsupported aggregate property `{other}`")));
                }
            }
        }

        Ok(aggregate)
    }

    fn parse_field(&mut self, name: String) -> Result<FieldDecl> {
        let mut field = FieldDecl {
            name,
            column_type: None,
            rules: None,
            hidden: None,
            locked: None,
        };

        loop {
            self.skip_ws_and_comments();
            if self.try_consume_char('}') {
                break;
            }

            let key = self.parse_word()?;
            match key.as_str() {
                "type" => {
                    set_once(
                        &mut field.column_type,
                        "type",
                        self.parse_assignment_value()?,
                    )?;
                }
                "rules" => {
                    set_once(&mut field.rules, "rules", self.parse_assignment_value()?)?;
                }
                "hidden" => {
                    set_once(&mut field.hidden, "hidden", self.parse_assignment_value()?)?;
                }
                "locked" => {
                    set_once(&mut field.locked, "locked", self.parse_assignment_value()?)?;
                }
                other => {
                    return Err(self.error(format!("unsupported field property `{other}`")));
                }
            }
        }

        Ok(field)
    }

    fn parse_event(&mut self, name: String) -> Result<EventDecl> {
        let mut event = EventDecl {
            name,
            fields: None,
            note: None,
        };

        loop {
            self.skip_ws_and_comments();
            if self.try_consume_char('}') {
                break;
            }

            let key = self.parse_word()?;
            match key.as_str() {
                "fields" => {
                    set_once(&mut event.fields, "fields", self.parse_assignment_value()?)?;
                }
                "note" => {
                    set_once(&mut event.note, "note", self.parse_assignment_value()?)?;
                }
                other => {
                    return Err(self.error(format!("unsupported event property `{other}`")));
                }
            }
        }

        Ok(event)
    }

    fn parse_assignment_value(&mut self) -> Result<Value> {
        self.expect_char('=')?;
        self.skip_ws_and_comments();
        self.parse_json_value()
    }

    fn parse_json_value(&mut self) -> Result<Value> {
        let slice = &self.input[self.offset..];
        let mut stream = serde_json::Deserializer::from_str(slice).into_iter::<Value>();
        let value = stream
            .next()
            .transpose()
            .map_err(|err| self.error(format!("invalid JSON value: {err}")))?
            .ok_or_else(|| self.error("expected JSON value".to_string()))?;
        self.offset += stream.byte_offset();
        Ok(value)
    }

    fn parse_name(&mut self) -> Result<String> {
        self.skip_ws_and_comments();
        if self.peek_char() == Some('"') {
            let value = self.parse_json_value()?;
            return parse_string(&value, "name");
        }

        let start = self.offset;
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() || matches!(ch, '{' | '}' | '=') {
                break;
            }
            self.offset += ch.len_utf8();
        }
        if self.offset == start {
            return Err(self.error("expected name".to_string()));
        }
        Ok(self.input[start..self.offset].to_string())
    }

    fn parse_word(&mut self) -> Result<String> {
        self.skip_ws_and_comments();
        let start = self.offset;
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() || matches!(ch, '{' | '}' | '=') {
                break;
            }
            self.offset += ch.len_utf8();
        }
        if self.offset == start {
            return Err(self.error("expected token".to_string()));
        }
        Ok(self.input[start..self.offset].to_string())
    }

    fn skip_ws_and_comments(&mut self) {
        loop {
            while let Some(ch) = self.peek_char() {
                if ch.is_whitespace() {
                    self.offset += ch.len_utf8();
                } else {
                    break;
                }
            }

            if self.remaining().starts_with("//") {
                while let Some(ch) = self.peek_char() {
                    self.offset += ch.len_utf8();
                    if ch == '\n' {
                        break;
                    }
                }
                continue;
            }

            if self.remaining().starts_with('#') {
                while let Some(ch) = self.peek_char() {
                    self.offset += ch.len_utf8();
                    if ch == '\n' {
                        break;
                    }
                }
                continue;
            }

            break;
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<()> {
        self.skip_ws_and_comments();
        match self.peek_char() {
            Some(ch) if ch == expected => {
                self.offset += ch.len_utf8();
                Ok(())
            }
            Some(ch) => Err(self.error(format!("expected `{expected}`, found `{ch}`"))),
            None => Err(self.error(format!("expected `{expected}`, found end of file"))),
        }
    }

    fn try_consume_char(&mut self, expected: char) -> bool {
        self.skip_ws_and_comments();
        match self.peek_char() {
            Some(ch) if ch == expected => {
                self.offset += ch.len_utf8();
                true
            }
            _ => false,
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn remaining(&self) -> &str {
        &self.input[self.offset..]
    }

    fn is_eof(&self) -> bool {
        self.offset >= self.input.len()
    }

    fn error(&self, message: String) -> EventError {
        let (line, column) = line_and_column(self.input, self.offset);
        EventError::InvalidSchema(format!("{message} at line {line}, column {column}"))
    }
}

fn set_once(target: &mut Option<Value>, label: &str, value: Value) -> Result<()> {
    if target.is_some() {
        return Err(EventError::InvalidSchema(format!(
            "{label} cannot be defined more than once"
        )));
    }
    *target = Some(value);
    Ok(())
}

fn line_and_column(input: &str, offset: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut column = 1usize;
    for ch in input[..offset.min(input.len())].chars() {
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line, column)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn compiles_schema_source_with_full_features() {
        let compiled = compile_schema_source(
            r#"
            aggregate person {
              snapshot_threshold = 100
              locked = false
              hidden = false
              hidden_fields = ["internal_notes"]
              field_locks = ["id"]

              field email {
                type = "text"
                rules = {"required": true, "format": "email"}
              }

              field manager {
                type = "reference"
                hidden = true
                locked = true
                rules = {
                  "reference": {
                    "tenant": "default",
                    "aggregate_type": "person",
                    "integrity": "strong",
                    "cascade": "restrict"
                  }
                }
              }

              field profile {
                type = "object"
                rules = {
                  "properties": {
                    "country": {
                      "type": "text",
                      "format": "country_code"
                    }
                  }
                }
              }

              event person_created {
                fields = ["email", "manager"]
                note = "Initial import"
              }

              event person_updated {}
            }
            "#,
            None,
        )
        .expect("compile schema");

        let schema = compiled.schemas.get("person").expect("person schema");
        assert_eq!(schema.snapshot_threshold, Some(100));
        assert!(
            schema
                .hidden_fields
                .iter()
                .any(|field| field == "internal_notes")
        );
        assert!(schema.hidden_fields.iter().any(|field| field == "manager"));
        assert!(schema.field_locks.iter().any(|field| field == "id"));
        assert!(schema.field_locks.iter().any(|field| field == "manager"));
        assert_eq!(
            schema.column_types["email"].rules.format,
            Some(FieldFormat::Email)
        );
        assert_eq!(
            schema.column_types["manager"].rules.format,
            Some(FieldFormat::Reference)
        );
        assert_eq!(
            schema.events["person_created"].notes.as_deref(),
            Some("Initial import")
        );
    }

    #[test]
    fn rejects_duplicate_aggregate_names() {
        let err = compile_schema_source(
            r#"
            aggregate one { event created {} }
            aggregate one { event updated {} }
            "#,
            None,
        )
        .expect_err("expected duplicate aggregate to fail");
        assert!(err.to_string().contains("defined more than once"));
    }

    #[test]
    fn rejects_missing_field_type() {
        let err = compile_schema_source(
            r#"
            aggregate person {
              field email {}
              event person_created {}
            }
            "#,
            None,
        )
        .expect_err("expected missing field type to fail");
        assert!(err.to_string().contains("must define type"));
    }

    #[test]
    fn finds_schema_file_in_parent_directory() {
        let temp = tempdir().expect("tempdir");
        let project = temp.path().join("project");
        let nested = project.join("src").join("api");
        fs::create_dir_all(&nested).expect("nested dirs");
        let schema = project.join(DEFAULT_SCHEMA_FILE_NAME);
        fs::write(&schema, "aggregate person { event person_created {} }").expect("schema file");

        let found = find_schema_file_from(&nested).expect("find schema file");

        assert_eq!(found, schema);
    }
}
