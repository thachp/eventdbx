use std::{collections::BTreeMap, fs};

use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};

use crate::{
    config::SqlitePluginConfig,
    error::{EventError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

use super::{
    Plugin,
    util::{quote_identifier, sanitize_identifier},
};

pub(super) struct SqlitePlugin {
    config: SqlitePluginConfig,
}

impl SqlitePlugin {
    pub(super) fn new(config: SqlitePluginConfig) -> Self {
        Self { config }
    }

    fn ensure_database(&self) -> Result<Connection> {
        if let Some(parent) = self.config.path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|err| EventError::Storage(err.to_string()))?;
            }
        }

        Connection::open(&self.config.path).map_err(|err| EventError::Storage(err.to_string()))
    }

    fn ensure_table(
        conn: &Connection,
        aggregate: &str,
        schema: Option<&AggregateSchema>,
        state: &BTreeMap<String, String>,
    ) -> Result<(String, Vec<(String, String)>)> {
        let table_name = sanitize_identifier(aggregate);
        let quoted_table = quote_identifier(&table_name);

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT,
                updated_at TEXT NOT NULL,
                updated_by TEXT
            )",
            table = quoted_table
        );
        conn.execute(&create_sql, [])
            .map_err(|err| EventError::Storage(err.to_string()))?;

        for (column, ty) in [
            ("created_at", "TEXT"),
            ("created_by", "TEXT"),
            ("updated_at", "TEXT"),
            ("updated_by", "TEXT"),
        ] {
            let alter_sql = format!(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ty}",
                table = quoted_table,
                column = quote_identifier(column),
                ty = ty
            );
            conn.execute(&alter_sql, [])
                .map_err(|err| EventError::Storage(err.to_string()))?;
        }

        let mut fields = BTreeMap::new();
        if let Some(schema) = schema {
            for event_schema in schema.events.values() {
                for field in &event_schema.fields {
                    fields.insert(field.clone(), ());
                }
            }
        }
        for key in state.keys() {
            fields.insert(key.clone(), ());
        }

        let mut column_names = Vec::new();
        for field in fields.keys() {
            let column = sanitize_identifier(field);
            let quoted_column = quote_identifier(&column);
            let alter_sql = format!(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} TEXT",
                table = quoted_table,
                column = quoted_column
            );
            conn.execute(&alter_sql, [])
                .map_err(|err| EventError::Storage(err.to_string()))?;
            column_names.push((column, field.clone()));
        }

        Ok((quoted_table, column_names))
    }
}

impl Plugin for SqlitePlugin {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let conn = self.ensure_database()?;
        let (table, field_columns) =
            Self::ensure_table(&conn, &record.aggregate_type, schema, &state.state)?;

        let created_at = record.metadata.created_at.to_rfc3339();
        let updated_at = record.metadata.created_at.to_rfc3339();
        let actor_id = record
            .metadata
            .issued_by
            .as_ref()
            .map(|claims| claims.user.clone());
        let actor_value = actor_id.clone().map(Value::from).unwrap_or(Value::Null);

        let mut columns = vec![
            "id".to_string(),
            "version".to_string(),
            "created_at".to_string(),
            "created_by".to_string(),
            "updated_at".to_string(),
            "updated_by".to_string(),
        ];

        let mut params = vec![
            Value::from(state.aggregate_id.clone()),
            Value::from(state.version as i64),
            Value::from(created_at),
            actor_value.clone(),
            Value::from(updated_at),
            actor_value,
        ];

        for (column, original) in &field_columns {
            columns.push(column.clone());
            let value = state
                .state
                .get(original)
                .cloned()
                .map(Value::from)
                .unwrap_or(Value::Null);
            params.push(value);
        }

        let quoted_columns: Vec<String> = columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect();
        let placeholders: Vec<String> =
            (1..=columns.len()).map(|idx| format!("?{}", idx)).collect();
        let assignments: Vec<String> = columns
            .iter()
            .zip(quoted_columns.iter())
            .filter_map(|(name, quoted)| match name.as_str() {
                "id" | "created_at" | "created_by" => None,
                _ => Some(format!("{quoted} = excluded.{name}")),
            })
            .collect();

        let insert_sql = format!(
            "INSERT INTO {table} ({columns}) VALUES ({placeholders}) \
             ON CONFLICT(id) DO UPDATE SET {assignments}",
            table = table,
            columns = quoted_columns.join(", "),
            placeholders = placeholders.join(", "),
            assignments = assignments.join(", ")
        );

        conn.execute(&insert_sql, params_from_iter(params))
            .map_err(|err| EventError::Storage(err.to_string()))?;

        Ok(())
    }
}
