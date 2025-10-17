use std::{collections::BTreeMap, sync::Arc};

use postgres::{Client, NoTls};
use tracing::error;

use crate::{
    config::{Config, PluginConfig, PostgresColumnConfig, PostgresPluginConfig},
    error::{EventfulError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

pub trait Plugin: Send + Sync {
    fn name(&self) -> &'static str;
    fn notify_event(
        &self,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct PluginManager {
    plugins: Arc<Vec<Box<dyn Plugin>>>,
}

impl PluginManager {
    pub fn from_config(config: &Config) -> Result<Self> {
        let mut plugins: Vec<Box<dyn Plugin>> = Vec::new();
        for definition in &config.plugins {
            if !definition.enabled {
                continue;
            }
            match &definition.config {
                PluginConfig::Postgres(settings) => {
                    plugins.push(Box::new(PostgresPlugin::new(settings.clone())));
                }
            }
        }

        Ok(Self {
            plugins: Arc::new(plugins),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    pub fn notify_event(
        &self,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        for plugin in self.plugins.iter() {
            if let Err(err) = plugin.notify_event(record, state, schema) {
                error!("plugin {} failed: {}", plugin.name(), err);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PostgresPlugin {
    config: PostgresPluginConfig,
}

impl PostgresPlugin {
    fn new(config: PostgresPluginConfig) -> Self {
        Self { config }
    }

    fn ensure_table(
        &self,
        client: &mut Client,
        aggregate: &str,
        schema: Option<&AggregateSchema>,
        state: &BTreeMap<String, String>,
    ) -> Result<(String, Vec<(String, String)>)> {
        let table_name = sanitize_identifier(aggregate);
        let quoted_table = quote_identifier(&table_name);

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id TEXT NOT NULL,
                version BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                created_by TEXT,
                updated_at TIMESTAMPTZ NOT NULL,
                updated_by TEXT,
                PRIMARY KEY (id)
            )",
            quoted_table
        );
        client
            .batch_execute(&create_sql)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        for (column, ty) in [
            ("created_at", "TIMESTAMPTZ"),
            ("created_by", "TEXT"),
            ("updated_at", "TIMESTAMPTZ"),
            ("updated_by", "TEXT"),
        ] {
            let alter_sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                quoted_table,
                quote_identifier(column),
                ty
            );
            client
                .batch_execute(&alter_sql)
                .map_err(|err| EventfulError::Storage(err.to_string()))?;
        }

        // Collect required fields from schema or payload keys.
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
            let column_type = self
                .config
                .field_mappings
                .get(aggregate)
                .and_then(|map| map.get(field))
                .map(build_column_type)
                .unwrap_or_else(|| "TEXT".into());
            let alter_sql = format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                quoted_table, quoted_column, column_type
            );
            client
                .batch_execute(&alter_sql)
                .map_err(|err| EventfulError::Storage(err.to_string()))?;
            column_names.push((column, field.clone()));
        }

        Ok((quoted_table, column_names))
    }
}

impl Plugin for PostgresPlugin {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        let mut client = Client::connect(&self.config.connection_string, NoTls)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        let (table, field_columns) =
            self.ensure_table(&mut client, &record.aggregate_type, schema, &state.state)?;

        let mut columns = vec![
            "id".to_string(),
            "version".to_string(),
            "created_at".to_string(),
            "created_by".to_string(),
            "updated_at".to_string(),
            "updated_by".to_string(),
        ];

        let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> = Vec::new();
        params.push(Box::new(state.aggregate_id.clone()));
        params.push(Box::new(state.version as i64));

        let actor_id = record
            .metadata
            .issued_by
            .as_ref()
            .map(|claims| claims.identifier_id.clone());
        let created_at = record.metadata.created_at;
        let updated_at = record.metadata.created_at;

        params.push(Box::new(created_at));
        params.push(Box::new(actor_id.clone()));
        params.push(Box::new(updated_at));
        params.push(Box::new(actor_id.clone()));

        for (column, original) in &field_columns {
            let value: Option<String> = state.state.get(original).cloned();
            params.push(Box::new(value));
            columns.push(column.clone());
        }

        let placeholders: Vec<String> =
            (1..=columns.len()).map(|idx| format!("${}", idx)).collect();
        let quoted_columns: Vec<String> = columns.iter().map(|c| quote_identifier(c)).collect();

        let assignments: Vec<String> = columns
            .iter()
            .zip(quoted_columns.iter())
            .filter_map(|(name, column)| match name.as_str() {
                "id" | "created_at" | "created_by" => None,
                _ => Some(format!("{} = EXCLUDED.{}", column, column)),
            })
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {}",
            table,
            quoted_columns.join(", "),
            placeholders.join(", "),
            assignments.join(", ")
        );

        let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
            params.iter().map(|p| &**p as _).collect();

        client
            .execute(&insert_sql, &param_refs)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;

        Ok(())
    }
}

fn sanitize_identifier(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for (idx, ch) in input.chars().enumerate() {
        if ch.is_ascii_alphanumeric() {
            if idx == 0 && ch.is_ascii_digit() {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push('_');
        }
    }
    if result.is_empty() {
        "_".into()
    } else {
        result
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn build_column_type(config: &PostgresColumnConfig) -> String {
    config.data_type.clone().unwrap_or_else(|| "TEXT".into())
}
