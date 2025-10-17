use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, OpenOptions},
    path::PathBuf,
};

use csv::{ReaderBuilder, Writer, WriterBuilder};

use crate::{
    config::CsvPluginConfig,
    error::{EventfulError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

use super::{Plugin, util::sanitize_identifier};

pub(super) struct CsvPlugin {
    config: CsvPluginConfig,
}

impl CsvPlugin {
    pub(super) fn new(config: CsvPluginConfig) -> Self {
        Self { config }
    }

    fn ensure_output_dir(&self) -> Result<()> {
        if !self.config.output_dir.exists() {
            fs::create_dir_all(&self.config.output_dir)
                .map_err(|err| EventfulError::Storage(err.to_string()))?;
        }
        Ok(())
    }

    fn output_path(&self, aggregate: &str) -> PathBuf {
        let table = sanitize_identifier(aggregate);
        self.config.output_dir.join(format!("{table}.csv"))
    }

    fn base_columns() -> Vec<String> {
        vec![
            "id".to_string(),
            "version".to_string(),
            "created_at".to_string(),
            "created_by".to_string(),
            "updated_at".to_string(),
            "updated_by".to_string(),
        ]
    }

    fn base_column_set() -> BTreeSet<String> {
        Self::base_columns().into_iter().collect()
    }

    fn collect_dynamic_columns(
        schema: Option<&AggregateSchema>,
        state: &BTreeMap<String, String>,
    ) -> BTreeSet<String> {
        let base = Self::base_column_set();
        let mut fields = BTreeSet::new();
        if let Some(schema) = schema {
            for event_schema in schema.events.values() {
                for field in &event_schema.fields {
                    let column = sanitize_identifier(field);
                    if !base.contains(&column) {
                        fields.insert(column);
                    }
                }
            }
        }
        for key in state.keys() {
            let column = sanitize_identifier(key);
            if !base.contains(&column) {
                fields.insert(column);
            }
        }
        fields
    }

    fn read_existing(
        path: &PathBuf,
    ) -> Result<Option<(Vec<String>, Vec<HashMap<String, String>>)>> {
        if !path.exists() {
            return Ok(None);
        }

        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_path(path)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        let headers = reader
            .headers()
            .map_err(|err| EventfulError::Storage(err.to_string()))?
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();

        let mut rows = Vec::new();
        for result in reader.records() {
            let record = result.map_err(|err| EventfulError::Storage(err.to_string()))?;
            let mut map = HashMap::new();
            for (idx, value) in record.iter().enumerate() {
                if let Some(column) = headers.get(idx) {
                    map.insert(column.clone(), value.to_string());
                }
            }
            rows.push(map);
        }

        Ok(Some((headers, rows)))
    }

    fn write_rows(
        path: &PathBuf,
        columns: &[String],
        rows: &[HashMap<String, String>],
    ) -> Result<()> {
        let mut writer =
            Writer::from_path(path).map_err(|err| EventfulError::Storage(err.to_string()))?;
        writer
            .write_record(columns)
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        for row in rows {
            let record = columns
                .iter()
                .map(|column| row.get(column).cloned().unwrap_or_default());
            writer
                .write_record(record)
                .map_err(|err| EventfulError::Storage(err.to_string()))?;
        }
        writer
            .flush()
            .map_err(|err| EventfulError::Storage(err.to_string()))?;
        Ok(())
    }

    fn build_row_map(
        state: &AggregateState,
        record: &EventRecord,
        sanitized_state: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut row = HashMap::new();
        row.insert("id".into(), state.aggregate_id.clone());
        row.insert("version".into(), state.version.to_string());
        let timestamp = record.metadata.created_at.to_rfc3339();
        row.insert("created_at".into(), timestamp.clone());
        row.insert("updated_at".into(), timestamp);

        let actor = record
            .metadata
            .issued_by
            .as_ref()
            .map(|claims| claims.identifier_id.clone())
            .unwrap_or_default();
        row.insert("created_by".into(), actor.clone());
        row.insert("updated_by".into(), actor);

        for (column, value) in sanitized_state {
            row.insert(column.clone(), value.clone());
        }
        row
    }
}

impl Plugin for CsvPlugin {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn notify_event(
        &self,
        record: &EventRecord,
        state: &AggregateState,
        schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        self.ensure_output_dir()?;
        let path = self.output_path(&record.aggregate_type);

        let dynamic_schema = Self::collect_dynamic_columns(schema, &state.state);

        let existing = Self::read_existing(&path)?;
        let base_columns = Self::base_columns();
        let base_set = Self::base_column_set();
        let sanitized_state = state
            .state
            .iter()
            .filter_map(|(key, value)| {
                let column = sanitize_identifier(key);
                if base_set.contains(&column) {
                    None
                } else {
                    Some((column, value.clone()))
                }
            })
            .collect::<HashMap<_, _>>();

        let row_map = Self::build_row_map(state, record, &sanitized_state);

        match existing {
            None => {
                let mut columns = base_columns.clone();
                columns.extend(dynamic_schema.into_iter());
                let rows = vec![row_map];
                Self::write_rows(&path, &columns, &rows)?;
            }
            Some((headers, mut rows)) => {
                let mut existing_dynamic = BTreeSet::new();
                for column in &headers {
                    if !base_set.contains(column) {
                        existing_dynamic.insert(column.clone());
                    }
                }
                let mut all_dynamic = existing_dynamic;
                all_dynamic.extend(dynamic_schema.into_iter());

                let mut final_columns = base_columns.clone();
                final_columns.extend(all_dynamic.iter().cloned());

                if final_columns != headers {
                    rows.push(row_map);
                    Self::write_rows(&path, &final_columns, &rows)?;
                } else {
                    let mut writer = WriterBuilder::new().has_headers(false).from_writer(
                        OpenOptions::new()
                            .append(true)
                            .open(&path)
                            .map_err(|err| EventfulError::Storage(err.to_string()))?,
                    );
                    let record = final_columns
                        .iter()
                        .map(|column| row_map.get(column).cloned().unwrap_or_default());
                    writer
                        .write_record(record)
                        .map_err(|err| EventfulError::Storage(err.to_string()))?;
                    writer
                        .flush()
                        .map_err(|err| EventfulError::Storage(err.to_string()))?;
                }
            }
        }

        Ok(())
    }
}
