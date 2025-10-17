use std::sync::Arc;

use tracing::error;

use crate::{
    config::{Config, PluginConfig},
    error::Result,
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

mod sqlite;
mod util;
use sqlite::SqlitePlugin;
mod postgres;
use postgres::PostgresPlugin;
mod csv;
use csv::CsvPlugin;

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
                PluginConfig::Sqlite(settings) => {
                    plugins.push(Box::new(SqlitePlugin::new(settings.clone())));
                }
                PluginConfig::Csv(settings) => {
                    plugins.push(Box::new(CsvPlugin::new(settings.clone())));
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
