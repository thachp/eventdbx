use std::{collections::BTreeMap, sync::Arc};

use tracing::error;

use crate::{
    config::{Config, PluginConfig, PluginDefinition},
    error::Result,
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

mod csv;
mod postgres;
mod util;
use csv::CsvPlugin;
use postgres::PostgresPlugin;
mod tcp;
use tcp::TcpPlugin;
mod http;
use http::HttpPlugin;
mod json;
use json::JsonPlugin;
mod log;
use log::LogPlugin;

pub type ColumnTypes = BTreeMap<String, BTreeMap<String, String>>;

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
        let base_types: Arc<ColumnTypes> = Arc::new(config.column_types.clone());
        let mut definitions = config.load_plugins()?;
        if definitions.is_empty() && !config.plugins.is_empty() {
            definitions = config.plugins.clone();
        }

        for definition in definitions.into_iter() {
            if !definition.enabled {
                continue;
            }
            plugins.push(instantiate_plugin(&definition, Arc::clone(&base_types)));
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

pub fn establish_connection(definition: &PluginDefinition) -> Result<()> {
    match &definition.config {
        PluginConfig::Postgres(settings) => {
            let plugin = PostgresPlugin::new(settings.clone(), Arc::new(ColumnTypes::new()));
            plugin.ensure_ready()
        }
        PluginConfig::Csv(settings) => {
            let plugin = CsvPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Tcp(settings) => {
            let plugin = TcpPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Http(settings) => {
            let plugin = HttpPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Json(settings) => {
            let plugin = JsonPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Log(settings) => {
            let plugin = LogPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
    }
}

pub fn instantiate_plugin(
    definition: &PluginDefinition,
    base_types: Arc<ColumnTypes>,
) -> Box<dyn Plugin> {
    match &definition.config {
        PluginConfig::Postgres(settings) => {
            Box::new(PostgresPlugin::new(settings.clone(), base_types))
        }
        PluginConfig::Csv(settings) => Box::new(CsvPlugin::new(settings.clone())),
        PluginConfig::Tcp(settings) => Box::new(TcpPlugin::new(settings.clone())),
        PluginConfig::Http(settings) => Box::new(HttpPlugin::new(settings.clone())),
        PluginConfig::Json(settings) => Box::new(JsonPlugin::new(settings.clone())),
        PluginConfig::Log(settings) => Box::new(LogPlugin::new(settings.clone())),
    }
}
