use std::sync::Arc;

use tracing::error;

use crate::{
    config::{Config, PluginConfig},
    error::Result,
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

mod util;
mod csv;
use csv::CsvPlugin;
mod tcp;
use tcp::TcpPlugin;
mod http;
use http::HttpPlugin;
mod json;
use json::JsonPlugin;
mod log;
use log::LogPlugin;

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
        let mut definitions = config.load_plugins()?;
        if definitions.is_empty() && !config.plugins.is_empty() {
            definitions = config.plugins.clone();
        }

        for definition in definitions.into_iter() {
            if !definition.enabled {
                continue;
            }
            match &definition.config {
                PluginConfig::Csv(settings) => {
                    plugins.push(Box::new(CsvPlugin::new(settings.clone())));
                }
                PluginConfig::Tcp(settings) => {
                    plugins.push(Box::new(TcpPlugin::new(settings.clone())));
                }
                PluginConfig::Http(settings) => {
                    plugins.push(Box::new(HttpPlugin::new(settings.clone())));
                }
                PluginConfig::Json(settings) => {
                    plugins.push(Box::new(JsonPlugin::new(settings.clone())));
                }
                PluginConfig::Log(settings) => {
                    plugins.push(Box::new(LogPlugin::new(settings.clone())));
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
