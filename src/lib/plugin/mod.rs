use std::sync::Arc;

use tracing::error;

use crate::{
    config::{Config, PluginConfig, PluginDefinition},
    error::Result,
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

mod tcp;
use tcp::TcpPlugin;
mod http;
use http::HttpPlugin;
mod grpc;
use grpc::GrpcPlugin;
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
            plugins.push(instantiate_plugin(&definition));
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
        PluginConfig::Tcp(settings) => {
            let plugin = TcpPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Http(settings) => {
            let plugin = HttpPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Grpc(settings) => {
            let plugin = GrpcPlugin::new(settings.clone());
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
) -> Box<dyn Plugin> {
    match &definition.config {
        PluginConfig::Tcp(settings) => Box::new(TcpPlugin::new(settings.clone())),
        PluginConfig::Http(settings) => Box::new(HttpPlugin::new(settings.clone())),
        PluginConfig::Grpc(settings) => Box::new(GrpcPlugin::new(settings.clone())),
        PluginConfig::Log(settings) => Box::new(LogPlugin::new(settings.clone())),
    }
}
