use std::sync::Arc;

use tracing::error;

use crate::{
    config::{Config, PluginConfig, PluginDefinition},
    error::{EventError, Result},
    schema::AggregateSchema,
    store::{AggregateState, EventRecord},
};

mod process;
pub mod registry;
mod tcp;
use tcp::TcpPlugin;
mod http;
use http::HttpPlugin;
mod log;
use log::LogPlugin;
use process::ProcessPlugin;

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
            plugins.push(instantiate_plugin(&definition, config));
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
        PluginConfig::Log(settings) => {
            let plugin = LogPlugin::new(settings.clone());
            plugin.ensure_ready()
        }
        PluginConfig::Process(_) => Ok(()),
    }
}

pub fn instantiate_plugin(definition: &PluginDefinition, config: &Config) -> Box<dyn Plugin> {
    match &definition.config {
        PluginConfig::Tcp(settings) => Box::new(TcpPlugin::new(settings.clone())),
        PluginConfig::Http(settings) => Box::new(HttpPlugin::new(settings.clone())),
        PluginConfig::Log(settings) => Box::new(LogPlugin::new(settings.clone())),
        PluginConfig::Process(settings) => {
            let identifier = definition
                .name
                .clone()
                .unwrap_or_else(|| settings.name.clone());
            match ProcessPlugin::new(identifier.clone(), settings.clone(), &config.data_dir) {
                Ok(plugin) => Box::new(plugin),
                Err(err) => {
                    tracing::error!(
                        target: "eventdbx.plugin",
                        "failed to initialize process plugin {}: {}",
                        identifier,
                        err
                    );
                    Box::new(UnavailablePlugin::new(identifier, err.to_string()))
                }
            }
        }
    }
}

struct UnavailablePlugin {
    label: String,
    reason: String,
}

impl UnavailablePlugin {
    fn new(label: String, reason: String) -> Self {
        Self { label, reason }
    }
}

impl Plugin for UnavailablePlugin {
    fn name(&self) -> &'static str {
        "unavailable"
    }

    fn notify_event(
        &self,
        _record: &EventRecord,
        _state: &AggregateState,
        _schema: Option<&AggregateSchema>,
    ) -> Result<()> {
        Err(EventError::Config(format!(
            "plugin '{}' is unavailable: {}",
            self.label, self.reason
        )))
    }
}
