use std::env;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

pub const RUN_MODE_ENV: &str = "EVENTFUL_RUN_MODE";

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunMode {
    /// Unrestricted development mode (no schema enforcement)
    Dev,
    /// Restricted production mode (schema required)
    Prod,
}

impl RunMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Prod => "prod",
        }
    }

    pub fn requires_schema(self) -> bool {
        matches!(self, Self::Prod)
    }

    pub fn from_env() -> Self {
        env::var(RUN_MODE_ENV)
            .ok()
            .and_then(|value| Self::from_str(&value, true).ok())
            .unwrap_or(Self::Prod)
    }
}

pub fn set_run_mode_env(mode: RunMode) {
    unsafe {
        env::set_var(RUN_MODE_ENV, mode.as_str());
    }
}
