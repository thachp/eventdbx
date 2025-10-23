use std::{env, fmt};

use serde::{Deserialize, Serialize};

pub const RESTRICT_ENV: &str = "EVENTDB_RESTRICT";
const LEGACY_RESTRICT_ENV: &str = "EVENTFUL_RESTRICT";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RestrictMode {
    Off,
    Default,
    Strict,
}

impl RestrictMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Default => "default",
            Self::Strict => "strict",
        }
    }

    pub fn enforces_validation(self) -> bool {
        !matches!(self, Self::Off)
    }

    pub fn requires_declared_schema(self) -> bool {
        matches!(self, Self::Strict)
    }
}

impl Default for RestrictMode {
    fn default() -> Self {
        Self::Default
    }
}

impl fmt::Display for RestrictMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn from_env() -> RestrictMode {
    env::var(RESTRICT_ENV)
        .or_else(|_| env::var(LEGACY_RESTRICT_ENV))
        .ok()
        .and_then(|value| parse_mode(&value))
        .unwrap_or_default()
}

pub fn set_env(mode: RestrictMode) {
    // Setting environment variables is marked unsafe in the 2024 edition.
    unsafe {
        env::set_var(RESTRICT_ENV, mode.as_str());
    }
}

pub fn parse_mode(input: &str) -> Option<RestrictMode> {
    let normalized = input.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "off" | "false" | "0" | "no" | "none" | "unrestricted" | "dev" => Some(RestrictMode::Off),
        "strict" | "all" | "enforced" => Some(RestrictMode::Strict),
        "default" | "true" | "1" | "yes" | "on" | "prod" | "restricted" | "hybrid" => {
            Some(RestrictMode::Default)
        }
        _ => None,
    }
}

pub fn legacy_bool(enabled: bool) -> RestrictMode {
    if enabled {
        RestrictMode::Default
    } else {
        RestrictMode::Off
    }
}

pub fn strict_mode_missing_schema_message(aggregate: &str) -> String {
    format!(
        "aggregate {} requires a schema before events can be appended when restrict=strict",
        aggregate
    )
}
