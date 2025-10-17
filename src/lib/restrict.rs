use std::env;

pub const RESTRICT_ENV: &str = "EVENTDB_RESTRICT";
const LEGACY_RESTRICT_ENV: &str = "EVENTFUL_RESTRICT";

pub fn as_str(enabled: bool) -> &'static str {
    if enabled { "true" } else { "false" }
}

pub fn from_env() -> bool {
    env::var(RESTRICT_ENV)
        .or_else(|_| env::var(LEGACY_RESTRICT_ENV))
        .ok()
        .and_then(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "1" | "yes" | "on" | "prod" | "restricted" => Some(true),
                "false" | "0" | "no" | "off" | "dev" | "unrestricted" => Some(false),
                _ => None,
            }
        })
        .unwrap_or(true)
}

pub fn set_env(enabled: bool) {
    // Setting environment variables is marked unsafe in the 2024 edition.
    unsafe {
        env::set_var(RESTRICT_ENV, as_str(enabled));
    }
}
