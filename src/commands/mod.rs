use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use serde::Deserialize;
use std::env;

pub mod aggregate;
pub mod cli_token;
pub mod client;
pub mod config;
pub mod domain;
pub mod events;
pub mod plugin;
pub mod queue;
pub mod schema;
pub mod schema_version;
pub mod snapshots;
pub mod start;
pub mod system;
pub mod tenant;
pub mod token;
pub mod upgrade;
pub mod watch;

pub(crate) fn resolve_actor_name(
    explicit: Option<&str>,
    token: Option<&str>,
    fallback: &str,
) -> String {
    if let Some(value) = explicit.and_then(trimmed_non_empty) {
        return value;
    }

    if let Some(value) = token.and_then(actor_from_jwt) {
        return value;
    }

    env::var("USER")
        .or_else(|_| env::var("USERNAME"))
        .ok()
        .and_then(|value| trimmed_non_empty(value.as_str()))
        .unwrap_or_else(|| fallback.to_string())
}

fn actor_from_jwt(token: &str) -> Option<String> {
    let mut segments = token.trim().split('.');
    segments.next()?;
    let payload = segments.next()?;
    segments.next()?;

    let bytes = decode_jwt_payload(payload)?;
    let claims: JwtActorFragment = serde_json::from_slice(&bytes).ok()?;
    trimmed_non_empty(&claims.user).or_else(|| trimmed_non_empty(&claims.sub))
}

fn decode_jwt_payload(segment: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(segment)
        .or_else(|_| {
            let mut padded = segment.to_string();
            while padded.len() % 4 != 0 {
                padded.push('=');
            }
            URL_SAFE.decode(padded.as_bytes())
        })
        .ok()
}

fn trimmed_non_empty(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[derive(Debug, Deserialize)]
struct JwtActorFragment {
    #[serde(default)]
    user: String,
    #[serde(default)]
    sub: String,
}
