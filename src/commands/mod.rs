pub mod aggregate;
pub mod cli_token;
pub mod client;
pub mod config;
pub mod events;
pub mod schema;
pub mod start;
pub mod token;

pub(crate) fn is_lock_error_message(message: &str) -> bool {
    eventdbx::store::is_lock_error_message(message)
}
