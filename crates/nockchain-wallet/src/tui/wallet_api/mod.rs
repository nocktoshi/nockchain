//! JSON HTTP API for wallet commands and persisted TUI session settings.

mod auth;
mod executor;
mod server;
mod state;

pub(crate) use auth::generate_api_token;
pub(crate) use executor::{restart_api_server_if_listen_changed, run_api_job_loop};
pub(crate) use server::{spawn_http_server, ApiServerHandle, TuiApiJob};
pub(crate) use state::{
    load_session_state, save_session_state, WalletSessionState, SESSION_FILE_NAME,
    WALLET_SESSION_SCHEMA,
};
