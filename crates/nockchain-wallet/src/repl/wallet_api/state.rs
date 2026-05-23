//! Persisted REPL / API session settings (`session.json` under the wallet data dir).

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::command::WalletCli;
use crate::connection::GrpcEndpoint;

pub const WALLET_SESSION_SCHEMA: &str = "wallet-session-v1";
pub const SESSION_FILE_NAME: &str = "session.json";
/// Default JSON API bind address when no `session.json` exists yet.
pub const DEFAULT_API_LISTEN: &str = "127.0.0.1:8765";

/// Settings shared by the REPL TUI and the JSON API (stored on disk).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletSessionState {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    /// Public gRPC server (`host[:port]` or full URI).
    #[serde(default)]
    pub public_grpc_server_addr: String,
    /// JSON API bind address (`host:port`).
    #[serde(default = "default_api_listen_field")]
    pub api_listen: String,
}

fn default_schema_version() -> String {
    WALLET_SESSION_SCHEMA.to_string()
}

fn default_api_listen_field() -> String {
    DEFAULT_API_LISTEN.to_string()
}

impl Default for WalletSessionState {
    fn default() -> Self {
        Self {
            schema_version: WALLET_SESSION_SCHEMA.to_string(),
            public_grpc_server_addr: "https://nockchain-api.zorp.io".to_string(),
            api_listen: DEFAULT_API_LISTEN.to_string(),
        }
    }
}

impl WalletSessionState {
    pub fn from_wallet_cli(cli: &WalletCli) -> Self {
        Self {
            schema_version: WALLET_SESSION_SCHEMA.to_string(),
            public_grpc_server_addr: cli.connection.public_grpc_server_addr.to_string(),
            api_listen: DEFAULT_API_LISTEN.to_string(),
        }
    }
}

pub fn load_session_state(path: &Path) -> Result<WalletSessionState, String> {
    if !path.exists() {
        return Ok(WalletSessionState::default());
    }
    let raw = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    serde_json::from_str(&raw).map_err(|e| format!("parse {}: {e}", path.display()))
}

pub fn save_session_state(path: &Path, state: &WalletSessionState) -> Result<(), String> {
    validate_session_state(state)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let raw =
        serde_json::to_string_pretty(state).map_err(|e| format!("encode session state: {e}"))?;
    std::fs::write(path, raw).map_err(|e| format!("write {}: {e}", path.display()))
}

pub fn validate_session_state(state: &WalletSessionState) -> Result<(), String> {
    if state.schema_version != WALLET_SESSION_SCHEMA {
        return Err(format!(
            "unsupported schema_version {:?} (expected {WALLET_SESSION_SCHEMA})",
            state.schema_version
        ));
    }
    GrpcEndpoint::parse(state.public_grpc_server_addr.trim())?;
    state
        .api_listen
        .trim()
        .parse::<std::net::SocketAddr>()
        .map_err(|e| format!("invalid api_listen: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_json() {
        let state = WalletSessionState::default();
        let raw = serde_json::to_string(&state).unwrap();
        let back: WalletSessionState = serde_json::from_str(&raw).unwrap();
        assert_eq!(state, back);
    }
}
