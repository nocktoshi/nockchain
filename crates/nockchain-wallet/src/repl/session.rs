//! Apply persisted session settings to the REPL runtime.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use super::command_runner::ReplRuntime;
use super::session_client;
use crate::connection::GrpcEndpoint;
use crate::repl::wallet_api::{save_session_state, WalletSessionState};

pub(crate) fn apply_session_to_cli(rt: &ReplRuntime) {
    let session = rt.session_config.read().unwrap();
    if let Ok(endpoint) = GrpcEndpoint::parse(session.public_grpc_server_addr.trim()) {
        rt.cli.lock().unwrap().connection.public_grpc_server_addr = endpoint;
    }
}

pub(crate) fn current_api_listen(rt: &ReplRuntime) -> String {
    rt.session_config.read().unwrap().api_listen.clone()
}

/// Load session from disk, sync via GET, and apply to CLI.
pub(crate) async fn refresh_session_from_api(rt: &ReplRuntime) -> Result<(), String> {
    let listen = current_api_listen(rt);
    let token = rt.api_auth_token.as_ref();
    match session_client::get_session(&listen, token).await {
        Ok(remote) => {
            *rt.session_config.write().unwrap() = remote;
        }
        Err(e) => {
            tracing::debug!("session GET failed, using on-disk state: {e}");
        }
    }
    apply_session_to_cli(rt);
    Ok(())
}

/// POST full session state to the API (persists `session.json` server-side).
pub(crate) async fn commit_session(
    rt: &ReplRuntime,
    next: WalletSessionState,
) -> Result<WalletSessionState, String> {
    let old_listen = current_api_listen(rt);
    let token = rt.api_auth_token.as_ref();
    let updated = session_client::post_session(&old_listen, token, next).await?;
    *rt.session_config.write().unwrap() = updated.clone();
    save_session_state(&rt.session_path, &updated)?;
    apply_session_to_cli(rt);
    Ok(updated)
}

pub(crate) fn session_config_snapshot(rt: &ReplRuntime) -> WalletSessionState {
    rt.session_config.read().unwrap().clone()
}

pub(crate) fn init_session_config(
    session_path: PathBuf,
    cli: &crate::command::WalletCli,
) -> Arc<RwLock<WalletSessionState>> {
    let mut session =
        crate::repl::wallet_api::load_session_state(&session_path).unwrap_or_else(|e| {
            tracing::warn!("loading session.json: {e}, using defaults");
            WalletSessionState::default()
        });
    if session.public_grpc_server_addr.is_empty() {
        session = WalletSessionState::from_wallet_cli(cli);
    }
    if let Err(e) = save_session_state(&session_path, &session) {
        tracing::warn!("writing initial session.json: {e}");
    }
    Arc::new(RwLock::new(session))
}
