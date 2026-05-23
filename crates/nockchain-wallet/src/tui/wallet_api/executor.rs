//! TUI JSON API: HTTP listener lifecycle and command execution.

use std::sync::Arc;

use clap::Parser;
use tokio::sync::mpsc;

use super::{spawn_http_server, ApiServerHandle, TuiApiJob};
use crate::command::{Commands, WalletCli};
use crate::tui::command_runner::{self, TuiRuntime};
use crate::tui::session::current_api_listen;
use crate::wallet_outcome::WalletCommandJsonResponse;

/// Run a wallet command from the TUI JSON API (`argv` tokens, no binary name).
async fn execute_tui_api_command(
    rt: &TuiRuntime,
    argv: Vec<String>,
) -> WalletCommandJsonResponse {
    rt.wallet_event_sink.lock().unwrap().clear();

    let mut args = vec!["nockchain-wallet".to_string()];
    args.extend(argv);

    let parsed = match WalletCli::try_parse_from(args) {
        Ok(mut cli) => {
            let session = rt.cli.lock().unwrap();
            cli.boot = session.boot.clone();
            cli.verbose = session.verbose;
            cli.fakenet = session.fakenet;
            cli.connection = session.connection.clone();
            cli
        }
        Err(e) => {
            return WalletCommandJsonResponse {
                schema_version: crate::wallet_outcome::WALLET_OUTCOME_SCHEMA,
                success: None,
                error: Some(e.to_string()),
            };
        }
    };

    if matches!(parsed.command, Commands::Tui) {
        return WalletCommandJsonResponse {
            schema_version: crate::wallet_outcome::WALLET_OUTCOME_SCHEMA,
            success: None,
            error: Some("tui is not a valid API command".into()),
        };
    }

    let outcome =
        command_runner::run_command_on_runtime(rt, &parsed, parsed.command.clone(), None, None)
            .await;
    WalletCommandJsonResponse::from_outcome(outcome)
}

/// Start (or restart) the JSON API listener using session `api_listen`.
pub(crate) fn restart_api_server(rt: &TuiRuntime, handle_slot: &mut Option<ApiServerHandle>) {
    if let Some(prev) = handle_slot.take() {
        prev.stop();
    }
    let listen = current_api_listen(rt);
    match spawn_http_server(
        listen,
        rt.api_job_tx.clone(),
        rt.session_path.clone(),
        Arc::clone(&rt.session_config),
        Arc::clone(&rt.api_auth_token),
    ) {
        Ok(h) => {
            *handle_slot = Some(h);
        }
        Err(e) => {
            tracing::warn!("wallet API not listening: {e}");
        }
    }
}

/// Process HTTP API jobs on the TUI [`LocalSet`] (same wallet + capture sinks as the TUI).
pub(crate) async fn run_api_job_loop(rt: TuiRuntime, mut job_rx: mpsc::Receiver<TuiApiJob>) {
    let mut server = rt.api_server.lock().unwrap().take();
    restart_api_server(&rt, &mut server);
    *rt.api_server.lock().unwrap() = server;

    while let Some(job) = job_rx.recv().await {
        let resp = execute_tui_api_command(&rt, job.argv).await;
        let _ = job.resp.send(resp);
    }

    if let Some(h) = rt.api_server.lock().unwrap().take() {
        h.stop();
    }
}

/// After POST changes `api_listen`, rebind the HTTP listener.
pub(crate) fn restart_api_server_if_listen_changed(rt: &TuiRuntime, previous_listen: &str) {
    let now = current_api_listen(rt);
    if now != previous_listen {
        let mut server = rt.api_server.lock().unwrap().take();
        restart_api_server(rt, &mut server);
        *rt.api_server.lock().unwrap() = server;
    }
}
