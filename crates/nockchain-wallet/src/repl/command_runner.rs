//! REPL-only orchestration around [`crate::dispatch::execute_wallet_command`].
//! CLI entry continues to call dispatch directly with owned [`crate::Wallet`] — unaffected by this module.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nockapp::NockAppError;
use tokio::sync::{mpsc, watch, Mutex};
use wallet_tx_builder::adapter::NormalizedSnapshot;

use super::screens::Screen;
use super::store::{UIStore, UiAction};
use crate::command::{Commands, WalletCli};
use crate::dispatch::{execute_wallet_command, DispatchHooks};
use crate::repl::wallet_api::{ReplApiJob, WalletSessionState};
use crate::wallet_outcome::{WalletCommandData, WalletEvent};
use crate::Wallet;

/// [`NockApp::run`] can return before the structured effect driver has polled; yield briefly.
async fn snapshot_wallet_events(
    sink: &Arc<std::sync::Mutex<Vec<WalletEvent>>>,
) -> Vec<WalletEvent> {
    let mut events = sink.lock().unwrap().clone();
    if !events.is_empty() {
        return events;
    }
    for _ in 0..96 {
        tokio::task::yield_now().await;
        events = sink.lock().unwrap().clone();
        if !events.is_empty() {
            return events;
        }
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    sink.lock().unwrap().clone()
}

/// Job completion: command result plus structured events for the view layer.
pub(crate) type JobCompletion = (Result<(), NockAppError>, Vec<WalletEvent>);

/// Background balance sidebar refresh (same `ShowBalance` path as the menu; does not use [`Screen::Running`]).
pub(crate) type BalanceRefreshCompletion = (u64, Result<(), NockAppError>, Vec<WalletEvent>);

/// Shared wallet + snapshot for spawned REPL jobs (`repl::run` wraps with [`Arc`]).
#[derive(Clone)]
pub(crate) struct ReplRuntime {
    pub wallet: Arc<Mutex<Wallet>>,
    pub snapshot: Arc<Mutex<Option<NormalizedSnapshot>>>,
    /// Session CLI (connection may be updated from Settings).
    pub cli: Arc<std::sync::Mutex<WalletCli>>,
    /// Structured kernel/API events from `[%raw …]` effects.
    pub wallet_event_sink: Arc<std::sync::Mutex<Vec<WalletEvent>>>,
    /// Session settings persisted in `session.json` and exposed via GET/POST `/v1/wallet/state`.
    pub session_config: Arc<RwLock<WalletSessionState>>,
    pub session_path: PathBuf,
    /// Secret bearer token for this REPL session only (never written to disk).
    pub api_auth_token: Arc<str>,
    /// Channel to the background HTTP server (jobs executed on this REPL [`LocalSet`]).
    pub api_job_tx: mpsc::Sender<ReplApiJob>,
    /// Background HTTP listener (restarted when session `api_listen` changes).
    pub api_server: Arc<std::sync::Mutex<Option<crate::repl::wallet_api::ApiServerHandle>>>,
}

/// Run a wallet command on the shared REPL runtime (TUI jobs and JSON API).
pub(crate) async fn run_command_on_runtime(
    rt: &ReplRuntime,
    cli: &WalletCli,
    command: Commands,
    sync_attempt: Option<watch::Sender<(usize, usize)>>,
) -> Result<WalletCommandData, NockAppError> {
    let mut hooks = DispatchHooks::structured(Arc::clone(&rt.wallet_event_sink));
    if let Some(tx) = sync_attempt {
        hooks = hooks.with_sync_attempt(tx);
    }
    let outcome = {
        let mut w = rt.wallet.lock().await;
        let mut s = rt.snapshot.lock().await;
        execute_wallet_command(cli, &mut *w, &command, &mut *s, false, hooks).await
    };
    finalize_outcome(outcome, &rt.wallet_event_sink).await
}

async fn finalize_outcome(
    outcome: Result<WalletCommandData, NockAppError>,
    wallet_event_sink: &Arc<std::sync::Mutex<Vec<WalletEvent>>>,
) -> Result<WalletCommandData, NockAppError> {
    match outcome {
        Ok(mut data) => {
            if data.events.is_empty() {
                data.events = snapshot_wallet_events(wallet_event_sink).await;
            }
            Ok(data)
        }
        Err(e) => Err(e),
    }
}

/// Queue a wallet command: [`Screen::Running`] + in-TUI progress; work runs without leaving the alternate screen.
pub(crate) fn schedule_wallet_command(
    store: &mut UIStore,
    rt: &ReplRuntime,
    done_tx: mpsc::UnboundedSender<JobCompletion>,
    cmd: Commands,
    label: impl Into<String>,
) {
    if matches!(store.state.screen, Screen::Running { .. }) {
        return;
    }
    rt.wallet_event_sink.lock().unwrap().clear();
    let (progress_tx, progress_rx) = watch::channel((0usize, 5usize));
    let cmd_clone = cmd.clone();
    let label_s = label.into();
    store.dispatch(UiAction::EnterRunningWalletJob {
        cmd: cmd_clone.clone(),
        label: label_s,
        progress_rx,
    });

    let rt = rt.clone();
    tokio::task::spawn_local(async move {
        let cli = rt.cli.lock().unwrap().clone();
        let outcome = run_command_on_runtime(&rt, &cli, cmd_clone, Some(progress_tx)).await;
        let events = outcome
            .as_ref()
            .map(|d| d.events.clone())
            .unwrap_or_default();
        let exec_result = outcome.map(|_| ());
        let _ = done_tx.send((exec_result, events));
    });
}

/// Refresh balance text for the main-menu sidebar (does not swap to [`Screen::Running`]).
pub(crate) fn schedule_balance_sidebar_refresh(
    store: &mut UIStore,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<BalanceRefreshCompletion>,
) {
    if !matches!(store.state.screen, Screen::Main { .. }) {
        return;
    }
    if store.state.balance_panel.loading {
        return;
    }
    rt.wallet_event_sink.lock().unwrap().clear();
    let (progress_tx, progress_rx) = watch::channel((0usize, 5usize));
    store.dispatch(UiAction::BeginBalanceSidebarFetch { progress_rx });

    let nonce = store.state.balance_job_nonce;

    let rt = rt.clone();
    let tx = done_tx.clone();
    tokio::task::spawn_local(async move {
        let cli = rt.cli.lock().unwrap().clone();
        let outcome =
            run_command_on_runtime(&rt, &cli, Commands::ShowBalance, Some(progress_tx)).await;
        let events = outcome
            .as_ref()
            .map(|d| d.events.clone())
            .unwrap_or_default();
        let exec_result = outcome.map(|_| ());
        let _ = tx.send((nonce, exec_result, events));
    });
}

pub(crate) fn apply_balance_sidebar_result(
    store: &mut UIStore,
    nonce: u64,
    result: Result<(), NockAppError>,
    events: Vec<WalletEvent>,
) {
    store.dispatch(UiAction::BalanceSidebarCompleted {
        nonce,
        result,
        events,
    });
}

pub(crate) fn apply_job_result(
    store: &mut UIStore,
    result: Result<(), NockAppError>,
    events: Vec<WalletEvent>,
) {
    store.dispatch(UiAction::JobCompleted { result, events });
}
