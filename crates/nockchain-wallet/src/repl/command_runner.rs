//! REPL-only orchestration around [`crate::dispatch::execute_wallet_command`].
//! CLI entry continues to call dispatch directly with owned [`crate::Wallet`] — unaffected by this module.

use std::sync::Arc;
use std::time::Duration;

use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex, watch};
use wallet_tx_builder::adapter::NormalizedSnapshot;

use super::screens::Screen;
use super::store::{UIStore, UiAction};
use crate::command::Commands;
use crate::dispatch::{execute_wallet_command, DispatchHooks};
use crate::Wallet;

/// [`NockApp::run`] can return before broadcast subscribers (e.g. markdown capture) have polled;
/// snapshot the sink after a few scheduler turns so kernel `markdown` effects are appended.
async fn snapshot_repl_markdown_sink(sink: &Arc<std::sync::Mutex<String>>) -> String {
    let mut captured = sink.lock().unwrap().clone();
    if !captured.is_empty() {
        return captured;
    }
    for _ in 0..96 {
        tokio::task::yield_now().await;
        captured = sink.lock().unwrap().clone();
        if !captured.is_empty() {
            return captured;
        }
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    sink.lock().unwrap().clone()
}

/// Job completion: command result plus captured markdown output for the TUI panel.
pub(crate) type JobCompletion = (Result<(), NockAppError>, String);

/// Background balance sidebar refresh (same `ShowBalance` path as the menu; does not use [`Screen::Running`]).
pub(crate) type BalanceRefreshCompletion =
    (u64, Result<(), NockAppError>, String);

/// Shared wallet + snapshot for spawned REPL jobs (`repl::run` wraps with [`Arc`]).
#[derive(Clone)]
pub(crate) struct ReplRuntime {
    pub wallet: Arc<Mutex<Wallet>>,
    pub snapshot: Arc<Mutex<Option<NormalizedSnapshot>>>,
    pub cli: crate::command::WalletCli,
    /// Single capture buffer for the REPL; kernel markdown driver is installed once and always writes here.
    pub markdown_sink: Arc<std::sync::Mutex<String>>,
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
    {
        let mut g = rt.markdown_sink.lock().unwrap();
        g.clear();
    }
    let (progress_tx, progress_rx) = watch::channel((0usize, 5usize));
    let cmd_clone = cmd.clone();
    let label_s = label.into();
    store.dispatch(UiAction::EnterRunningWalletJob {
        cmd: cmd_clone.clone(),
        label: label_s,
        progress_rx,
    });

    let hooks = DispatchHooks {
        sync_attempt: Some(progress_tx),
        markdown_capture: Some(Arc::clone(&rt.markdown_sink)),
    };

    let rt = rt.clone();
    tokio::task::spawn_local(async move {
        let exec_result = {
            let mut w = rt.wallet.lock().await;
            let mut s = rt.snapshot.lock().await;
            execute_wallet_command(&rt.cli, &mut *w, &cmd_clone, &mut *s, false, hooks).await
        };
        let captured = snapshot_repl_markdown_sink(&rt.markdown_sink).await;
        let _ = done_tx.send((exec_result, captured));
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
    {
        let mut g = rt.markdown_sink.lock().unwrap();
        g.clear();
    }
    let (progress_tx, progress_rx) = watch::channel((0usize, 5usize));
    store.dispatch(UiAction::BeginBalanceSidebarFetch { progress_rx });

    let nonce = store.state.balance_job_nonce;

    let hooks = DispatchHooks {
        sync_attempt: Some(progress_tx),
        markdown_capture: Some(Arc::clone(&rt.markdown_sink)),
    };

    let rt = rt.clone();
    let tx = done_tx.clone();
    tokio::task::spawn_local(async move {
        let exec_result = {
            let mut w = rt.wallet.lock().await;
            let mut s = rt.snapshot.lock().await;
            execute_wallet_command(
                &rt.cli,
                &mut *w,
                &Commands::ShowBalance,
                &mut *s,
                false,
                hooks,
            )
            .await
        };
        let captured = snapshot_repl_markdown_sink(&rt.markdown_sink).await;
        let _ = tx.send((nonce, exec_result, captured));
    });
}

pub(crate) fn apply_balance_sidebar_result(
    store: &mut UIStore,
    nonce: u64,
    result: Result<(), NockAppError>,
    captured_markdown: String,
) {
    store.dispatch(UiAction::BalanceSidebarCompleted {
        nonce,
        result,
        markdown: captured_markdown,
    });
}

pub(crate) fn apply_job_result(
    store: &mut UIStore,
    result: Result<(), NockAppError>,
    captured_markdown: String,
) {
    store.dispatch(UiAction::JobCompleted(result, captured_markdown));
}
