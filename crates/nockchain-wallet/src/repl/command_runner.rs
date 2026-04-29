//! REPL-only orchestration around [`crate::dispatch::execute_wallet_command`].
//! CLI entry continues to call dispatch directly with owned [`crate::Wallet`] — unaffected by this module.

use std::sync::Arc;
use std::time::Duration;

use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};
use wallet_tx_builder::adapter::NormalizedSnapshot;

use super::app_state::PanelFocus;
use super::screens::{ErrorCtx, Screen};
use super::tui::{CT_ERR_ACTIONS, GENERIC_ERR};
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
    app: &mut super::app_state::AppState,
    rt: &ReplRuntime,
    done_tx: mpsc::UnboundedSender<JobCompletion>,
    cmd: Commands,
    label: impl Into<String>,
) {
    if matches!(app.screen, Screen::Running { .. }) {
        return;
    }
    app.balance_job_nonce = app.balance_job_nonce.wrapping_add(1);
    app.balance_panel.loading = false;
    let (progress_tx, progress_rx) = tokio::sync::watch::channel((0usize, 5usize));
    {
        let mut g = rt.markdown_sink.lock().unwrap();
        g.clear();
    }
    let hooks = DispatchHooks {
        sync_attempt: Some(progress_tx),
        markdown_capture: Some(Arc::clone(&rt.markdown_sink)),
    };

    let resume = Box::new(std::mem::replace(&mut app.screen, Screen::Main { sel: 0 }));
    let cmd_clone = cmd.clone();
    app.screen = Screen::Running {
        label: label.into(),
        restore: resume,
        cmd: cmd_clone.clone(),
    };
    app.panel_focus = PanelFocus::Menu;
    app.sync_progress = Some(progress_rx);

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
    app: &mut super::app_state::AppState,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<BalanceRefreshCompletion>,
) {
    if !matches!(app.screen, Screen::Main { .. }) {
        return;
    }
    if app.balance_panel.loading {
        return;
    }
    app.balance_panel.loading = true;
    app.balance_panel.error = None;
    app.balance_job_nonce = app.balance_job_nonce.wrapping_add(1);
    let nonce = app.balance_job_nonce;

    {
        let mut g = rt.markdown_sink.lock().unwrap();
        g.clear();
    }
    let (progress_tx, progress_rx) = tokio::sync::watch::channel((0usize, 5usize));
    app.sync_progress = Some(progress_rx);

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
    app: &mut super::app_state::AppState,
    nonce: u64,
    result: Result<(), NockAppError>,
    captured_markdown: String,
) {
    app.sync_progress = None;
    app.balance_panel.loading = false;
    if nonce != app.balance_job_nonce {
        return;
    }
    if matches!(app.screen, Screen::Running { .. }) {
        return;
    }
    match result {
        Ok(()) => {
            app.balance_panel.text = captured_markdown;
            app.balance_panel.error = None;
            app.balance_panel.scroll = 0;
        }
        Err(e) => {
            app.balance_panel.error = Some(e.to_string());
            if !captured_markdown.is_empty() {
                app.balance_panel.text = format!("{captured_markdown}\n\n--- error ---\n{e}");
            }
        }
    }
}

pub(crate) fn apply_job_result(
    app: &mut super::app_state::AppState,
    result: Result<(), NockAppError>,
    captured_markdown: String,
) {
    app.sync_progress = None;
    let placeholder = Screen::Main { sel: 0 };
    let taken = std::mem::replace(&mut app.screen, placeholder);
    match taken {
        Screen::Running { restore, cmd, .. } => match result {
            Ok(()) => {
                app.last_command_output = captured_markdown.clone();
                app.output_scroll = 0;
                app.panel_focus = PanelFocus::Output;
                if matches!(&cmd, Commands::CreateTx { .. }) {
                    app.screen = Screen::Transactions { sel: 0 };
                } else {
                    app.screen = *restore;
                }
                if matches!(&cmd, Commands::ShowBalance) {
                    app.balance_panel.text = captured_markdown;
                    app.balance_panel.error = None;
                    app.balance_panel.scroll = 0;
                }
                app.toast = Some(success_line(&cmd));
            }
            Err(e) => {
                if !captured_markdown.is_empty() {
                    app.last_command_output =
                        format!("{captured_markdown}\n\n--- error ---\n{}", e);
                } else {
                    app.last_command_output = e.to_string();
                }
                app.output_scroll = 0;
                app.panel_focus = PanelFocus::Output;
                if matches!(&cmd, Commands::ShowBalance) {
                    app.balance_panel.error = Some(e.to_string());
                    if !captured_markdown.is_empty() {
                        app.balance_panel.text =
                            format!("{captured_markdown}\n\n--- error ---\n{e}");
                    }
                }
                app.screen = Screen::ErrorScreen {
                    msg: e.to_string(),
                    sel: 0,
                    actions: error_actions_for_command(&cmd),
                    ctx: error_ctx_for_command(&cmd),
                };
            }
        },
        other => {
            app.screen = other;
        }
    }
}

fn error_ctx_for_command(cmd: &Commands) -> ErrorCtx {
    match cmd {
        Commands::CreateTx { .. } => ErrorCtx::CreateTx { cmd: cmd.clone() },
        _ => ErrorCtx::Retry(cmd.clone()),
    }
}

fn error_actions_for_command(cmd: &Commands) -> &'static [&'static str] {
    match cmd {
        Commands::CreateTx { .. } => CT_ERR_ACTIONS,
        _ => GENERIC_ERR,
    }
}

fn success_line(cmd: &Commands) -> String {
    match cmd {
        Commands::ShowBalance => "Balance updated.".into(),
        Commands::Keygen => "New keys generated.".into(),
        Commands::CreateTx { .. } => "Transaction command finished.".into(),
        Commands::ListNotes => "Notes listed.".into(),
        Commands::DeriveChild { .. } => "Derived child key.".into(),
        Commands::ImportKeys { .. } => "Import completed.".into(),
        Commands::ExportKeys => "Export completed.".into(),
        Commands::MigrateV0Notes { .. } => "Migration step finished.".into(),
        Commands::SendTx { .. } => "Send completed.".into(),
        Commands::ShowTx { .. } => "Transaction shown.".into(),
        _ => "Done.".into(),
    }
}
