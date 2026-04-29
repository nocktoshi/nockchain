//! Single transition function for REPL UI state (`apply_ui_action`).

use nockapp::NockAppError;

use crate::command::Commands;

use super::action::UiAction;
use super::super::app_state::{PanelFocus, UiState};
use super::super::components::menus::{CT_ERR_ACTIONS, GENERIC_ERR};
use super::super::screens::{ErrorCtx, Screen};

/// Invariants: at most one `Screen::Running`; `balance_job_nonce` monotonic for stale sidebar drops.
pub(crate) fn apply_ui_action(state: &mut UiState, action: UiAction) {
    match action {
        UiAction::Tick => {
            state.ui_fx.frame_clock = state.ui_fx.frame_clock.wrapping_add(1);
        }
        UiAction::TakeToast => {
            state.toast.take();
        }
        UiAction::TogglePanelFocus => {
            state.panel_focus = state.panel_focus.toggle();
        }
        UiAction::SetPanelFocus(f) => {
            state.panel_focus = f;
        }
        UiAction::ReplaceScreen(s) => {
            state.screen = s;
        }
        UiAction::EnterMainFromSplash => {
            state.screen = Screen::Main { sel: 0 };
            state.panel_focus = PanelFocus::Menu;
        }
        UiAction::EnterRunningWalletJob {
            cmd,
            label,
            progress_rx,
        } => {
            if matches!(state.screen, Screen::Running { .. }) {
                return;
            }
            state.balance_job_nonce = state.balance_job_nonce.wrapping_add(1);
            state.balance_panel.loading = false;
            let resume = Box::new(std::mem::replace(&mut state.screen, Screen::Main { sel: 0 }));
            let cmd_clone = cmd.clone();
            state.screen = Screen::Running {
                label,
                restore: resume,
                cmd: cmd_clone,
            };
            state.panel_focus = PanelFocus::Menu;
            state.sync_progress = Some(progress_rx);
        }
        UiAction::BeginBalanceSidebarFetch { progress_rx } => {
            if !matches!(state.screen, Screen::Main { .. }) {
                return;
            }
            if state.balance_panel.loading {
                return;
            }
            state.balance_panel.loading = true;
            state.balance_panel.error = None;
            state.balance_job_nonce = state.balance_job_nonce.wrapping_add(1);
            state.sync_progress = Some(progress_rx);
        }
        UiAction::JobCompleted(result, captured_markdown) => {
            apply_job_completed(state, result, captured_markdown);
        }
        UiAction::BalanceSidebarCompleted {
            nonce,
            result,
            markdown,
        } => {
            apply_balance_sidebar_completed(state, nonce, result, markdown);
        }
        UiAction::NudgeBalanceScroll { delta } => {
            if delta >= 0 {
                state.balance_panel.scroll = state
                    .balance_panel
                    .scroll
                    .saturating_add(delta as u16);
            } else {
                state.balance_panel.scroll = state
                    .balance_panel
                    .scroll
                    .saturating_sub((-delta) as u16);
            }
        }
        UiAction::NudgeOutputScroll { delta } => {
            if delta >= 0 {
                state.output_scroll = state.output_scroll.saturating_add(delta as u16);
            } else {
                state.output_scroll = state.output_scroll.saturating_sub((-delta) as u16);
            }
        }
        UiAction::SetBalanceScroll(y) => {
            state.balance_panel.scroll = y;
        }
        UiAction::SetOutputScroll(y) => {
            state.output_scroll = y;
        }
    }
}

fn apply_balance_sidebar_completed(
    state: &mut UiState,
    nonce: u64,
    result: Result<(), NockAppError>,
    captured_markdown: String,
) {
    state.sync_progress = None;
    state.balance_panel.loading = false;
    if nonce != state.balance_job_nonce {
        return;
    }
    if matches!(state.screen, Screen::Running { .. }) {
        return;
    }
    match result {
        Ok(()) => {
            state.balance_panel.text = captured_markdown;
            state.balance_panel.error = None;
            state.balance_panel.scroll = 0;
        }
        Err(e) => {
            state.balance_panel.error = Some(e.to_string());
            if !captured_markdown.is_empty() {
                state.balance_panel.text = format!("{captured_markdown}\n\n--- error ---\n{e}");
            }
        }
    }
}

fn apply_job_completed(
    state: &mut UiState,
    result: Result<(), NockAppError>,
    captured_markdown: String,
) {
    state.sync_progress = None;
    let placeholder = Screen::Main { sel: 0 };
    let taken = std::mem::replace(&mut state.screen, placeholder);
    match taken {
        Screen::Running { restore, cmd, .. } => match result {
            Ok(()) => {
                state.last_command_output = captured_markdown.clone();
                state.output_scroll = 0;
                state.panel_focus = PanelFocus::Output;
                if matches!(&cmd, Commands::CreateTx { .. }) {
                    state.screen = Screen::Transactions { sel: 0 };
                } else {
                    state.screen = *restore;
                }
                if matches!(&cmd, Commands::ShowBalance) {
                    state.balance_panel.text = captured_markdown;
                    state.balance_panel.error = None;
                    state.balance_panel.scroll = 0;
                }
                state.toast = Some(success_line(&cmd));
            }
            Err(e) => {
                if !captured_markdown.is_empty() {
                    state.last_command_output =
                        format!("{captured_markdown}\n\n--- error ---\n{}", e);
                } else {
                    state.last_command_output = e.to_string();
                }
                state.output_scroll = 0;
                state.panel_focus = PanelFocus::Output;
                if matches!(&cmd, Commands::ShowBalance) {
                    state.balance_panel.error = Some(e.to_string());
                    if !captured_markdown.is_empty() {
                        state.balance_panel.text =
                            format!("{captured_markdown}\n\n--- error ---\n{e}");
                    }
                }
                state.screen = Screen::ErrorScreen {
                    msg: e.to_string(),
                    sel: 0,
                    actions: error_actions_for_command(&cmd),
                    ctx: error_ctx_for_command(&cmd),
                };
            }
        },
        other => {
            state.screen = other;
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
