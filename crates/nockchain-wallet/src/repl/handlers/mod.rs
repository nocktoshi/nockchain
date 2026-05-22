//! Keyboard dispatch for the wallet REPL TUI.

mod error;
mod input;
mod menus;
mod prompts;

use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};

use super::app_state::PanelFocus;
use super::command_runner::{BalanceRefreshCompletion, JobCompletion, ReplRuntime};
use super::hooks::terminal::Term;
use super::screens::{ReplControl, Screen};
use super::store::{UIStore, UiAction};
use super::ct_dispatch;
use crate::command::Commands;

use input::{try_balance_scroll_keys, try_output_scroll_keys};

fn schedule_cmd(
    store: &mut UIStore,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
    cmd: Commands,
    label: &'static str,
) {
    super::command_runner::schedule_wallet_command(store, rt, done_tx.clone(), cmd, label);
}

/// Route screen transitions through [`super::store::apply_ui_action`].
pub(super) fn replace_screen(store: &mut UIStore, screen: Screen) {
    store.dispatch(UiAction::ReplaceScreen(screen));
}

pub(super) async fn dispatch_key(
    cli: &crate::command::WalletCli,
    rt: &ReplRuntime,
    store: &mut UIStore,
    key: KeyEvent,
    terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
    balance_done_tx: &mpsc::UnboundedSender<BalanceRefreshCompletion>,
) -> Result<ReplControl, NockAppError> {
    if key.kind == KeyEventKind::Release {
        return Ok(ReplControl::Continue);
    }
    if store.state.toast.is_some() {
        store.dispatch(UiAction::TakeToast);
        return Ok(ReplControl::Continue);
    }
    if matches!(store.state.screen, Screen::Running { .. }) {
        return Ok(ReplControl::Continue);
    }
    if matches!(store.state.screen, Screen::Splash) {
        store.dispatch(UiAction::EnterMainFromSplash);
        super::command_runner::schedule_balance_sidebar_refresh(store, rt, balance_done_tx);
        return Ok(ReplControl::Continue);
    }
    if key.code == KeyCode::Tab {
        store.dispatch(UiAction::TogglePanelFocus);
        return Ok(ReplControl::Continue);
    }
    if store.state.panel_focus == PanelFocus::Balance {
        if key.code == KeyCode::Enter {
            store.dispatch(UiAction::SetPanelFocus(PanelFocus::Menu));
            return Ok(ReplControl::Continue);
        }
        if try_balance_scroll_keys(store, key) {
            return Ok(ReplControl::Continue);
        }
        if !matches!(
            key.code,
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q')
        ) {
            return Ok(ReplControl::Continue);
        }
    }
    if store.state.panel_focus == PanelFocus::Output {
        if key.code == KeyCode::Enter {
            store.dispatch(UiAction::SetPanelFocus(PanelFocus::Menu));
            return Ok(ReplControl::Continue);
        }
        if try_output_scroll_keys(store, key) {
            return Ok(ReplControl::Continue);
        }
        // Don't let menu handlers see j/k/letters while reading output; Esc/q still reach screens.
        if !matches!(
            key.code,
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q')
        ) {
            return Ok(ReplControl::Continue);
        }
    }
    match &mut store.state.screen {
        Screen::Splash => Ok(ReplControl::Continue),
        Screen::Main { .. } => menus::handle_main(cli, store, key).await,
        Screen::Keys { .. } => menus::handle_keys(cli, store, key, rt, terminal, done_tx).await,
        Screen::KeysImport { .. } => menus::handle_keys_import(cli, store, key).await,
        Screen::Notes { .. } => menus::handle_notes(cli, store, key, rt, terminal, done_tx).await,
        Screen::Transactions { .. } => menus::handle_transactions(cli, store, key).await,
        Screen::Watch { .. } => menus::handle_watch(cli, store, key).await,
        Screen::SignVerify { .. } => menus::handle_sign(cli, store, key).await,
        Screen::Settings { .. } => menus::handle_settings(cli, store, key),
        Screen::Quick { .. } => menus::handle_quick(cli, store, key),
        Screen::TextPrompt { .. } => {
            prompts::text_prompt(cli, store, key, rt, terminal, done_tx).await
        }
        Screen::Confirm { .. } => {
            prompts::confirm_prompt(cli, store, key, rt, terminal, done_tx).await
        }
        Screen::CreateTx { .. } => ct_dispatch::handle_create_tx(cli, store, key, rt, done_tx).await,
        Screen::ExitConfirm { .. } => menus::handle_exit_confirm(store, key),
        Screen::ErrorScreen { .. } => {
            error::error_screen(cli, store, key, rt, terminal, done_tx).await
        }
        Screen::Running { .. } => Ok(ReplControl::Continue),
    }
}

/// Insert bracketed-paste clipboard text into the focused field (create-tx lines, REPL prompts, quick line).
pub(super) async fn dispatch_paste(
    _cli: &crate::command::WalletCli,
    store: &mut UIStore,
    pasted: String,
    rt: &ReplRuntime,
    balance_done_tx: &mpsc::UnboundedSender<BalanceRefreshCompletion>,
) -> Result<ReplControl, NockAppError> {
    if matches!(store.state.screen, Screen::Splash) {
        store.dispatch(UiAction::EnterMainFromSplash);
        super::command_runner::schedule_balance_sidebar_refresh(store, rt, balance_done_tx);
        return Ok(ReplControl::Continue);
    }
    match &mut store.state.screen {
        Screen::TextPrompt { value, then, .. } => {
            if super::paste::text_prompt_allows_multiline(then) {
                super::paste::paste_multiline(value, &pasted);
            } else {
                super::paste::paste_single_line(value, &pasted);
            }
            Ok(ReplControl::Continue)
        }
        Screen::Quick { line } => {
            super::paste::paste_single_line(line, &pasted);
            Ok(ReplControl::Continue)
        }
        Screen::CreateTx { w } => {
            ct_dispatch::apply_paste_to_wizard(w, &pasted);
            Ok(ReplControl::Continue)
        }
        _ => Ok(ReplControl::Continue),
    }
}
