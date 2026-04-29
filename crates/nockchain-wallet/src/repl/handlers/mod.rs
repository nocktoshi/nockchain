//! Keyboard dispatch for the wallet REPL TUI.

mod error;
mod input;
mod menus;
mod prompts;

use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};

use super::app_state::{AppState, PanelFocus};
use super::command_runner::{BalanceRefreshCompletion, JobCompletion, ReplRuntime};
use super::hooks::terminal::Term;
use super::screens::{ReplControl, Screen};
use super::ct_dispatch;
use crate::command::Commands;

use input::{try_balance_scroll_keys, try_output_scroll_keys};

fn schedule_cmd(
    app: &mut AppState,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
    cmd: Commands,
    label: &'static str,
) {
    super::command_runner::schedule_wallet_command(app, rt, done_tx.clone(), cmd, label);
}

pub(super) async fn dispatch_key(
    cli: &crate::command::WalletCli,
    rt: &ReplRuntime,
    app: &mut AppState,
    key: KeyEvent,
    terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
    balance_done_tx: &mpsc::UnboundedSender<BalanceRefreshCompletion>,
) -> Result<ReplControl, NockAppError> {
    if key.kind == KeyEventKind::Release {
        return Ok(ReplControl::Continue);
    }
    if app.toast.take().is_some() {
        return Ok(ReplControl::Continue);
    }
    if matches!(app.screen, Screen::Running { .. }) {
        return Ok(ReplControl::Continue);
    }
    if matches!(app.screen, Screen::Splash) {
        app.screen = Screen::Main { sel: 0 };
        app.panel_focus = PanelFocus::Menu;
        super::command_runner::schedule_balance_sidebar_refresh(app, rt, balance_done_tx);
        return Ok(ReplControl::Continue);
    }
    if key.code == KeyCode::Tab {
        app.panel_focus = app.panel_focus.toggle();
        return Ok(ReplControl::Continue);
    }
    if app.panel_focus == PanelFocus::Balance {
        if key.code == KeyCode::Enter {
            app.panel_focus = PanelFocus::Menu;
            return Ok(ReplControl::Continue);
        }
        if try_balance_scroll_keys(app, key) {
            return Ok(ReplControl::Continue);
        }
        if !matches!(
            key.code,
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q')
        ) {
            return Ok(ReplControl::Continue);
        }
    }
    if app.panel_focus == PanelFocus::Output {
        if key.code == KeyCode::Enter {
            app.panel_focus = PanelFocus::Menu;
            return Ok(ReplControl::Continue);
        }
        if try_output_scroll_keys(app, key) {
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
    match &mut app.screen {
        Screen::Splash => Ok(ReplControl::Continue),
        Screen::Main { .. } => menus::handle_main(cli, &mut app.screen, key).await,
        Screen::Keys { .. } => menus::handle_keys(cli, app, key, rt, terminal, done_tx).await,
        Screen::KeysImport { .. } => menus::handle_keys_import(cli, app, key).await,
        Screen::Notes { .. } => menus::handle_notes(cli, app, key, rt, terminal, done_tx).await,
        Screen::Transactions { .. } => menus::handle_transactions(cli, &mut app.screen, key).await,
        Screen::Watch { .. } => menus::handle_watch(cli, &mut app.screen, key).await,
        Screen::SignVerify { .. } => menus::handle_sign(cli, &mut app.screen, key).await,
        Screen::Settings { .. } => menus::handle_settings(cli, &mut app.screen, key),
        Screen::Quick { .. } => menus::handle_quick(cli, &mut app.screen, key),
        Screen::TextPrompt { .. } => {
            prompts::text_prompt(cli, app, key, rt, terminal, done_tx).await
        }
        Screen::Confirm { .. } => {
            prompts::confirm_prompt(cli, app, key, rt, terminal, done_tx).await
        }
        Screen::CreateTx { .. } => ct_dispatch::handle_create_tx(cli, app, key, rt, done_tx).await,
        Screen::ExitConfirm { .. } => menus::handle_exit_confirm(&mut app.screen, key),
        Screen::ErrorScreen { .. } => {
            error::error_screen(cli, app, key, rt, terminal, done_tx).await
        }
        Screen::Running { .. } => Ok(ReplControl::Continue),
    }
}

/// Insert bracketed-paste clipboard text into the focused field (create-tx lines, REPL prompts, quick line).
pub(super) async fn dispatch_paste(
    _cli: &crate::command::WalletCli,
    app: &mut AppState,
    pasted: String,
    rt: &ReplRuntime,
    balance_done_tx: &mpsc::UnboundedSender<BalanceRefreshCompletion>,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    match screen {
        Screen::Splash => {
            app.screen = Screen::Main { sel: 0 };
            app.panel_focus = PanelFocus::Menu;
            super::command_runner::schedule_balance_sidebar_refresh(app, rt, balance_done_tx);
            Ok(ReplControl::Continue)
        }
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
