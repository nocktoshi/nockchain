//! Keyboard dispatch for the wallet REPL TUI.

use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};
use tracing::warn;

use super::app_state::{AppState, PanelFocus};
use super::command_runner::{BalanceRefreshCompletion, JobCompletion, ReplRuntime};
use super::create_tx::CreateTxWizard;
use super::screens::{ConfirmThen, ErrorCtx, ReplControl, Screen, TextThen};
use super::tui::{
    Term, BOOL, IMPORT_SRC, KEYS_MENU, MAIN_MENU, NOTES_MENU, SETTINGS_MENU, SIGN_MENU, TX_MENU,
    WATCH_MENU,
};
use super::{ct_dispatch, normalize_slash_cmd};
use crate::command::{Commands, WalletCli, WatchSubcommand};

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
    cli: &WalletCli,
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
        Screen::Main { .. } => handle_main(cli, &mut app.screen, key).await,
        Screen::Keys { .. } => handle_keys(cli, app, key, rt, terminal, done_tx).await,
        Screen::KeysImport { .. } => handle_keys_import(cli, app, key).await,
        Screen::Notes { .. } => handle_notes(cli, app, key, rt, terminal, done_tx).await,
        Screen::Transactions { .. } => handle_transactions(cli, &mut app.screen, key).await,
        Screen::Watch { .. } => handle_watch(cli, &mut app.screen, key).await,
        Screen::SignVerify { .. } => handle_sign(cli, &mut app.screen, key).await,
        Screen::Settings { .. } => handle_settings(cli, &mut app.screen, key),
        Screen::Quick { .. } => handle_quick(cli, &mut app.screen, key),
        Screen::TextPrompt { .. } => text_prompt(cli, app, key, rt, terminal, done_tx).await,
        Screen::Confirm { .. } => confirm_prompt(cli, app, key, rt, terminal, done_tx).await,
        Screen::CreateTx { .. } => ct_dispatch::handle_create_tx(cli, app, key, rt, done_tx).await,
        Screen::ExitConfirm { .. } => handle_exit_confirm(&mut app.screen, key),
        Screen::ErrorScreen { .. } => error_screen(cli, app, key, rt, terminal, done_tx).await,
        Screen::Running { .. } => Ok(ReplControl::Continue),
    }
}

/// Insert bracketed-paste clipboard text into the focused field (create-tx lines, REPL prompts, quick line).
pub(super) async fn dispatch_paste(
    _cli: &WalletCli,
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

/// ↑/↓ when the balance sidebar is focused (scroll clamp in `draw_ui`).
fn try_balance_scroll_keys(app: &mut AppState, key: KeyEvent) -> bool {
    if app.panel_focus != PanelFocus::Balance {
        return false;
    }
    if !matches!(app.screen, Screen::Main { .. }) {
        return false;
    }
    const LINE_STEP: u16 = 3;
    const PAGE_STEP: u16 = 6;
    match key.code {
        KeyCode::Up => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Down => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_add(LINE_STEP);
            true
        }
        KeyCode::PageUp => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_sub(PAGE_STEP);
            true
        }
        KeyCode::PageDown => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_add(PAGE_STEP);
            true
        }
        KeyCode::Home => {
            app.balance_panel.scroll = 0;
            true
        }
        KeyCode::End => {
            app.balance_panel.scroll = u16::MAX;
            true
        }
        KeyCode::Char('k') => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Char('j') => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_add(LINE_STEP);
            true
        }
        _ => false,
    }
}

/// ↑/↓ and page keys when the output panel is focused (scroll clamp in `draw_ui`).
fn try_output_scroll_keys(app: &mut AppState, key: KeyEvent) -> bool {
    if app.panel_focus != PanelFocus::Output {
        return false;
    }
    if matches!(app.screen, Screen::Running { .. }) {
        return false;
    }
    const LINE_STEP: u16 = 3;
    const PAGE_STEP: u16 = 6;
    match key.code {
        KeyCode::Up => {
            app.output_scroll = app.output_scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Down => {
            app.output_scroll = app.output_scroll.saturating_add(LINE_STEP);
            true
        }
        KeyCode::PageUp => {
            app.output_scroll = app.output_scroll.saturating_sub(PAGE_STEP);
            true
        }
        KeyCode::PageDown => {
            app.output_scroll = app.output_scroll.saturating_add(PAGE_STEP);
            true
        }
        KeyCode::Home => {
            app.output_scroll = 0;
            true
        }
        KeyCode::End => {
            app.output_scroll = u16::MAX;
            true
        }
        KeyCode::Char('k') => {
            app.output_scroll = app.output_scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Char('j') => {
            app.output_scroll = app.output_scroll.saturating_add(LINE_STEP);
            true
        }
        _ => false,
    }
}

fn esc_back(code: KeyCode) -> bool {
    matches!(code, KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q'))
}

fn list_activate(sel: &mut usize, len: usize, key: KeyCode) -> Result<Option<usize>, ()> {
    match key {
        KeyCode::Up | KeyCode::Char('k') => {
            *sel = sel.saturating_sub(1);
            Err(())
        }
        KeyCode::Down | KeyCode::Char('j') => {
            *sel = (*sel + 1).min(len.saturating_sub(1));
            Err(())
        }
        KeyCode::Enter => Ok(Some(*sel)),
        _ => Ok(None),
    }
}

fn edit_line(line: &mut String, key: KeyEvent) {
    match key.code {
        KeyCode::Char(c) => line.push(c),
        KeyCode::Backspace => {
            line.pop();
        }
        _ => {}
    }
}

async fn handle_main(
    _cli: &WalletCli,
    screen: &mut Screen,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = std::mem::replace(screen, Screen::Main { sel: 0 });
    match taken {
        Screen::Main { mut sel } => match list_activate(&mut sel, MAIN_MENU.len(), key.code) {
            Err(()) => {
                *screen = Screen::Main { sel };
                Ok(ReplControl::Continue)
            }
            Ok(None) => {
                if esc_back(key.code) {
                    *screen = Screen::Main { sel };
                    return Ok(ReplControl::Quit);
                }
                *screen = Screen::Main { sel };
                Ok(ReplControl::Continue)
            }
            Ok(Some(i)) => {
                *screen = match i {
                    0 => Screen::Keys { sel: 0 },
                    1 => Screen::Notes { sel: 0 },
                    2 => Screen::Transactions { sel: 0 },
                    3 => Screen::Watch { sel: 0 },
                    4 => Screen::SignVerify { sel: 0 },
                    5 => Screen::Settings { sel: 0 },
                    6 => Screen::Quick {
                        line: String::new(),
                    },
                    7 => Screen::ExitConfirm { sel: 1 },
                    _ => Screen::Main { sel },
                };
                Ok(ReplControl::Continue)
            }
        },
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

async fn handle_keys(
    _cli: &WalletCli,
    app: &mut AppState,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    let taken = std::mem::replace(screen, Screen::Keys { sel: 0 });
    match taken {
        Screen::Keys { mut sel } => {
            if esc_back(key.code) {
                *screen = Screen::Main { sel: 0 };
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, KEYS_MENU.len(), key.code) {
                Err(()) => {
                    *screen = Screen::Keys { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    *screen = Screen::Keys { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(app, rt, done_tx, Commands::Keygen, "Keygen");
                        }
                        1 => {
                            *screen = Screen::TextPrompt {
                                title: "Child index (u64)".into(),
                                value: String::new(),
                                then: TextThen::KeysDeriveIndex,
                            };
                        }
                        2 => *screen = Screen::KeysImport { sel: 0 },
                        3 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(app, rt, done_tx, Commands::ExportKeys, "ExportKeys");
                        }
                        4 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(
                                app,
                                rt,
                                done_tx,
                                Commands::ShowSeedphrase,
                                "ShowSeedphrase",
                            );
                        }
                        5 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(
                                app,
                                rt,
                                done_tx,
                                Commands::ShowMasterZPub,
                                "ShowMasterZPub",
                            );
                        }
                        6 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(
                                app,
                                rt,
                                done_tx,
                                Commands::ShowMasterZPrv,
                                "ShowMasterZPrv",
                            );
                        }
                        7 => {
                            *screen = Screen::Confirm {
                                title: "Include values at each path?".into(),
                                sel: 1,
                                labels: BOOL,
                                then: ConfirmThen::KeysKeyTree,
                            };
                        }
                        8 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(
                                app,
                                rt,
                                done_tx,
                                Commands::ListActiveAddresses,
                                "ListActiveAddresses",
                            );
                        }
                        9 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(
                                app,
                                rt,
                                done_tx,
                                Commands::ListMasterAddresses,
                                "ListMasterAddresses",
                            );
                        }
                        10 => {
                            *screen = Screen::TextPrompt {
                                title: "Address (base58)".into(),
                                value: String::new(),
                                then: TextThen::KeysSetActive,
                            };
                        }
                        11 => {
                            *screen = Screen::TextPrompt {
                                title: "Path to exported master pubkey file".into(),
                                value: String::new(),
                                then: TextThen::KeysImportMaster,
                            };
                        }
                        12 => {
                            *screen = Screen::Keys { sel };
                            schedule_cmd(
                                app,
                                rt,
                                done_tx,
                                Commands::ExportMasterPubkey,
                                "ExportMasterPubkey",
                            );
                        }
                        13 => *screen = Screen::Main { sel: 0 },
                        _ => {
                            *screen = Screen::Keys { sel };
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

async fn handle_keys_import(
    _cli: &WalletCli,
    app: &mut AppState,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    let taken = std::mem::replace(screen, Screen::KeysImport { sel: 0 });
    match taken {
        Screen::KeysImport { mut sel } => {
            if esc_back(key.code) {
                *screen = Screen::Keys { sel: 2 };
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, IMPORT_SRC.len(), key.code) {
                Err(()) => {
                    *screen = Screen::KeysImport { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    *screen = Screen::KeysImport { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            *screen = Screen::TextPrompt {
                                title: "Path to jammed keys file".into(),
                                value: String::new(),
                                then: TextThen::KeysImportFile,
                            };
                        }
                        1 => {
                            *screen = Screen::TextPrompt {
                                title: "Extended key (zprv/zpub…)".into(),
                                value: String::new(),
                                then: TextThen::KeysImportExtended,
                            };
                        }
                        2 => {
                            *screen = Screen::TextPrompt {
                                title: "Seed phrase".into(),
                                value: String::new(),
                                then: TextThen::KeysImportSeed,
                            };
                        }
                        3 => *screen = Screen::Keys { sel: 2 },
                        _ => {
                            *screen = Screen::KeysImport { sel };
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

async fn handle_notes(
    _cli: &WalletCli,
    app: &mut AppState,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    let taken = std::mem::replace(screen, Screen::Notes { sel: 0 });
    match taken {
        Screen::Notes { mut sel } => {
            if esc_back(key.code) {
                *screen = Screen::Main { sel: 0 };
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, NOTES_MENU.len(), key.code) {
                Err(()) => {
                    *screen = Screen::Notes { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    *screen = Screen::Notes { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            schedule_cmd(app, rt, done_tx, Commands::ListNotes, "ListNotes");
                        }
                        1 => {
                            *screen = Screen::TextPrompt {
                                title: "Public key / filter".into(),
                                value: String::new(),
                                then: TextThen::NotesListByAddr,
                            };
                        }
                        2 => {
                            *screen = Screen::TextPrompt {
                                title: "Public key".into(),
                                value: String::new(),
                                then: TextThen::NotesListCsv,
                            };
                        }
                        3 => {
                            schedule_cmd(app, rt, done_tx, Commands::ShowBalance, "ShowBalance");
                        }
                        4 => *screen = Screen::Main { sel: 0 },
                        _ => {
                            *screen = Screen::Notes { sel };
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

async fn handle_transactions(
    _cli: &WalletCli,
    screen: &mut Screen,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = std::mem::replace(screen, Screen::Transactions { sel: 0 });
    match taken {
        Screen::Transactions { mut sel } => {
            if esc_back(key.code) {
                *screen = Screen::Main { sel: 0 };
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, TX_MENU.len(), key.code) {
                Err(()) => {
                    *screen = Screen::Transactions { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    *screen = Screen::Transactions { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            *screen = Screen::CreateTx {
                                w: CreateTxWizard::new(),
                            };
                        }
                        1 => {
                            *screen = Screen::TextPrompt {
                                title: "Transaction file path".into(),
                                value: String::new(),
                                then: TextThen::TxSendPath,
                            };
                        }
                        2 => {
                            *screen = Screen::TextPrompt {
                                title: "Transaction file path".into(),
                                value: String::new(),
                                then: TextThen::TxShowPath,
                            };
                        }
                        3 => {
                            *screen = Screen::TextPrompt {
                                title: "Transaction file path".into(),
                                value: String::new(),
                                then: TextThen::TxSignMultisigTxFile,
                            };
                        }
                        4 => {
                            *screen = Screen::TextPrompt {
                                title: "Destination v1 address (base58)".into(),
                                value: String::new(),
                                then: TextThen::TxMigrateDest,
                            };
                        }
                        5 => *screen = Screen::Main { sel: 0 },
                        _ => {
                            *screen = Screen::Transactions { sel };
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

async fn handle_watch(
    _cli: &WalletCli,
    screen: &mut Screen,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = std::mem::replace(screen, Screen::Watch { sel: 0 });
    match taken {
        Screen::Watch { mut sel } => {
            if esc_back(key.code) {
                *screen = Screen::Main { sel: 0 };
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, WATCH_MENU.len(), key.code) {
                Err(()) => {
                    *screen = Screen::Watch { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    *screen = Screen::Watch { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            *screen = Screen::TextPrompt {
                                title: "Address or pubkey (base58)".into(),
                                value: String::new(),
                                then: TextThen::WatchAddr,
                            };
                        }
                        1 => {
                            *screen = Screen::TextPrompt {
                                title: "Pubkey (base58)".into(),
                                value: String::new(),
                                then: TextThen::WatchPubkey,
                            };
                        }
                        2 => {
                            *screen = Screen::TextPrompt {
                                title: "Threshold (m)".into(),
                                value: String::new(),
                                then: TextThen::TxMultisigThreshold,
                            };
                        }
                        3 => *screen = Screen::Main { sel: 0 },
                        _ => {
                            *screen = Screen::Watch { sel };
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

async fn handle_sign(
    _cli: &WalletCli,
    screen: &mut Screen,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = std::mem::replace(screen, Screen::SignVerify { sel: 0 });
    match taken {
        Screen::SignVerify { mut sel } => {
            if esc_back(key.code) {
                *screen = Screen::Main { sel: 0 };
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, SIGN_MENU.len(), key.code) {
                Err(()) => {
                    *screen = Screen::SignVerify { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    *screen = Screen::SignVerify { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            *screen = Screen::TextPrompt {
                                title: "Message to sign".into(),
                                value: String::new(),
                                then: TextThen::SignMsgStepMessage,
                            };
                        }
                        1 => {
                            *screen = Screen::TextPrompt {
                                title: "Message (plain text)".into(),
                                value: String::new(),
                                then: TextThen::VerifyMsgM,
                            };
                        }
                        2 => {
                            *screen = Screen::TextPrompt {
                                title: "Hash (base58)".into(),
                                value: String::new(),
                                then: TextThen::SignHashGetHash,
                            };
                        }
                        3 => {
                            *screen = Screen::TextPrompt {
                                title: "Hash (base58)".into(),
                                value: String::new(),
                                then: TextThen::VerifyHashFirst,
                            };
                        }
                        4 => *screen = Screen::Main { sel: 0 },
                        _ => {
                            *screen = Screen::SignVerify { sel };
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

fn handle_settings(
    cli: &WalletCli,
    screen: &mut Screen,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = std::mem::replace(screen, Screen::Settings { sel: 0 });
    match taken {
        Screen::Settings { mut sel } => {
            if esc_back(key.code) {
                *screen = Screen::Main { sel: 0 };
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, SETTINGS_MENU.len(), key.code) {
                Err(()) => {
                    *screen = Screen::Settings { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    *screen = Screen::Settings { sel };
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            super::tui::log_help(cli.verbose);
                        }
                        1 => {
                            super::tui::log_verbose_info();
                        }
                        2 => *screen = Screen::Main { sel: 0 },
                        _ => {
                            *screen = Screen::Settings { sel };
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

fn handle_quick(
    cli: &WalletCli,
    screen: &mut Screen,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = std::mem::replace(
        screen,
        Screen::Quick {
            line: String::new(),
        },
    );
    match taken {
        Screen::Quick { mut line } => {
            match key.code {
                KeyCode::Esc => {
                    *screen = Screen::Main { sel: 0 };
                }
                KeyCode::Enter => {
                    let cmd = normalize_slash_cmd(&line);
                    match cmd.to_ascii_lowercase().as_str() {
                        "exit" | "quit" => return Ok(ReplControl::Quit),
                        "help" => {
                            super::tui::log_help(cli.verbose);
                        }
                        "verbose" => {
                            super::tui::log_verbose_info();
                        }
                        "menu" => {
                            *screen = Screen::Main { sel: 0 };
                            return Ok(ReplControl::Continue);
                        }
                        "" => {}
                        other => {
                            warn!(
                                "Unknown command {:?}; type `help` or open the Wallet menu.",
                                other
                            );
                        }
                    }
                    line.clear();
                    *screen = Screen::Quick { line };
                }
                _ => {
                    edit_line(&mut line, key);
                    *screen = Screen::Quick { line };
                }
            }
            Ok(ReplControl::Continue)
        }
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

fn handle_exit_confirm(screen: &mut Screen, key: KeyEvent) -> Result<ReplControl, NockAppError> {
    let taken = std::mem::replace(screen, Screen::ExitConfirm { sel: 0 });
    match taken {
        Screen::ExitConfirm { mut sel } => match list_activate(&mut sel, BOOL.len(), key.code) {
            Err(()) => {
                *screen = Screen::ExitConfirm { sel };
                Ok(ReplControl::Continue)
            }
            Ok(None) => {
                if esc_back(key.code) {
                    *screen = Screen::Main { sel: 0 };
                } else {
                    *screen = Screen::ExitConfirm { sel };
                }
                Ok(ReplControl::Continue)
            }
            Ok(Some(i)) => {
                if i == 0 {
                    return Ok(ReplControl::Quit);
                }
                *screen = Screen::Main { sel: 0 };
                Ok(ReplControl::Continue)
            }
        },
        other => {
            *screen = other;
            Ok(ReplControl::Continue)
        }
    }
}

async fn error_screen(
    _cli: &WalletCli,
    app: &mut AppState,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    let state = std::mem::replace(screen, Screen::Main { sel: 0 });
    let (msg, mut sel, actions, ctx) = match state {
        Screen::ErrorScreen {
            msg,
            sel,
            actions,
            ctx,
        } => (msg, sel, actions, ctx),
        other => {
            *screen = other;
            return Ok(ReplControl::Continue);
        }
    };

    if esc_back(key.code) {
        *screen = Screen::Main { sel: 0 };
        return Ok(ReplControl::Continue);
    }

    match list_activate(&mut sel, actions.len(), key.code) {
        Err(()) => {
            *screen = Screen::ErrorScreen {
                msg,
                sel,
                actions,
                ctx,
            };
            Ok(ReplControl::Continue)
        }
        Ok(None) => {
            *screen = Screen::ErrorScreen {
                msg,
                sel,
                actions,
                ctx,
            };
            Ok(ReplControl::Continue)
        }
        Ok(Some(i)) => {
            match &ctx {
                ErrorCtx::Retry(cmd) => match i {
                    0 => {
                        schedule_cmd(app, rt, done_tx, cmd.clone(), "Retry");
                    }
                    1 => {
                        *screen = Screen::Main { sel: 0 };
                    }
                    _ => {
                        *screen = Screen::ErrorScreen {
                            msg,
                            sel,
                            actions,
                            ctx,
                        };
                    }
                },
                ErrorCtx::CreateTx { cmd } => {
                    if !matches!(cmd, Commands::CreateTx { .. }) {
                        *screen = Screen::Main { sel: 0 };
                    } else {
                        match i {
                            0 => {
                                schedule_cmd(app, rt, done_tx, cmd.clone(), "Retry");
                            }
                            1 => {
                                if let Some(w) = CreateTxWizard::from_command(&cmd) {
                                    *screen = Screen::CreateTx { w };
                                } else {
                                    *screen = Screen::Transactions { sel: 0 };
                                }
                            }
                            2 => {
                                *screen = Screen::CreateTx {
                                    w: CreateTxWizard::new(),
                                };
                            }
                            3 => {
                                *screen = Screen::Transactions { sel: 0 };
                            }
                            _ => {
                                *screen = Screen::ErrorScreen {
                                    msg,
                                    sel,
                                    actions,
                                    ctx,
                                };
                            }
                        }
                    }
                }
            }
            Ok(ReplControl::Continue)
        }
    }
}

async fn text_prompt(
    _cli: &WalletCli,
    app: &mut AppState,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    let state = std::mem::replace(screen, Screen::Main { sel: 0 });
    let (title, mut value, then) = match state {
        Screen::TextPrompt { title, value, then } => (title, value, then),
        other => {
            *screen = other;
            return Ok(ReplControl::Continue);
        }
    };
    if esc_back(key.code) {
        *screen = Screen::Main { sel: 0 };
        return Ok(ReplControl::Continue);
    }
    if key.code == KeyCode::Enter {
        let v = value.trim().to_string();
        match then {
            TextThen::KeysDeriveIndex => match v.parse::<u64>() {
                Ok(index) => {
                    *screen = Screen::Confirm {
                        title: "Hardened?".into(),
                        sel: 1,
                        labels: BOOL,
                        then: ConfirmThen::KeysDeriveAfterIndex { index },
                    };
                }
                Err(e) => warn!("Invalid index: {e}"),
            },
            TextThen::KeysDeriveRun { index, hardened } => {
                let label = if v.is_empty() { None } else { Some(v) };
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::DeriveChild {
                        index,
                        hardened,
                        label,
                    },
                    "DeriveChild",
                );
            }
            TextThen::KeysImportFile => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::ImportKeys {
                        file: Some(v),
                        key: None,
                        seedphrase: None,
                        version: None,
                    },
                    "ImportKeys",
                );
            }
            TextThen::KeysImportExtended => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::ImportKeys {
                        file: None,
                        key: Some(v),
                        seedphrase: None,
                        version: None,
                    },
                    "ImportKeys",
                );
            }
            TextThen::KeysImportSeed => {
                *screen = Screen::TextPrompt {
                    title: "Master key version (optional, u64)".into(),
                    value: String::new(),
                    then: TextThen::KeysImportSeedVersion { seed: v },
                };
            }
            TextThen::KeysImportSeedVersion { seed } => {
                let version = if v.is_empty() {
                    None
                } else {
                    match v.parse::<u64>() {
                        Ok(n) => Some(n),
                        Err(e) => {
                            warn!("Invalid version: {e}");
                            *screen = Screen::TextPrompt {
                                title: "Master key version (optional, u64)".into(),
                                value: v,
                                then: TextThen::KeysImportSeedVersion { seed },
                            };
                            return Ok(ReplControl::Continue);
                        }
                    }
                };
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::ImportKeys {
                        file: None,
                        key: None,
                        seedphrase: Some(seed),
                        version,
                    },
                    "ImportKeys",
                );
            }
            TextThen::KeysSetActive => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::SetActiveMasterAddress { address_b58: v },
                    "SetActiveMasterAddress",
                );
            }
            TextThen::KeysImportMaster => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::ImportMasterPubkey { key_path: v },
                    "ImportMasterPubkey",
                );
            }
            TextThen::NotesListByAddr => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::ListNotesByAddress {
                        address: if v.is_empty() { None } else { Some(v) },
                    },
                    "ListNotesByAddress",
                );
            }
            TextThen::NotesListCsv => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::ListNotesByAddressCsv { address: v },
                    "ListNotesByAddressCsv",
                );
            }
            TextThen::TxSendPath => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::SendTx { transaction: v },
                    "SendTx",
                );
            }
            TextThen::TxShowPath => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::ShowTx { transaction: v },
                    "ShowTx",
                );
            }
            TextThen::TxSignMultisigTxFile => {
                *screen = Screen::TextPrompt {
                    title: "Sign keys (optional: index:hardened, comma-separated)".into(),
                    value: String::new(),
                    then: TextThen::TxSignMultisigKeys { transaction: v },
                };
            }
            TextThen::TxSignMultisigKeys { transaction } => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::SignMultisigTx {
                        transaction,
                        sign_keys: if v.is_empty() { None } else { Some(v) },
                    },
                    "SignMultisigTx",
                );
            }
            TextThen::TxMultisigThreshold => match v.parse::<u64>() {
                Ok(threshold) => {
                    *screen = Screen::TextPrompt {
                        title: "Participants (comma-separated pubkey hashes)".into(),
                        value: String::new(),
                        then: TextThen::TxMultisigParticipants { threshold },
                    };
                }
                Err(e) => warn!("Invalid threshold: {e}"),
            },
            TextThen::TxMultisigParticipants { threshold } => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::Watch {
                        subcommand: WatchSubcommand::Multisig {
                            threshold,
                            participants: v,
                        },
                    },
                    "Watch",
                );
            }
            TextThen::TxMigrateDest => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::MigrateV0Notes { destination: v },
                    "MigrateV0Notes",
                );
            }
            TextThen::WatchAddr => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::Watch {
                        subcommand: WatchSubcommand::Address { address: v },
                    },
                    "Watch",
                );
            }
            TextThen::WatchPubkey => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::Watch {
                        subcommand: WatchSubcommand::Pubkey { pubkey: v },
                    },
                    "Watch",
                );
            }
            TextThen::SignMsgStepMessage => {
                *screen = Screen::TextPrompt {
                    title: "Key index (optional, u64; empty = master)".into(),
                    value: String::new(),
                    then: TextThen::SignMsgStepIndex { message: v },
                };
            }
            TextThen::SignMsgStepIndex { message } => {
                let index = if v.is_empty() {
                    None
                } else {
                    match v.parse::<u64>() {
                        Ok(i) => Some(i),
                        Err(e) => {
                            warn!("Invalid index: {e}");
                            *screen = Screen::TextPrompt {
                                title: "Key index (optional, u64; empty = master)".into(),
                                value: v,
                                then: TextThen::SignMsgStepIndex { message },
                            };
                            return Ok(ReplControl::Continue);
                        }
                    }
                };
                *screen = Screen::Confirm {
                    title: "Hardened?".into(),
                    sel: 1,
                    labels: BOOL,
                    then: ConfirmThen::SignMsgHardened {
                        message: Some(message),
                        message_file: None,
                        message_pos: None,
                        index,
                    },
                };
            }
            TextThen::VerifyMsgM => {
                *screen = Screen::TextPrompt {
                    title: "Path to signature file".into(),
                    value: String::new(),
                    then: TextThen::VerifyMsgS { message: v },
                };
            }
            TextThen::VerifyMsgS { message } => {
                *screen = Screen::TextPrompt {
                    title: "Public key (base58)".into(),
                    value: String::new(),
                    then: TextThen::VerifyMsgP {
                        message,
                        sig_path: v,
                    },
                };
            }
            TextThen::VerifyMsgP { message, sig_path } => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::VerifyMessage {
                        message: Some(message),
                        message_file: None,
                        message_pos: None,
                        signature_path: Some(sig_path),
                        signature_pos: None,
                        pubkey: None,
                        pubkey_pos: Some(v),
                    },
                    "VerifyMessage",
                );
            }
            TextThen::SignHashGetHash => {
                *screen = Screen::TextPrompt {
                    title: "Key index (optional, u64)".into(),
                    value: String::new(),
                    then: TextThen::SignHashIndex { hash_b58: v },
                };
            }
            TextThen::SignHashIndex { hash_b58 } => {
                let index = if v.is_empty() {
                    None
                } else {
                    match v.parse::<u64>() {
                        Ok(i) => Some(i),
                        Err(e) => {
                            warn!("Invalid index: {e}");
                            *screen = Screen::TextPrompt {
                                title: "Key index (optional, u64)".into(),
                                value: v,
                                then: TextThen::SignHashIndex { hash_b58 },
                            };
                            return Ok(ReplControl::Continue);
                        }
                    }
                };
                *screen = Screen::Confirm {
                    title: "Hardened?".into(),
                    sel: 1,
                    labels: BOOL,
                    then: ConfirmThen::SignHashHardened { hash_b58, index },
                };
            }
            TextThen::VerifyHashFirst => {
                *screen = Screen::TextPrompt {
                    title: "Path to signature file".into(),
                    value: String::new(),
                    then: TextThen::VerifyHashSig { hash_b58: v },
                };
            }
            TextThen::VerifyHashSig { hash_b58 } => {
                *screen = Screen::TextPrompt {
                    title: "Public key (base58)".into(),
                    value: String::new(),
                    then: TextThen::VerifyHashPk {
                        hash_b58,
                        sig_path: v,
                    },
                };
            }
            TextThen::VerifyHashPk { hash_b58, sig_path } => {
                schedule_cmd(
                    app,
                    rt,
                    done_tx,
                    Commands::VerifyHash {
                        hash_b58,
                        signature_path: Some(sig_path),
                        signature_pos: None,
                        pubkey: None,
                        pubkey_pos: Some(v),
                    },
                    "VerifyHash",
                );
            }
        }
    } else {
        edit_line(&mut value, key);
        *screen = Screen::TextPrompt { title, value, then };
    }
    Ok(ReplControl::Continue)
}

async fn confirm_prompt(
    _cli: &WalletCli,
    app: &mut AppState,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    let state = std::mem::replace(screen, Screen::Main { sel: 0 });
    let (title, mut sel, labels, then) = match state {
        Screen::Confirm {
            title,
            sel,
            labels,
            then,
        } => (title, sel, labels, then),
        other => {
            *screen = other;
            return Ok(ReplControl::Continue);
        }
    };
    if esc_back(key.code) {
        *screen = Screen::Main { sel: 0 };
        return Ok(ReplControl::Continue);
    }
    match list_activate(&mut sel, labels.len(), key.code) {
        Err(()) => {
            *screen = Screen::Confirm {
                title,
                sel,
                labels,
                then,
            };
            Ok(ReplControl::Continue)
        }
        Ok(None) => {
            *screen = Screen::Confirm {
                title,
                sel,
                labels,
                then,
            };
            Ok(ReplControl::Continue)
        }
        Ok(Some(i)) => {
            match then {
                ConfirmThen::KeysDeriveAfterIndex { index } => {
                    let hardened = i == 0;
                    *screen = Screen::TextPrompt {
                        title: "Label (optional)".into(),
                        value: String::new(),
                        then: TextThen::KeysDeriveRun { index, hardened },
                    };
                }
                ConfirmThen::KeysKeyTree => {
                    let include_values = i == 0;
                    schedule_cmd(
                        app,
                        rt,
                        done_tx,
                        Commands::ShowKeyTree { include_values },
                        "ShowKeyTree",
                    );
                }
                ConfirmThen::SignMsgHardened {
                    message,
                    message_file,
                    message_pos,
                    index,
                } => {
                    let hardened = i == 0;
                    schedule_cmd(
                        app,
                        rt,
                        done_tx,
                        Commands::SignMessage {
                            message,
                            message_file,
                            message_pos,
                            index,
                            hardened,
                        },
                        "SignMessage",
                    );
                }
                ConfirmThen::SignHashHardened { hash_b58, index } => {
                    let hardened = i == 0;
                    schedule_cmd(
                        app,
                        rt,
                        done_tx,
                        Commands::SignHash {
                            hash_b58,
                            index,
                            hardened,
                        },
                        "SignHash",
                    );
                }
            }
            Ok(ReplControl::Continue)
        }
    }
}
