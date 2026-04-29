//! Error screen actions (retry, navigation, create-tx recovery).

use std::sync::Arc;

use crossterm::event::KeyEvent;
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};

use crate::command::{Commands, WalletCli};
use crate::repl::app_state::AppState;
use crate::repl::command_runner::{JobCompletion, ReplRuntime};
use crate::repl::create_tx::CreateTxWizard;
use crate::repl::screens::{ErrorCtx, ReplControl, Screen};
use crate::repl::hooks::terminal::Term;

use super::input::{esc_back, list_activate};

pub(super) async fn error_screen(
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
                        super::schedule_cmd(app, rt, done_tx, cmd.clone(), "Retry");
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
                                super::schedule_cmd(app, rt, done_tx, cmd.clone(), "Retry");
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
