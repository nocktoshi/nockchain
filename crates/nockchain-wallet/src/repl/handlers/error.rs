//! Error screen actions (retry, navigation, create-tx recovery).

use std::sync::Arc;

use crossterm::event::KeyEvent;
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};

use crate::command::{Commands, WalletCli};
use crate::repl::store::UIStore;
use crate::repl::command_runner::{JobCompletion, ReplRuntime};
use crate::repl::create_tx::CreateTxWizard;
use crate::repl::screens::{ErrorCtx, ReplControl, Screen};
use crate::repl::hooks::terminal::Term;

use super::input::{esc_back, list_activate};

pub(super) async fn error_screen(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let state = store.state.screen.clone();
    super::replace_screen(store, Screen::Main { sel: 0 });
    let (msg, mut sel, actions, ctx) = match state {
        Screen::ErrorScreen {
            msg,
            sel,
            actions,
            ctx,
        } => (msg, sel, actions, ctx),
        other => {
            super::replace_screen(store, other);
            return Ok(ReplControl::Continue);
        }
    };

    if esc_back(key.code) {
        super::replace_screen(store, Screen::Main { sel: 0 });
        return Ok(ReplControl::Continue);
    }

    match list_activate(&mut sel, actions.len(), key.code) {
        Err(()) => {
            super::replace_screen(store, Screen::ErrorScreen {
                msg,
                sel,
                actions,
                ctx,
            });
            Ok(ReplControl::Continue)
        }
        Ok(None) => {
            super::replace_screen(store, Screen::ErrorScreen {
                msg,
                sel,
                actions,
                ctx,
            });
            Ok(ReplControl::Continue)
        }
        Ok(Some(i)) => {
            match &ctx {
                ErrorCtx::Retry(cmd) => match i {
                    0 => {
                        super::schedule_cmd(store, rt, done_tx, cmd.clone(), "Retry");
                    }
                    1 => {
                        super::replace_screen(store, Screen::Main { sel: 0 });
                    }
                    _ => {
                        super::replace_screen(store, Screen::ErrorScreen {
                            msg,
                            sel,
                            actions,
                            ctx,
                        });
                    }
                },
                ErrorCtx::CreateTx { cmd } => {
                    if !matches!(cmd, Commands::CreateTx { .. }) {
                        super::replace_screen(store, Screen::Main { sel: 0 });
                    } else {
                        match i {
                            0 => {
                                super::schedule_cmd(store, rt, done_tx, cmd.clone(), "Retry");
                            }
                            1 => {
                                if let Some(w) = CreateTxWizard::from_command(&cmd) {
                                    super::replace_screen(store, Screen::CreateTx { w });
                                } else {
                                    super::replace_screen(store, Screen::Transactions { sel: 0 });
                                }
                            }
                            2 => {
                                super::replace_screen(store, Screen::CreateTx {
                                    w: CreateTxWizard::new(),
                                });
                            }
                            3 => {
                                super::replace_screen(store, Screen::Transactions { sel: 0 });
                            }
                            _ => {
                                super::replace_screen(store, Screen::ErrorScreen {
                                    msg,
                                    sel,
                                    actions,
                                    ctx,
                                });
                            }
                        }
                    }
                }
            }
            Ok(ReplControl::Continue)
        }
    }
}
