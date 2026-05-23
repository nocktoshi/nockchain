//! Error screen actions (retry, navigation, create-tx recovery).

use std::sync::Arc;

use crossterm::event::KeyEvent;
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};

use super::input::{esc_back, list_activate};
use crate::command::{Commands, WalletCli};
use crate::tui::command_runner::{JobCompletion, TuiRuntime};
use crate::tui::create_tx::CreateTxWizard;
use crate::tui::hooks::terminal::Term;
use crate::tui::screens::{ErrorCtx, TuiControl, Screen};
use crate::tui::store::UIStore;

pub(super) async fn error_screen(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &TuiRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<TuiControl, NockAppError> {
    let state = store.state.screen.clone();
    super::replace_screen(store, Screen::Home);
    let (msg, mut sel, actions, ctx) = match state {
        Screen::ErrorScreen {
            msg,
            sel,
            actions,
            ctx,
        } => (msg, sel, actions, ctx),
        other => {
            super::replace_screen(store, other);
            return Ok(TuiControl::Continue);
        }
    };

    if esc_back(key.code) {
        super::replace_screen(store, Screen::Home);
        return Ok(TuiControl::Continue);
    }

    match list_activate(&mut sel, actions.len(), key.code) {
        Err(()) => {
            super::replace_screen(
                store,
                Screen::ErrorScreen {
                    msg,
                    sel,
                    actions,
                    ctx,
                },
            );
            Ok(TuiControl::Continue)
        }
        Ok(None) => {
            super::replace_screen(
                store,
                Screen::ErrorScreen {
                    msg,
                    sel,
                    actions,
                    ctx,
                },
            );
            Ok(TuiControl::Continue)
        }
        Ok(Some(i)) => {
            match &ctx {
                ErrorCtx::Retry(cmd) => match i {
                    0 => {
                        super::schedule_cmd(store, rt, done_tx, cmd.clone(), "Retry");
                    }
                    1 => {
                        super::replace_screen(store, Screen::Home);
                    }
                    _ => {
                        super::replace_screen(
                            store,
                            Screen::ErrorScreen {
                                msg,
                                sel,
                                actions,
                                ctx,
                            },
                        );
                    }
                },
                ErrorCtx::CreateTx { cmd } => {
                    if !matches!(cmd, Commands::CreateTx { .. }) {
                        super::replace_screen(store, Screen::Home);
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
                                super::replace_screen(
                                    store,
                                    Screen::CreateTx {
                                        w: CreateTxWizard::new(),
                                    },
                                );
                            }
                            3 => {
                                super::replace_screen(store, Screen::Transactions { sel: 0 });
                            }
                            _ => {
                                super::replace_screen(
                                    store,
                                    Screen::ErrorScreen {
                                        msg,
                                        sel,
                                        actions,
                                        ctx,
                                    },
                                );
                            }
                        }
                    }
                }
            }
            Ok(TuiControl::Continue)
        }
    }
}
