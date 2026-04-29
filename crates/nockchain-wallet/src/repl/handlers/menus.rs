//! Menu list routers (main menu, keys, notes, transactions, …).

use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};
use tracing::warn;

use crate::command::{Commands, WalletCli};
use crate::repl::command_runner::{JobCompletion, ReplRuntime};
use crate::repl::create_tx::CreateTxWizard;
use crate::repl::screens::{ConfirmThen, ReplControl, Screen, TextThen};
use crate::repl::components::menus::{
    BOOL, IMPORT_SRC, KEYS_MENU, MAIN_MENU, NOTES_MENU, SETTINGS_MENU, SIGN_MENU, TX_MENU,
    WATCH_MENU,
};
use crate::repl::hooks::logging::{log_help, log_verbose_info};
use crate::repl::hooks::terminal::Term;
use crate::repl::normalize_slash_cmd;
use crate::repl::store::UIStore;

use super::input::{edit_line, esc_back, list_activate};

pub(super) async fn handle_main(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::Main { sel: 0 });
    match taken {
        Screen::Main { mut sel } => match list_activate(&mut sel, MAIN_MENU.len(), key.code) {
            Err(()) => {
                super::replace_screen(store, Screen::Main { sel });
                Ok(ReplControl::Continue)
            }
            Ok(None) => {
                if esc_back(key.code) {
                    super::replace_screen(store, Screen::Main { sel });
                    return Ok(ReplControl::Quit);
                }
                super::replace_screen(store, Screen::Main { sel });
                Ok(ReplControl::Continue)
            }
            Ok(Some(i)) => {
                let next = match i {
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
                super::replace_screen(store, next);
                Ok(ReplControl::Continue)
            }
        },
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) async fn handle_keys(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::Keys { sel: 0 });
    match taken {
        Screen::Keys { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Main { sel: 0 });
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, KEYS_MENU.len(), key.code) {
                Err(()) => {
                    super::replace_screen(store, Screen::Keys { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    super::replace_screen(store, Screen::Keys { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(store, rt, done_tx, Commands::Keygen, "Keygen");
                        }
                        1 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Child index (u64)".into(),
                                value: String::new(),
                                then: TextThen::KeysDeriveIndex,
                            });
                        }
                        2 => super::replace_screen(store, Screen::KeysImport { sel: 0 }),
                        3 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(store, rt, done_tx, Commands::ExportKeys, "ExportKeys");
                        }
                        4 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ShowSeedphrase,
                                "ShowSeedphrase",
                            );
                        }
                        5 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ShowMasterZPub,
                                "ShowMasterZPub",
                            );
                        }
                        6 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ShowMasterZPrv,
                                "ShowMasterZPrv",
                            );
                        }
                        7 => {
                            super::replace_screen(store, Screen::Confirm {
                                title: "Include values at each path?".into(),
                                sel: 1,
                                labels: BOOL,
                                then: ConfirmThen::KeysKeyTree,
                            });
                        }
                        8 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ListActiveAddresses,
                                "ListActiveAddresses",
                            );
                        }
                        9 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ListMasterAddresses,
                                "ListMasterAddresses",
                            );
                        }
                        10 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Address (base58)".into(),
                                value: String::new(),
                                then: TextThen::KeysSetActive,
                            });
                        }
                        11 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Path to exported master pubkey file".into(),
                                value: String::new(),
                                then: TextThen::KeysImportMaster,
                            });
                        }
                        12 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ExportMasterPubkey,
                                "ExportMasterPubkey",
                            );
                        }
                        13 => super::replace_screen(store, Screen::Main { sel: 0 }),
                        _ => {
                            super::replace_screen(store, Screen::Keys { sel });
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) async fn handle_keys_import(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::KeysImport { sel: 0 });
    match taken {
        Screen::KeysImport { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Keys { sel: 2 });
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, IMPORT_SRC.len(), key.code) {
                Err(()) => {
                    super::replace_screen(store, Screen::KeysImport { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    super::replace_screen(store, Screen::KeysImport { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Path to jammed keys file".into(),
                                value: String::new(),
                                then: TextThen::KeysImportFile,
                            });
                        }
                        1 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Extended key (zprv/zpub…)".into(),
                                value: String::new(),
                                then: TextThen::KeysImportExtended,
                            });
                        }
                        2 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Seed phrase".into(),
                                value: String::new(),
                                then: TextThen::KeysImportSeed,
                            });
                        }
                        3 => super::replace_screen(store, Screen::Keys { sel: 2 }),
                        _ => {
                            super::replace_screen(store, Screen::KeysImport { sel });
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) async fn handle_notes(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &ReplRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::Notes { sel: 0 });
    match taken {
        Screen::Notes { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Main { sel: 0 });
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, NOTES_MENU.len(), key.code) {
                Err(()) => {
                    super::replace_screen(store, Screen::Notes { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    super::replace_screen(store, Screen::Notes { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            super::schedule_cmd(store, rt, done_tx, Commands::ListNotes, "ListNotes");
                        }
                        1 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Public key / filter".into(),
                                value: String::new(),
                                then: TextThen::NotesListByAddr,
                            });
                        }
                        2 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Public key".into(),
                                value: String::new(),
                                then: TextThen::NotesListCsv,
                            });
                        }
                        3 => {
                            super::schedule_cmd(store, rt, done_tx, Commands::ShowBalance, "ShowBalance");
                        }
                        4 => super::replace_screen(store, Screen::Main { sel: 0 }),
                        _ => {
                            super::replace_screen(store, Screen::Notes { sel });
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) async fn handle_transactions(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::Transactions { sel: 0 });
    match taken {
        Screen::Transactions { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Main { sel: 0 });
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, TX_MENU.len(), key.code) {
                Err(()) => {
                    super::replace_screen(store, Screen::Transactions { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    super::replace_screen(store, Screen::Transactions { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            super::replace_screen(store, Screen::CreateTx {
                                w: CreateTxWizard::new(),
                            });
                        }
                        1 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Transaction file path".into(),
                                value: String::new(),
                                then: TextThen::TxSendPath,
                            });
                        }
                        2 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Transaction file path".into(),
                                value: String::new(),
                                then: TextThen::TxShowPath,
                            });
                        }
                        3 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Transaction file path".into(),
                                value: String::new(),
                                then: TextThen::TxSignMultisigTxFile,
                            });
                        }
                        4 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Destination v1 address (base58)".into(),
                                value: String::new(),
                                then: TextThen::TxMigrateDest,
                            });
                        }
                        5 => super::replace_screen(store, Screen::Main { sel: 0 }),
                        _ => {
                            super::replace_screen(store, Screen::Transactions { sel });
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) async fn handle_watch(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::Watch { sel: 0 });
    match taken {
        Screen::Watch { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Main { sel: 0 });
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, WATCH_MENU.len(), key.code) {
                Err(()) => {
                    super::replace_screen(store, Screen::Watch { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    super::replace_screen(store, Screen::Watch { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Address or pubkey (base58)".into(),
                                value: String::new(),
                                then: TextThen::WatchAddr,
                            });
                        }
                        1 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Pubkey (base58)".into(),
                                value: String::new(),
                                then: TextThen::WatchPubkey,
                            });
                        }
                        2 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Threshold (m)".into(),
                                value: String::new(),
                                then: TextThen::TxMultisigThreshold,
                            });
                        }
                        3 => super::replace_screen(store, Screen::Main { sel: 0 }),
                        _ => {
                            super::replace_screen(store, Screen::Watch { sel });
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) async fn handle_sign(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::SignVerify { sel: 0 });
    match taken {
        Screen::SignVerify { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Main { sel: 0 });
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, SIGN_MENU.len(), key.code) {
                Err(()) => {
                    super::replace_screen(store, Screen::SignVerify { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    super::replace_screen(store, Screen::SignVerify { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Message to sign".into(),
                                value: String::new(),
                                then: TextThen::SignMsgStepMessage,
                            });
                        }
                        1 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Message (plain text)".into(),
                                value: String::new(),
                                then: TextThen::VerifyMsgM,
                            });
                        }
                        2 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Hash (base58)".into(),
                                value: String::new(),
                                then: TextThen::SignHashGetHash,
                            });
                        }
                        3 => {
                            super::replace_screen(store, Screen::TextPrompt {
                                title: "Hash (base58)".into(),
                                value: String::new(),
                                then: TextThen::VerifyHashFirst,
                            });
                        }
                        4 => super::replace_screen(store, Screen::Main { sel: 0 }),
                        _ => {
                            super::replace_screen(store, Screen::SignVerify { sel });
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) fn handle_settings(
    cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::Settings { sel: 0 });
    match taken {
        Screen::Settings { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Main { sel: 0 });
                return Ok(ReplControl::Continue);
            }
            match list_activate(&mut sel, SETTINGS_MENU.len(), key.code) {
                Err(()) => {
                    super::replace_screen(store, Screen::Settings { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(None) => {
                    super::replace_screen(store, Screen::Settings { sel });
                    Ok(ReplControl::Continue)
                }
                Ok(Some(i)) => {
                    match i {
                        0 => {
                            log_help(cli.verbose);
                        }
                        1 => {
                            log_verbose_info();
                        }
                        2 => super::replace_screen(store, Screen::Main { sel: 0 }),
                        _ => {
                            super::replace_screen(store, Screen::Settings { sel });
                        }
                    }
                    Ok(ReplControl::Continue)
                }
            }
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) fn handle_quick(
    cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(
        store,
        Screen::Quick {
            line: String::new(),
        },
    );
    match taken {
        Screen::Quick { mut line } => {
            match key.code {
                KeyCode::Esc => {
                    super::replace_screen(store, Screen::Main { sel: 0 });
                }
                KeyCode::Enter => {
                    let cmd = normalize_slash_cmd(&line);
                    match cmd.to_ascii_lowercase().as_str() {
                        "exit" | "quit" => return Ok(ReplControl::Quit),
                        "help" => {
                            log_help(cli.verbose);
                        }
                        "verbose" => {
                            log_verbose_info();
                        }
                        "menu" => {
                            super::replace_screen(store, Screen::Main { sel: 0 });
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
                    super::replace_screen(store, Screen::Quick { line });
                }
                _ => {
                    edit_line(&mut line, key);
                    super::replace_screen(store, Screen::Quick { line });
                }
            }
            Ok(ReplControl::Continue)
        }
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}

pub(super) fn handle_exit_confirm(
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::ExitConfirm { sel: 0 });
    match taken {
        Screen::ExitConfirm { mut sel } => match list_activate(&mut sel, BOOL.len(), key.code) {
            Err(()) => {
                super::replace_screen(store, Screen::ExitConfirm { sel });
                Ok(ReplControl::Continue)
            }
            Ok(None) => {
                if esc_back(key.code) {
                    super::replace_screen(store, Screen::Main { sel: 0 });
                } else {
                    super::replace_screen(store, Screen::ExitConfirm { sel });
                }
                Ok(ReplControl::Continue)
            }
            Ok(Some(i)) => {
                if i == 0 {
                    return Ok(ReplControl::Quit);
                }
                super::replace_screen(store, Screen::Main { sel: 0 });
                Ok(ReplControl::Continue)
            }
        },
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}
