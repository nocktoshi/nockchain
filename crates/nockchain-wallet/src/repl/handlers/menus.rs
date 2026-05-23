//! Menu list routers (main menu, keys, notes, transactions, …).

use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};
use tracing::warn;

use super::input::{edit_line, esc_back, list_activate};
use crate::command::{Commands, WalletCli};
use crate::repl::command_runner::{JobCompletion, ReplRuntime};
use crate::repl::components::menus::{
    BOOL, IMPORT_SRC, KEYS_MENU, NOTES_MENU, SETTINGS_MENU, SIGN_MENU, TX_MENU,
    WATCH_MENU,
};
use crate::repl::create_tx::CreateTxWizard;
use crate::repl::hooks::logging::{log_help, log_verbose_info};
use crate::repl::hooks::terminal::Term;
use crate::repl::screens::{ConfirmThen, ReplControl, Screen, TextThen};
use crate::repl::session::current_api_listen;
use crate::repl::session_client::api_base_url;
use crate::repl::store::UIStore;
use crate::repl::{normalize_slash_cmd, session};

/// Navigate from the home Menu tab (`MAIN_MENU` index).
pub(super) fn navigate_main_menu_item(store: &mut UIStore, i: usize) {
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
        7 => crate::repl::prompt_overlay::exit_confirm(Screen::Home, 1),
        _ => Screen::Home,
    };
    super::replace_screen(store, next);
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
                super::replace_screen(store, Screen::Home);
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
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Child index (u64)",
                                    String::new(),
                                    TextThen::KeysDeriveIndex,
                                ),
                            );
                        }
                        2 => super::replace_screen(store, Screen::KeysImport { sel: 0 }),
                        3 => {
                            super::replace_screen(store, Screen::Keys { sel });
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ExportKeys,
                                "ExportKeys",
                            );
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
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::confirm_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Include values at each path?",
                                    1,
                                    BOOL,
                                    ConfirmThen::KeysKeyTree,
                                ),
                            );
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
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Address (base58)",
                                    String::new(),
                                    TextThen::KeysSetActive,
                                ),
                            );
                        }
                        11 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Path to exported master pubkey file",
                                    String::new(),
                                    TextThen::KeysImportMaster,
                                ),
                            );
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
                        13 => super::replace_screen(store, Screen::Home),
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
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Path to jammed keys file",
                                    String::new(),
                                    TextThen::KeysImportFile,
                                ),
                            );
                        }
                        1 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Extended key (zprv/zpub…)",
                                    String::new(),
                                    TextThen::KeysImportExtended,
                                ),
                            );
                        }
                        2 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Seed phrase",
                                    String::new(),
                                    TextThen::KeysImportSeed,
                                ),
                            );
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
                super::replace_screen(store, Screen::Home);
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
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ListNotes,
                                "ListNotes",
                            );
                        }
                        1 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Public key / filter",
                                    String::new(),
                                    TextThen::NotesListByAddr,
                                ),
                            );
                        }
                        2 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Public key",
                                    String::new(),
                                    TextThen::NotesListCsv,
                                ),
                            );
                        }
                        3 => {
                            super::schedule_cmd(
                                store,
                                rt,
                                done_tx,
                                Commands::ShowBalance,
                                "ShowBalance",
                            );
                        }
                        4 => super::replace_screen(store, Screen::Home),
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
                super::replace_screen(store, Screen::Home);
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
                            super::replace_screen(
                                store,
                                Screen::CreateTx {
                                    w: CreateTxWizard::new(),
                                },
                            );
                        }
                        1 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Transaction file path",
                                    String::new(),
                                    TextThen::TxSendPath,
                                ),
                            );
                        }
                        2 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Transaction file path",
                                    String::new(),
                                    TextThen::TxShowPath,
                                ),
                            );
                        }
                        3 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Transaction file path",
                                    String::new(),
                                    TextThen::TxSignMultisigTxFile,
                                ),
                            );
                        }
                        4 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Destination v1 address (base58)",
                                    String::new(),
                                    TextThen::TxMigrateDest,
                                ),
                            );
                        }
                        5 => {
                            super::replace_screen(store, Screen::nns_buy_new());
                        }
                        6 => super::replace_screen(store, Screen::Home),
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
                super::replace_screen(store, Screen::Home);
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
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Address or pubkey (base58)",
                                    String::new(),
                                    TextThen::WatchAddr,
                                ),
                            );
                        }
                        1 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Pubkey (base58)",
                                    String::new(),
                                    TextThen::WatchPubkey,
                                ),
                            );
                        }
                        2 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Threshold (m)",
                                    String::new(),
                                    TextThen::TxMultisigThreshold,
                                ),
                            );
                        }
                        3 => super::replace_screen(store, Screen::Home),
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
                super::replace_screen(store, Screen::Home);
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
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Message to sign",
                                    String::new(),
                                    TextThen::SignMsgStepMessage,
                                ),
                            );
                        }
                        1 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Message (plain text)",
                                    String::new(),
                                    TextThen::VerifyMsgM,
                                ),
                            );
                        }
                        2 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Hash (base58)",
                                    String::new(),
                                    TextThen::SignHashGetHash,
                                ),
                            );
                        }
                        3 => {
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Hash (base58)",
                                    String::new(),
                                    TextThen::VerifyHashFirst,
                                ),
                            );
                        }
                        4 => super::replace_screen(store, Screen::Home),
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
    rt: &ReplRuntime,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    super::replace_screen(store, Screen::Settings { sel: 0 });
    match taken {
        Screen::Settings { mut sel } => {
            if esc_back(key.code) {
                super::replace_screen(store, Screen::Home);
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
                            let session = session::session_config_snapshot(rt);
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "Public gRPC server (host[:port] or URI)",
                                    session.public_grpc_server_addr,
                                    TextThen::SettingsGrpcEndpoint,
                                ),
                            );
                        }
                        1 => {
                            let session = session::session_config_snapshot(rt);
                            super::replace_screen(
                                store,
                                crate::repl::prompt_overlay::text_prompt_screen(
                                    crate::repl::prompt_overlay::prompt_underlay(&taken),
                                    "JSON API listen (host:port)",
                                    session.api_listen,
                                    TextThen::SettingsApiListen,
                                ),
                            );
                        }
                        2 => {
                            const API_CURL_TEMPLATE: &str = include_str!("../docs/api-curl.txt");
                            let base = api_base_url(&current_api_listen(rt));
                            let token = rt.api_auth_token.as_ref();
                            let auth = format!("Authorization: Bearer {token}");

                            store.state.last_command_output = API_CURL_TEMPLATE
                                .replace("{listen}", &current_api_listen(rt))
                                .replace("{token}", &rt.api_auth_token.as_ref())
                                .replace("{base}", &base)
                                .replace("{auth}", &auth);
                            store.state.output_scroll = 0;
                            super::replace_screen(store, Screen::Settings { sel });
                        }
                        3 => {
                            log_help(cli.verbose);
                        }
                        4 => {
                            log_verbose_info();
                        }
                        5 => super::replace_screen(store, Screen::Home),
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
                    super::replace_screen(store, Screen::Home);
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
                            super::replace_screen(store, Screen::Home);
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
    match taken {
        Screen::ExitConfirm {
            underlay,
            mut sel,
        } => match list_activate(&mut sel, BOOL.len(), key.code) {
            Err(()) => {
                super::replace_screen(
                    store,
                    crate::repl::prompt_overlay::exit_confirm((*underlay).clone(), sel),
                );
                Ok(ReplControl::Continue)
            }
            Ok(None) => {
                if esc_back(key.code) {
                    super::replace_screen(store, *underlay);
                } else {
                    super::replace_screen(
                        store,
                        crate::repl::prompt_overlay::exit_confirm((*underlay).clone(), sel),
                    );
                }
                Ok(ReplControl::Continue)
            }
            Ok(Some(i)) => {
                if i == 0 {
                    return Ok(ReplControl::Quit);
                }
                super::replace_screen(store, *underlay);
                Ok(ReplControl::Continue)
            }
        },
        other => {
            super::replace_screen(store, other);
            Ok(ReplControl::Continue)
        }
    }
}
