//! Multi-line text prompts and yes/no confirm flows.

use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;
use tokio::sync::{mpsc, Mutex};
use tracing::warn;

use super::input::{edit_line, esc_back, list_activate};
use crate::command::{Commands, WalletCli, WatchSubcommand};
use crate::tui::command_runner::{JobCompletion, TuiRuntime};
use crate::tui::components::menus::BOOL;
use crate::tui::hooks::terminal::Term;
use crate::tui::prompt_overlay::{
    confirm_prompt_screen as overlay_confirm, text_prompt_screen as overlay_text,
};
use crate::tui::screens::{ConfirmThen, TuiControl, Screen, TextThen};
use crate::tui::store::UIStore;
use crate::tui::{session, wallet_api};

pub(super) async fn text_prompt(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &TuiRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<TuiControl, NockAppError> {
    let state = store.state.screen.clone();
    let (underlay, title, mut value, then) = match state {
        Screen::TextPrompt {
            underlay,
            title,
            value,
            then,
        } => (underlay, title, value, then),
        _other => return Ok(TuiControl::Continue),
    };
    if esc_back(key.code) {
        super::replace_screen(store, *underlay);
        return Ok(TuiControl::Continue);
    }
    if key.code == KeyCode::Enter {
        let v = value.trim().to_string();
        match then {
            TextThen::KeysDeriveIndex => match v.parse::<u64>() {
                Ok(index) => {
                    super::replace_screen(
                        store,
                        overlay_confirm(
                            (*underlay).clone(),
                            "Hardened?",
                            1,
                            BOOL,
                            ConfirmThen::KeysDeriveAfterIndex { index },
                        ),
                    );
                }
                Err(e) => warn!("Invalid index: {e}"),
            },
            TextThen::KeysDeriveRun { index, hardened } => {
                let label = if v.is_empty() { None } else { Some(v) };
                super::schedule_cmd(
                    store,
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
                super::schedule_cmd(
                    store,
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
                super::schedule_cmd(
                    store,
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
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Master key version (optional, u64)",
                        String::new(),
                        TextThen::KeysImportSeedVersion { seed: v },
                    ),
                );
            }
            TextThen::KeysImportSeedVersion { seed } => {
                let version = if v.is_empty() {
                    None
                } else {
                    match v.parse::<u64>() {
                        Ok(n) => Some(n),
                        Err(e) => {
                            warn!("Invalid version: {e}");
                            super::replace_screen(
                                store,
                                overlay_text(
                                    (*underlay).clone(),
                                    "Master key version (optional, u64)",
                                    v,
                                    TextThen::KeysImportSeedVersion { seed },
                                ),
                            );
                            return Ok(TuiControl::Continue);
                        }
                    }
                };
                super::schedule_cmd(
                    store,
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
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::SetActiveMasterAddress { address_b58: v },
                    "SetActiveMasterAddress",
                );
            }
            TextThen::KeysImportMaster => {
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::ImportMasterPubkey { key_path: v },
                    "ImportMasterPubkey",
                );
            }
            TextThen::NotesListByAddr => {
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::ListNotesByAddress {
                        address: if v.is_empty() { None } else { Some(v) },
                    },
                    "ListNotesByAddress",
                );
            }
            TextThen::NotesListCsv => {
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::ListNotesByAddressCsv { address: v },
                    "ListNotesByAddressCsv",
                );
            }
            TextThen::TxSendPath => {
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::SendTx { transaction: v },
                    "SendTx",
                );
            }
            TextThen::TxShowPath => {
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::ShowTx { transaction: v },
                    "ShowTx",
                );
            }
            TextThen::TxSignMultisigTxFile => {
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Sign keys (optional: index:hardened, comma-separated)",
                        String::new(),
                        TextThen::TxSignMultisigKeys { transaction: v },
                    ),
                );
            }
            TextThen::TxSignMultisigKeys { transaction } => {
                super::schedule_cmd(
                    store,
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
                    super::replace_screen(
                        store,
                        overlay_text(
                            (*underlay).clone(),
                            "Participants (comma-separated pubkey hashes)",
                            String::new(),
                            TextThen::TxMultisigParticipants { threshold },
                        ),
                    );
                }
                Err(e) => warn!("Invalid threshold: {e}"),
            },
            TextThen::TxMultisigParticipants { threshold } => {
                super::schedule_cmd(
                    store,
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
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::MigrateV0Notes { destination: v },
                    "MigrateV0Notes",
                );
            }
            TextThen::SettingsGrpcEndpoint => match crate::connection::GrpcEndpoint::parse(&v) {
                Ok(endpoint) => {
                    let old_listen = session::current_api_listen(rt);
                    let mut next = session::session_config_snapshot(rt);
                    next.public_grpc_server_addr = endpoint.to_string();
                    match session::commit_session(rt, next).await {
                        Ok(_) => {
                            wallet_api::restart_api_server_if_listen_changed(rt, &old_listen);
                            store.session_display = session::session_config_snapshot(rt);
                            super::replace_screen(store, Screen::Settings { sel: 0 });
                        }
                        Err(e) => warn!("{e}"),
                    }
                }
                Err(e) => warn!("{e}"),
            },
            TextThen::SettingsApiListen => {
                let old_listen = session::current_api_listen(rt);
                let mut next = session::session_config_snapshot(rt);
                next.api_listen = v.trim().to_string();
                match session::commit_session(rt, next).await {
                    Ok(_) => {
                        wallet_api::restart_api_server_if_listen_changed(rt, &old_listen);
                        store.session_display = session::session_config_snapshot(rt);
                        super::replace_screen(store, Screen::Settings { sel: 0 });
                    }
                    Err(e) => warn!("{e}"),
                }
            }
            TextThen::WatchAddr => {
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::Watch {
                        subcommand: WatchSubcommand::Address { address: v },
                    },
                    "Watch",
                );
            }
            TextThen::WatchPubkey => {
                super::schedule_cmd(
                    store,
                    rt,
                    done_tx,
                    Commands::Watch {
                        subcommand: WatchSubcommand::Pubkey { pubkey: v },
                    },
                    "Watch",
                );
            }
            TextThen::SignMsgStepMessage => {
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Key index (optional, u64; empty = master)",
                        String::new(),
                        TextThen::SignMsgStepIndex { message: v },
                    ),
                );
            }
            TextThen::SignMsgStepIndex { message } => {
                let index = if v.is_empty() {
                    None
                } else {
                    match v.parse::<u64>() {
                        Ok(i) => Some(i),
                        Err(e) => {
                            warn!("Invalid index: {e}");
                            super::replace_screen(
                                store,
                                overlay_text(
                                    (*underlay).clone(),
                                    "Key index (optional, u64; empty = master)",
                                    v,
                                    TextThen::SignMsgStepIndex { message },
                                ),
                            );
                            return Ok(TuiControl::Continue);
                        }
                    }
                };
                super::replace_screen(
                    store,
                    overlay_confirm(
                        (*underlay).clone(),
                        "Hardened?",
                        1,
                        BOOL,
                        ConfirmThen::SignMsgHardened {
                            message: Some(message),
                            message_file: None,
                            message_pos: None,
                            index,
                        },
                    ),
                );
            }
            TextThen::VerifyMsgM => {
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Path to signature file",
                        String::new(),
                        TextThen::VerifyMsgS { message: v },
                    ),
                );
            }
            TextThen::VerifyMsgS { message } => {
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Public key (base58)",
                        String::new(),
                        TextThen::VerifyMsgP {
                            message,
                            sig_path: v,
                        },
                    ),
                );
            }
            TextThen::VerifyMsgP { message, sig_path } => {
                super::schedule_cmd(
                    store,
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
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Key index (optional, u64)",
                        String::new(),
                        TextThen::SignHashIndex { hash_b58: v },
                    ),
                );
            }
            TextThen::SignHashIndex { hash_b58 } => {
                let index = if v.is_empty() {
                    None
                } else {
                    match v.parse::<u64>() {
                        Ok(i) => Some(i),
                        Err(e) => {
                            warn!("Invalid index: {e}");
                            super::replace_screen(
                                store,
                                overlay_text(
                                    (*underlay).clone(),
                                    "Key index (optional, u64)",
                                    v,
                                    TextThen::SignHashIndex { hash_b58 },
                                ),
                            );
                            return Ok(TuiControl::Continue);
                        }
                    }
                };
                super::replace_screen(
                    store,
                    overlay_confirm(
                        (*underlay).clone(),
                        "Hardened?",
                        1,
                        BOOL,
                        ConfirmThen::SignHashHardened { hash_b58, index },
                    ),
                );
            }
            TextThen::VerifyHashFirst => {
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Path to signature file",
                        String::new(),
                        TextThen::VerifyHashSig { hash_b58: v },
                    ),
                );
            }
            TextThen::VerifyHashSig { hash_b58 } => {
                super::replace_screen(
                    store,
                    overlay_text(
                        (*underlay).clone(),
                        "Public key (base58)",
                        String::new(),
                        TextThen::VerifyHashPk {
                            hash_b58,
                            sig_path: v,
                        },
                    ),
                );
            }
            TextThen::VerifyHashPk { hash_b58, sig_path } => {
                super::schedule_cmd(
                    store,
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
        super::replace_screen(
            store,
            Screen::TextPrompt {
                underlay,
                title,
                value,
                then,
            },
        );
    }
    Ok(TuiControl::Continue)
}

pub(super) async fn confirm_prompt(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &TuiRuntime,
    _terminal: &Arc<Mutex<Term>>,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<TuiControl, NockAppError> {
    let state = store.state.screen.clone();
    let (underlay, title, mut sel, labels, then) = match state {
        Screen::Confirm {
            underlay,
            title,
            sel,
            labels,
            then,
        } => (underlay, title, sel, labels, then),
        _ => return Ok(TuiControl::Continue),
    };
    if esc_back(key.code) {
        super::replace_screen(store, *underlay);
        return Ok(TuiControl::Continue);
    }
    match list_activate(&mut sel, labels.len(), key.code) {
        Err(()) | Ok(None) => {
            super::replace_screen(
                store,
                Screen::Confirm {
                    underlay,
                    title,
                    sel,
                    labels,
                    then,
                },
            );
            Ok(TuiControl::Continue)
        }
        Ok(Some(i)) => {
            match then {
                ConfirmThen::KeysDeriveAfterIndex { index } => {
                    let hardened = i == 0;
                    super::replace_screen(
                        store,
                        overlay_text(
                            (*underlay).clone(),
                            "Label (optional)",
                            String::new(),
                            TextThen::KeysDeriveRun { index, hardened },
                        ),
                    );
                }
                ConfirmThen::KeysKeyTree => {
                    let include_values = i == 0;
                    super::schedule_cmd(
                        store,
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
                    super::schedule_cmd(
                        store,
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
                    super::schedule_cmd(
                        store,
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
            Ok(TuiControl::Continue)
        }
    }
}
