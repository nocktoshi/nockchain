//! Create-tx wizard keyboard handling.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use nockapp::NockAppError;
use nockchain_types::common::Hash;
use tokio::sync::mpsc;

use super::app_state::AppState;
use super::command_runner::{schedule_wallet_command, JobCompletion, ReplRuntime};
use super::create_tx::{CreateTxWizard, OptSub, Phase, RecSub};
use super::screens::{ReplControl, Screen};
use crate::command::{NoteSelectionStrategyCli, WalletCli};
use crate::recipient::{validate_blob_field, validate_memo_utf8, RecipientSpecToken};

pub(super) async fn handle_create_tx(
    _cli: &WalletCli,
    app: &mut AppState,
    key: KeyEvent,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
) -> Result<ReplControl, NockAppError> {
    let screen = &mut app.screen;
    let Screen::CreateTx { w } = screen else {
        return Ok(ReplControl::Continue);
    };
    if key.kind == KeyEventKind::Release {
        return Ok(ReplControl::Continue);
    }
    if matches!(
        key.code,
        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q')
    ) {
        *screen = Screen::Transactions { sel: 0 };
        return Ok(ReplControl::Continue);
    }

    match &mut w.phase {
        Phase::Recipients { list, sub } => match sub {
            RecSub::AddAnother { sel } => match key.code {
                KeyCode::Up | KeyCode::Char('k') => *sel = sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => *sel = (*sel + 1).min(1),
                KeyCode::Enter => {
                    let yes = *sel == 0;
                    if yes {
                        *sub = RecSub::Address {
                            line: String::new(),
                        };
                    } else {
                        let list = std::mem::take(list);
                        w.phase = Phase::Options {
                            recipients: list,
                            names: None,
                            fee: None,
                            allow_low_fee: false,
                            refund_pkh: None,
                            index: None,
                            hardened: false,
                            include_data: true,
                            sign_keys: Vec::new(),
                            save_raw_tx: false,
                            note_selection_strategy: NoteSelectionStrategyCli::Ascending,
                            sub: OptSub::Names {
                                line: String::new(),
                            },
                        };
                    }
                }
                _ => {}
            },
            RecSub::Address { line } => {
                edit_line(line, key);
                if key.code == KeyCode::Enter {
                    w.status = None;
                    let t = line.trim();
                    if t.is_empty() {
                        if list.is_empty() {
                            w.status = Some("Add an address or press q.".into());
                        } else {
                            *sub = RecSub::AddAnother { sel: 1 };
                        }
                    } else {
                        let addr = t.to_string();
                        *sub = RecSub::Amount {
                            addr,
                            line: String::new(),
                        };
                    }
                }
            }
            RecSub::Amount { addr, line } => {
                edit_line(line, key);
                if key.code == KeyCode::Enter {
                    w.status = None;
                    match line.trim().parse::<u64>() {
                        Ok(0) => w.status = Some("Amount must be > 0".into()),
                        Ok(a) => {
                            *sub = RecSub::Memo {
                                addr: addr.clone(),
                                amount: a,
                                line: String::new(),
                            };
                        }
                        Err(e) => w.status = Some(format!("Invalid amount: {e}")),
                    }
                }
            }
            RecSub::Memo { addr, amount, line } => {
                edit_line(line, key);
                if key.code == KeyCode::Enter {
                    w.status = None;
                    let memo = if line.trim().is_empty() {
                        None
                    } else {
                        Some(line.trim().to_string())
                    };
                    if let Err(e) = validate_memo_utf8(memo.as_deref()) {
                        w.status = Some(e.to_string());
                    } else {
                        *sub = RecSub::Blob {
                            addr: addr.clone(),
                            amount: *amount,
                            memo,
                            line: String::new(),
                        };
                    }
                }
            }
            RecSub::Blob {
                addr,
                amount,
                memo,
                line,
            } => {
                edit_line(line, key);
                if key.code == KeyCode::Enter {
                    w.status = None;
                    let blob = if line.trim().is_empty() {
                        None
                    } else {
                        Some(line.trim().to_string())
                    };
                    if Hash::from_base58(addr.trim()).is_err() {
                        w.status = Some("Invalid base58 address.".into());
                    } else if let Err(e) = validate_blob_field(blob.clone()) {
                        w.status = Some(e.to_string());
                    } else {
                        list.push(RecipientSpecToken::P2pkh {
                            address: addr.clone(),
                            amount: *amount,
                            memo: memo.clone(),
                            blob,
                        });
                        *sub = RecSub::AddAnother { sel: 1 };
                    }
                }
            }
        },
        Phase::Options {
            recipients: _,
            names,
            fee,
            allow_low_fee,
            refund_pkh,
            index,
            hardened,
            include_data,
            sign_keys,
            save_raw_tx,
            note_selection_strategy,
            sub,
        } => match sub {
            OptSub::Names { line }
            | OptSub::Fee { line }
            | OptSub::Refund { line }
            | OptSub::Index { line }
            | OptSub::SignKeys { line } => {
                edit_line(line, key);
                if key.code == KeyCode::Enter {
                    advance_options_line(
                        &mut w.status, names, fee, *allow_low_fee, refund_pkh, index, *hardened,
                        *include_data, sign_keys, *save_raw_tx, *note_selection_strategy, sub,
                    );
                }
            }
            OptSub::AllowLowFee { sel }
            | OptSub::Hardened { sel }
            | OptSub::IncludeData { sel }
            | OptSub::SaveRaw { sel } => match key.code {
                KeyCode::Up | KeyCode::Char('k') => *sel = sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => *sel = (*sel + 1).min(1),
                KeyCode::Enter => {
                    advance_options_toggle(
                        &mut w.status, allow_low_fee, refund_pkh, hardened, include_data,
                        save_raw_tx, note_selection_strategy, sign_keys, sub,
                    );
                }
                _ => {}
            },
            OptSub::NoteSelection { sel } => match key.code {
                KeyCode::Up | KeyCode::Char('k') => *sel = sel.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => *sel = (*sel + 1).min(1),
                KeyCode::Enter => {
                    *note_selection_strategy = if *sel == 0 {
                        NoteSelectionStrategyCli::Ascending
                    } else {
                        NoteSelectionStrategyCli::Descending
                    };
                    if let Some(cmd) = w.build_command() {
                        schedule_wallet_command(
                            app,
                            rt,
                            done_tx.clone(),
                            cmd,
                            "Create transaction",
                        );
                    }
                }
                _ => {}
            },
        },
    }
    Ok(ReplControl::Continue)
}

pub(super) fn apply_paste_to_wizard(w: &mut CreateTxWizard, pasted: &str) {
    match &mut w.phase {
        Phase::Recipients { sub, .. } => match sub {
            RecSub::Address { line } => super::paste::paste_single_line(line, pasted),
            RecSub::Amount { line, .. } => super::paste::paste_single_line(line, pasted),
            RecSub::Memo { line, .. } => super::paste::paste_multiline(line, pasted),
            RecSub::Blob { line, .. } => super::paste::paste_single_line(line, pasted),
            RecSub::AddAnother { .. } => {}
        },
        Phase::Options { sub, .. } => match sub {
            OptSub::Names { line }
            | OptSub::Fee { line }
            | OptSub::Refund { line }
            | OptSub::Index { line }
            | OptSub::SignKeys { line } => super::paste::paste_single_line(line, pasted),
            OptSub::AllowLowFee { .. }
            | OptSub::Hardened { .. }
            | OptSub::IncludeData { .. }
            | OptSub::SaveRaw { .. }
            | OptSub::NoteSelection { .. } => {}
        },
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

#[allow(clippy::too_many_arguments)]
fn advance_options_line(
    status: &mut Option<String>,
    names: &mut Option<String>,
    fee: &mut Option<u64>,
    allow_low_fee: bool,
    refund_pkh: &mut Option<String>,
    index: &mut Option<u64>,
    hardened: bool,
    _include_data: bool,
    sign_keys: &mut Vec<String>,
    save_raw_tx: bool,
    _note_selection_strategy: NoteSelectionStrategyCli,
    sub: &mut OptSub,
) {
    *status = None;
    match sub {
        OptSub::Names { line } => {
            *names = if line.trim().is_empty() {
                None
            } else {
                Some(line.trim().to_string())
            };
            *sub = OptSub::Fee {
                line: fee.map(|f| f.to_string()).unwrap_or_default(),
            };
        }
        OptSub::Fee { line } => {
            if line.trim().is_empty() {
                *fee = None;
            } else {
                match line.trim().parse::<u64>() {
                    Ok(f) => *fee = Some(f),
                    Err(e) => {
                        *status = Some(format!("Invalid fee: {e}"));
                        return;
                    }
                }
            }
            *sub = OptSub::AllowLowFee {
                sel: if allow_low_fee { 0 } else { 1 },
            };
        }
        OptSub::Refund { line } => {
            *refund_pkh = if line.trim().is_empty() {
                None
            } else {
                Some(line.trim().to_string())
            };
            *sub = OptSub::Index {
                line: index.map(|i| i.to_string()).unwrap_or_default(),
            };
        }
        OptSub::Index { line } => {
            *index = if line.trim().is_empty() {
                None
            } else {
                match line.trim().parse::<u64>() {
                    Ok(i) => Some(i),
                    Err(e) => {
                        *status = Some(format!("Invalid index: {e}"));
                        return;
                    }
                }
            };
            *sub = OptSub::Hardened {
                sel: if hardened { 0 } else { 1 },
            };
        }
        OptSub::SignKeys { line } => {
            *sign_keys = if line.trim().is_empty() {
                Vec::new()
            } else {
                line.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            };
            *sub = OptSub::SaveRaw {
                sel: if save_raw_tx { 0 } else { 1 },
            };
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn advance_options_toggle(
    status: &mut Option<String>,
    allow_low_fee: &mut bool,
    refund_pkh: &Option<String>,
    hardened: &mut bool,
    include_data: &mut bool,
    save_raw_tx: &mut bool,
    _note_selection_strategy: &mut NoteSelectionStrategyCli,
    sign_keys: &[String],
    sub: &mut OptSub,
) {
    *status = None;
    match sub {
        OptSub::AllowLowFee { sel } => {
            *allow_low_fee = *sel == 0;
            *sub = OptSub::Refund {
                line: refund_pkh.clone().unwrap_or_default(),
            };
        }
        OptSub::Hardened { sel } => {
            *hardened = *sel == 0;
            *sub = OptSub::IncludeData {
                sel: if *include_data { 0 } else { 1 },
            };
        }
        OptSub::IncludeData { sel } => {
            *include_data = *sel == 0;
            *sub = OptSub::SignKeys {
                line: sign_keys.join(","),
            };
        }
        OptSub::SaveRaw { sel } => {
            *save_raw_tx = *sel == 0;
            *sub = OptSub::NoteSelection { sel: 0 };
        }
        _ => {}
    }
}
