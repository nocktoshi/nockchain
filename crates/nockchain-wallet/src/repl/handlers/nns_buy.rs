//! NNS buy screen keyboard handling.

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;
use tokio::sync::mpsc;

use super::input::esc_back;
use super::replace_screen;
use crate::repl::command_runner::{
    schedule_nns_lookup, schedule_nns_register, JobCompletion, NnsLookupCompletion, ReplRuntime,
};
use crate::repl::nns;
use crate::repl::screens::{NnsBuyFocus, ReplControl, Screen};
use crate::repl::store::UIStore;

pub(crate) fn apply_nns_lookup_result(store: &mut UIStore, result: NnsLookupCompletion) {
    let Screen::NnsBuy {
        value,
        cursor,
        focus,
        ..
    } = store.state.screen.clone()
    else {
        return;
    };

    match result {
        Ok(ok) => {
            let status = nns::availability_message(&ok, store.state.price.usd_per_coin);
            store.dispatch(crate::repl::store::UiAction::ReplaceScreen(Screen::NnsBuy {
                value,
                cursor,
                focus,
                status: Some(status),
                lookup_busy: false,
                verified_name: Some(ok.canonical_name),
            }));
        }
        Err(msg) => {
            store.dispatch(crate::repl::store::UiAction::ReplaceScreen(Screen::NnsBuy {
                value,
                cursor,
                focus,
                status: Some(format!("Error: {msg}")),
                lookup_busy: false,
                verified_name: None,
            }));
        }
    }
}

pub(super) async fn handle_nns_buy(
    store: &mut UIStore,
    key: KeyEvent,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
    lookup_done_tx: &mpsc::UnboundedSender<NnsLookupCompletion>,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    let Screen::NnsBuy {
        mut value,
        mut cursor,
        mut focus,
        mut status,
        lookup_busy,
        verified_name,
    } = taken
    else {
        return Ok(ReplControl::Continue);
    };

    if lookup_busy {
        return Ok(ReplControl::Continue);
    }

    if esc_back(key.code) {
        replace_screen(store, Screen::Home);
        return Ok(ReplControl::Continue);
    }

    let mut clear_verify = false;

    match key.code {
        KeyCode::Tab => focus = next_focus(focus),
        KeyCode::BackTab => focus = prev_focus(focus),
        KeyCode::Left if matches!(focus, NnsBuyFocus::Cancel | NnsBuyFocus::Register) => {
            focus = NnsBuyFocus::Cancel;
        }
        KeyCode::Right if matches!(focus, NnsBuyFocus::Cancel | NnsBuyFocus::Register) => {
            focus = NnsBuyFocus::Register;
        }
        KeyCode::Up | KeyCode::Down => {
            focus = if key.code == KeyCode::Up {
                prev_focus(focus)
            } else {
                next_focus(focus)
            };
            clear_verify = false;
        }
        KeyCode::Enter => match focus {
            NnsBuyFocus::Search => {
                start_lookup(store, &value, lookup_done_tx);
                return Ok(ReplControl::Continue);
            }
            NnsBuyFocus::Cancel => {
                replace_screen(store, Screen::Home);
                return Ok(ReplControl::Continue);
            }
            NnsBuyFocus::Register => {
                let canonical = match nns::normalize_nns_name(&value) {
                    Ok(n) => n,
                    Err(e) => {
                        status = Some(format!("Error: {e}"));
                        replace_screen(
                            store,
                            Screen::NnsBuy {
                                value,
                                cursor,
                                focus,
                                status,
                                lookup_busy: false,
                                verified_name: None,
                            },
                        );
                        return Ok(ReplControl::Continue);
                    }
                };
                if verified_name.as_deref() != Some(canonical.as_str()) {
                    status = Some(
                        "Error: Search for the name first to confirm it is available".into(),
                    );
                } else {
                    replace_screen(
                        store,
                        Screen::NnsBuy {
                            value: value.clone(),
                            cursor,
                            focus,
                            status: None,
                            lookup_busy: true,
                            verified_name: Some(canonical.clone()),
                        },
                    );
                    if let Err(e) =
                        schedule_nns_register(store, rt, done_tx.clone(), &canonical)
                    {
                        replace_screen(
                            store,
                            Screen::NnsBuy {
                                value,
                                cursor,
                                focus,
                                status: Some(format!("Error: {e}")),
                                lookup_busy: false,
                                verified_name: Some(canonical),
                            },
                        );
                    }
                    return Ok(ReplControl::Continue);
                }
            }
            NnsBuyFocus::Name => {
                start_lookup(store, &value, lookup_done_tx);
                return Ok(ReplControl::Continue);
            }
        },
        KeyCode::Char(c) if focus == NnsBuyFocus::Name => {
            insert_char(&mut value, &mut cursor, c);
            clear_verify = true;
        }
        KeyCode::Backspace if focus == NnsBuyFocus::Name => {
            delete_char(&mut value, &mut cursor);
            clear_verify = true;
        }
        KeyCode::Left if focus == NnsBuyFocus::Name => {
            cursor = cursor.saturating_sub(1);
        }
        KeyCode::Right if focus == NnsBuyFocus::Name => {
            cursor = (cursor + 1).min(value.chars().count());
        }
        _ => {}
    }

    replace_screen(
        store,
        Screen::NnsBuy {
            value,
            cursor,
            focus,
            status: if clear_verify { None } else { status },
            lookup_busy: false,
            verified_name: if clear_verify {
                None
            } else {
                verified_name
            },
        },
    );
    Ok(ReplControl::Continue)
}

fn start_lookup(
    store: &mut UIStore,
    value: &str,
    lookup_done_tx: &mpsc::UnboundedSender<NnsLookupCompletion>,
) {
    let Screen::NnsBuy {
        cursor,
        focus,
        ..
    } = store.state.screen.clone()
    else {
        return;
    };
    replace_screen(
        store,
        Screen::NnsBuy {
            value: value.to_string(),
            cursor,
            focus,
            status: None,
            lookup_busy: true,
            verified_name: None,
        },
    );
    schedule_nns_lookup(value.to_string(), lookup_done_tx.clone());
}

fn next_focus(f: NnsBuyFocus) -> NnsBuyFocus {
    match f {
        NnsBuyFocus::Name => NnsBuyFocus::Search,
        NnsBuyFocus::Search => NnsBuyFocus::Cancel,
        NnsBuyFocus::Cancel => NnsBuyFocus::Register,
        NnsBuyFocus::Register => NnsBuyFocus::Name,
    }
}

fn prev_focus(f: NnsBuyFocus) -> NnsBuyFocus {
    match f {
        NnsBuyFocus::Name => NnsBuyFocus::Register,
        NnsBuyFocus::Search => NnsBuyFocus::Name,
        NnsBuyFocus::Cancel => NnsBuyFocus::Search,
        NnsBuyFocus::Register => NnsBuyFocus::Cancel,
    }
}

fn insert_char(line: &mut String, cursor: &mut usize, c: char) {
    if c.is_ascii_uppercase() {
        return;
    }
    let byte_index = line
        .char_indices()
        .map(|(i, _)| i)
        .nth(*cursor)
        .unwrap_or(line.len());
    line.insert(byte_index, c.to_ascii_lowercase());
    *cursor = cursor.saturating_add(1);
}

fn delete_char(line: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let from = cursor.saturating_sub(1);
    let before = line.chars().take(from);
    let after = line.chars().skip(*cursor);
    *line = before.chain(after).collect();
    *cursor = from;
}
