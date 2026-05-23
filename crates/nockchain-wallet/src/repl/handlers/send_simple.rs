//! Simple send form (home CTA) keyboard handling.

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;
use tokio::sync::mpsc;

use super::input::{esc_back, try_output_scroll_keys};
use super::replace_screen;
use crate::repl::command_runner::{
    schedule_send_simple_create_and_send, schedule_send_simple_plan, JobCompletion, ReplRuntime,
    SendSimplePlanCompletion,
};
use crate::repl::screens::{ReplControl, Screen, SendSimpleFocus, SendSimplePhase};
use crate::repl::send_simple::{build_create_tx_command, max_amount_string};
use crate::repl::store::{UIStore, UiAction};

pub(super) async fn handle_send_simple(
    store: &mut UIStore,
    key: KeyEvent,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
    plan_done_tx: &mpsc::UnboundedSender<SendSimplePlanCompletion>,
) -> Result<ReplControl, NockAppError> {
    let taken = store.state.screen.clone();
    let Screen::SendSimple {
        mut amount,
        mut recipient,
        mut amount_cursor,
        mut recipient_cursor,
        mut focus,
        phase,
        mut status,
        review_scroll,
    } = taken
    else {
        return Ok(ReplControl::Continue);
    };

    match phase {
        SendSimplePhase::Planning => return Ok(ReplControl::Continue),
        SendSimplePhase::Review { cmd, preview } => {
            return handle_send_simple_review(
                store,
                key,
                rt,
                done_tx,
                amount,
                recipient,
                amount_cursor,
                recipient_cursor,
                focus,
                cmd,
                preview,
                status,
                review_scroll,
            )
            .await;
        }
        SendSimplePhase::Form => {}
    }

    if esc_back(key.code) {
        replace_screen(store, Screen::Home);
        return Ok(ReplControl::Continue);
    }

    match key.code {
        KeyCode::Tab => {
            focus = next_focus(focus);
            status = None;
        }
        KeyCode::BackTab => {
            focus = prev_focus(focus);
            status = None;
        }
        KeyCode::Left if matches!(focus, SendSimpleFocus::Cancel | SendSimpleFocus::Continue) => {
            focus = SendSimpleFocus::Cancel;
            status = None;
        }
        KeyCode::Right if matches!(focus, SendSimpleFocus::Cancel | SendSimpleFocus::Continue) => {
            focus = SendSimpleFocus::Continue;
            status = None;
        }
        KeyCode::Char('m') if focus == SendSimpleFocus::Amount => {
            if let Some(max) = max_amount_string(&store.state.balance_panel.events) {
                amount = max;
                amount_cursor = amount.chars().count();
            }
            status = None;
        }
        KeyCode::Enter => match focus {
            SendSimpleFocus::Cancel => {
                replace_screen(store, Screen::Home);
                return Ok(ReplControl::Continue);
            }
            SendSimpleFocus::Continue => match build_create_tx_command(&amount, &recipient) {
                Ok(cmd) => {
                    replace_screen(
                        store,
                        Screen::SendSimple {
                            amount: amount.clone(),
                            recipient: recipient.clone(),
                            amount_cursor,
                            recipient_cursor,
                            focus,
                            phase: SendSimplePhase::Planning,
                            status: None,
                            review_scroll: 0,
                        },
                    );
                    schedule_send_simple_plan(
                        rt.clone(),
                        cmd,
                        plan_done_tx.clone(),
                    );
                    return Ok(ReplControl::Continue);
                }
                Err(e) => status = Some(e),
            },
            SendSimpleFocus::Amount => {
                focus = SendSimpleFocus::Recipient;
                status = None;
            }
            SendSimpleFocus::Recipient => {
                focus = SendSimpleFocus::Continue;
                status = None;
            }
        },
        KeyCode::Char(c) if focus == SendSimpleFocus::Amount => {
            insert_char(&mut amount, &mut amount_cursor, c);
            status = None;
        }
        KeyCode::Char(c) if focus == SendSimpleFocus::Recipient => {
            insert_char(&mut recipient, &mut recipient_cursor, c);
            status = None;
        }
        KeyCode::Backspace if focus == SendSimpleFocus::Amount => {
            delete_char(&mut amount, &mut amount_cursor);
            status = None;
        }
        KeyCode::Backspace if focus == SendSimpleFocus::Recipient => {
            delete_char(&mut recipient, &mut recipient_cursor);
            status = None;
        }
        KeyCode::Left if focus == SendSimpleFocus::Amount => {
            amount_cursor = amount_cursor.saturating_sub(1);
            status = None;
        }
        KeyCode::Right if focus == SendSimpleFocus::Amount => {
            amount_cursor = (amount_cursor + 1).min(amount.chars().count());
            status = None;
        }
        KeyCode::Left if focus == SendSimpleFocus::Recipient => {
            recipient_cursor = recipient_cursor.saturating_sub(1);
            status = None;
        }
        KeyCode::Right if focus == SendSimpleFocus::Recipient => {
            recipient_cursor = (recipient_cursor + 1).min(recipient.chars().count());
            status = None;
        }
        KeyCode::Up | KeyCode::Down => {
            focus = if key.code == KeyCode::Up {
                prev_focus(focus)
            } else {
                next_focus(focus)
            };
            status = None;
        }
        _ => {}
    }

    replace_screen(
        store,
        Screen::SendSimple {
            amount,
            recipient,
            amount_cursor,
            recipient_cursor,
            focus,
            phase,
            status,
            review_scroll,
        },
    );
    Ok(ReplControl::Continue)
}

async fn handle_send_simple_review(
    store: &mut UIStore,
    key: KeyEvent,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<JobCompletion>,
    amount: String,
    recipient: String,
    amount_cursor: usize,
    recipient_cursor: usize,
    mut focus: SendSimpleFocus,
    cmd: crate::command::Commands,
    preview: String,
    status: Option<String>,
    mut review_scroll: u16,
) -> Result<ReplControl, NockAppError> {
    if esc_back(key.code) {
        replace_screen(
            store,
            Screen::SendSimple {
                amount,
                recipient,
                amount_cursor,
                recipient_cursor,
                focus: SendSimpleFocus::Continue,
                phase: SendSimplePhase::Form,
                status: None,
                review_scroll: 0,
            },
        );
        return Ok(ReplControl::Continue);
    }
    if try_output_scroll_keys(store, key) {
        return Ok(ReplControl::Continue);
    }
    if matches!(
        key.code,
        KeyCode::Up | KeyCode::Down | KeyCode::PageUp | KeyCode::PageDown
    ) {
        let delta = match key.code {
            KeyCode::Up | KeyCode::PageUp => -3,
            _ => 3,
        };
        review_scroll = review_scroll.saturating_add_signed(delta);
        replace_screen(
            store,
            Screen::SendSimple {
                amount,
                recipient,
                amount_cursor,
                recipient_cursor,
                focus,
                phase: SendSimplePhase::Review { cmd, preview },
                status,
                review_scroll,
            },
        );
        return Ok(ReplControl::Continue);
    }

    let mut next_phase = SendSimplePhase::Review {
        cmd: cmd.clone(),
        preview: preview.clone(),
    };
    match key.code {
        KeyCode::Tab => focus = next_focus(focus),
        KeyCode::BackTab => focus = prev_focus(focus),
        KeyCode::Left if matches!(focus, SendSimpleFocus::Cancel | SendSimpleFocus::Continue) => {
            focus = SendSimpleFocus::Cancel;
        }
        KeyCode::Right if matches!(focus, SendSimpleFocus::Cancel | SendSimpleFocus::Continue) => {
            focus = SendSimpleFocus::Continue;
        }
        KeyCode::Enter => match focus {
            SendSimpleFocus::Cancel => {
                next_phase = SendSimplePhase::Form;
                focus = SendSimpleFocus::Continue;
                review_scroll = 0;
            }
            SendSimpleFocus::Continue => {
                schedule_send_simple_create_and_send(store, rt, done_tx.clone(), cmd);
                return Ok(ReplControl::Continue);
            }
            _ => {}
        },
        KeyCode::Up | KeyCode::Down => {
            focus = if key.code == KeyCode::Up {
                prev_focus(focus)
            } else {
                next_focus(focus)
            };
        }
        _ => {}
    }

    replace_screen(
        store,
        Screen::SendSimple {
            amount,
            recipient,
            amount_cursor,
            recipient_cursor,
            focus,
            phase: next_phase,
            status,
            review_scroll,
        },
    );
    Ok(ReplControl::Continue)
}

pub(crate) fn apply_send_simple_plan_result(
    store: &mut UIStore,
    result: SendSimplePlanCompletion,
) {
    let Screen::SendSimple {
        amount,
        recipient,
        amount_cursor,
        recipient_cursor,
        ..
    } = store.state.screen.clone()
    else {
        return;
    };
    match result {
        Ok((preview, cmd)) => {
            store.dispatch(UiAction::ReplaceScreen(Screen::SendSimple {
                amount,
                recipient,
                amount_cursor,
                recipient_cursor,
                focus: SendSimpleFocus::Continue,
                phase: SendSimplePhase::Review { cmd, preview },
                status: None,
                review_scroll: 0,
            }));
        }
        Err(msg) => {
            store.dispatch(UiAction::ReplaceScreen(Screen::SendSimple {
                amount,
                recipient,
                amount_cursor,
                recipient_cursor,
                focus: SendSimpleFocus::Continue,
                phase: SendSimplePhase::Form,
                status: Some(msg),
                review_scroll: 0,
            }));
        }
    }
}

fn next_focus(f: SendSimpleFocus) -> SendSimpleFocus {
    match f {
        SendSimpleFocus::Amount => SendSimpleFocus::Recipient,
        SendSimpleFocus::Recipient => SendSimpleFocus::Cancel,
        SendSimpleFocus::Cancel => SendSimpleFocus::Continue,
        SendSimpleFocus::Continue => SendSimpleFocus::Amount,
    }
}

fn prev_focus(f: SendSimpleFocus) -> SendSimpleFocus {
    match f {
        SendSimpleFocus::Amount => SendSimpleFocus::Continue,
        SendSimpleFocus::Recipient => SendSimpleFocus::Amount,
        SendSimpleFocus::Cancel => SendSimpleFocus::Continue,
        SendSimpleFocus::Continue => SendSimpleFocus::Cancel,
    }
}

fn insert_char(line: &mut String, cursor: &mut usize, c: char) {
    let byte_index = line
        .char_indices()
        .map(|(i, _)| i)
        .nth(*cursor)
        .unwrap_or(line.len());
    line.insert(byte_index, c);
    *cursor = cursor.saturating_add(1);
}

fn delete_char(line: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let current = *cursor;
    let from = current - 1;
    let before = line.chars().take(from);
    let after = line.chars().skip(current);
    *line = before.chain(after).collect();
    *cursor = from;
}
