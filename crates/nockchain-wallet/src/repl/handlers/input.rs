//! Shared key handling for balance/output scroll and line editing.

use crossterm::event::{KeyCode, KeyEvent};

use crate::repl::app_state::PanelFocus;
use crate::repl::screens::Screen;
use crate::repl::store::{UIStore, UiAction};

/// ↑/↓ when the balance sidebar is focused (scroll clamp in `draw_ui`).
pub(super) fn try_balance_scroll_keys(store: &mut UIStore, key: KeyEvent) -> bool {
    if store.state.panel_focus != PanelFocus::Balance {
        return false;
    }
    if !matches!(store.state.screen, Screen::Main { .. }) {
        return false;
    }
    const LINE_STEP: i32 = 3;
    const PAGE_STEP: i32 = 6;
    match key.code {
        KeyCode::Up => {
            store.dispatch(UiAction::NudgeBalanceScroll {
                delta: -LINE_STEP,
            });
            true
        }
        KeyCode::Down => {
            store.dispatch(UiAction::NudgeBalanceScroll {
                delta: LINE_STEP,
            });
            true
        }
        KeyCode::PageUp => {
            store.dispatch(UiAction::NudgeBalanceScroll {
                delta: -PAGE_STEP,
            });
            true
        }
        KeyCode::PageDown => {
            store.dispatch(UiAction::NudgeBalanceScroll {
                delta: PAGE_STEP,
            });
            true
        }
        KeyCode::Home => {
            store.dispatch(UiAction::SetBalanceScroll(0));
            true
        }
        KeyCode::End => {
            store.dispatch(UiAction::SetBalanceScroll(u16::MAX));
            true
        }
        KeyCode::Char('k') => {
            store.dispatch(UiAction::NudgeBalanceScroll {
                delta: -LINE_STEP,
            });
            true
        }
        KeyCode::Char('j') => {
            store.dispatch(UiAction::NudgeBalanceScroll {
                delta: LINE_STEP,
            });
            true
        }
        _ => false,
    }
}

/// ↑/↓ and page keys when the output panel is focused (scroll clamp in `draw_ui`).
pub(super) fn try_output_scroll_keys(store: &mut UIStore, key: KeyEvent) -> bool {
    if store.state.panel_focus != PanelFocus::Output {
        return false;
    }
    if matches!(store.state.screen, Screen::Running { .. }) {
        return false;
    }
    const LINE_STEP: i32 = 3;
    const PAGE_STEP: i32 = 6;
    match key.code {
        KeyCode::Up => {
            store.dispatch(UiAction::NudgeOutputScroll {
                delta: -LINE_STEP,
            });
            true
        }
        KeyCode::Down => {
            store.dispatch(UiAction::NudgeOutputScroll {
                delta: LINE_STEP,
            });
            true
        }
        KeyCode::PageUp => {
            store.dispatch(UiAction::NudgeOutputScroll {
                delta: -PAGE_STEP,
            });
            true
        }
        KeyCode::PageDown => {
            store.dispatch(UiAction::NudgeOutputScroll {
                delta: PAGE_STEP,
            });
            true
        }
        KeyCode::Home => {
            store.dispatch(UiAction::SetOutputScroll(0));
            true
        }
        KeyCode::End => {
            store.dispatch(UiAction::SetOutputScroll(u16::MAX));
            true
        }
        KeyCode::Char('k') => {
            store.dispatch(UiAction::NudgeOutputScroll {
                delta: -LINE_STEP,
            });
            true
        }
        KeyCode::Char('j') => {
            store.dispatch(UiAction::NudgeOutputScroll {
                delta: LINE_STEP,
            });
            true
        }
        _ => false,
    }
}

pub(super) fn esc_back(code: KeyCode) -> bool {
    matches!(code, KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q'))
}

pub(super) fn list_activate(sel: &mut usize, len: usize, key: KeyCode) -> Result<Option<usize>, ()> {
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

pub(super) fn edit_line(line: &mut String, key: KeyEvent) {
    match key.code {
        KeyCode::Char(c) => line.push(c),
        KeyCode::Backspace => {
            line.pop();
        }
        _ => {}
    }
}
