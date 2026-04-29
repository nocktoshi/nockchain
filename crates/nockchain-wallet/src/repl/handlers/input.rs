//! Shared key handling for balance/output scroll and line editing.

use crossterm::event::{KeyCode, KeyEvent};

use crate::repl::app_state::{AppState, PanelFocus};
use crate::repl::screens::Screen;

/// ↑/↓ when the balance sidebar is focused (scroll clamp in `draw_ui`).
pub(super) fn try_balance_scroll_keys(app: &mut AppState, key: KeyEvent) -> bool {
    if app.panel_focus != PanelFocus::Balance {
        return false;
    }
    if !matches!(app.screen, Screen::Main { .. }) {
        return false;
    }
    const LINE_STEP: u16 = 3;
    const PAGE_STEP: u16 = 6;
    match key.code {
        KeyCode::Up => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Down => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_add(LINE_STEP);
            true
        }
        KeyCode::PageUp => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_sub(PAGE_STEP);
            true
        }
        KeyCode::PageDown => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_add(PAGE_STEP);
            true
        }
        KeyCode::Home => {
            app.balance_panel.scroll = 0;
            true
        }
        KeyCode::End => {
            app.balance_panel.scroll = u16::MAX;
            true
        }
        KeyCode::Char('k') => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Char('j') => {
            app.balance_panel.scroll = app.balance_panel.scroll.saturating_add(LINE_STEP);
            true
        }
        _ => false,
    }
}

/// ↑/↓ and page keys when the output panel is focused (scroll clamp in `draw_ui`).
pub(super) fn try_output_scroll_keys(app: &mut AppState, key: KeyEvent) -> bool {
    if app.panel_focus != PanelFocus::Output {
        return false;
    }
    if matches!(app.screen, Screen::Running { .. }) {
        return false;
    }
    const LINE_STEP: u16 = 3;
    const PAGE_STEP: u16 = 6;
    match key.code {
        KeyCode::Up => {
            app.output_scroll = app.output_scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Down => {
            app.output_scroll = app.output_scroll.saturating_add(LINE_STEP);
            true
        }
        KeyCode::PageUp => {
            app.output_scroll = app.output_scroll.saturating_sub(PAGE_STEP);
            true
        }
        KeyCode::PageDown => {
            app.output_scroll = app.output_scroll.saturating_add(PAGE_STEP);
            true
        }
        KeyCode::Home => {
            app.output_scroll = 0;
            true
        }
        KeyCode::End => {
            app.output_scroll = u16::MAX;
            true
        }
        KeyCode::Char('k') => {
            app.output_scroll = app.output_scroll.saturating_sub(LINE_STEP);
            true
        }
        KeyCode::Char('j') => {
            app.output_scroll = app.output_scroll.saturating_add(LINE_STEP);
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
