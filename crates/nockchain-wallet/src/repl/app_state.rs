//! Aggregates REPL screen plus ephemeral UI state (toast, sync progress watch).

use ratatui::widgets::ListState;
use tokio::sync::watch;

use super::screens::Screen;

/// Which UI region receives ↑/↓ before normal screen handlers (when not overridden).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PanelFocus {
    #[default]
    Menu,
    Output,
}

impl PanelFocus {
    pub(crate) fn toggle(self) -> Self {
        match self {
            PanelFocus::Menu => PanelFocus::Output,
            PanelFocus::Output => PanelFocus::Menu,
        }
    }
}

/// Full REPL UI state: primary screen, optional toast after success, optional sync progress reader,
/// and the last captured markdown/kernel output from a wallet command.
pub(crate) struct AppState {
    pub screen: Screen,
    pub toast: Option<String>,
    pub sync_progress: Option<watch::Receiver<(usize, usize)>>,
    /// Terminal-rendered markdown text from the last `markdown` effect(s), shown in the output panel.
    pub last_command_output: String,
    /// Vertical scroll (wrapped lines) for the output panel.
    pub output_scroll: u16,
    /// Scroll position for menu [`List`](ratatui::widgets::List) widgets (long menus).
    pub list_state: ListState,
    /// Whether ↑/↓ scroll the main menu or the output panel.
    pub panel_focus: PanelFocus,
}

impl AppState {
    pub fn new(screen: Screen) -> Self {
        Self {
            screen,
            toast: None,
            sync_progress: None,
            last_command_output: String::new(),
            output_scroll: 0,
            list_state: ListState::default(),
            panel_focus: PanelFocus::Menu,
        }
    }
}
