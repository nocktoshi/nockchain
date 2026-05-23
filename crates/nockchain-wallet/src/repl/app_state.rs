//! Aggregates REPL screen plus ephemeral UI state (toast, sync progress watch).
//! Persisted connection/API settings live in [`crate::repl::wallet_api::WalletSessionState`]
//! (`session.json`, GET/POST `/v1/wallet/state`).

use ratatui::widgets::ListState;
use tokio::sync::watch;

use super::screens::Screen;

/// Which UI region receives ↑/↓ before normal screen handlers (when not overridden).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PanelFocus {
    #[default]
    Menu,
    Balance,
    Output,
}

impl PanelFocus {
    pub(crate) fn toggle(self) -> Self {
        match self {
            PanelFocus::Menu => PanelFocus::Balance,
            PanelFocus::Balance => PanelFocus::Output,
            PanelFocus::Output => PanelFocus::Menu,
        }
    }
}

/// Presentation-only: animation frame clock, etc. (no wallet semantics).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UiFx {
    /// Drives braille / create-tx spinners; advanced by [`super::store::UiAction::Tick`].
    pub frame_clock: u64,
}

/// Full REPL UI state: primary screen, optional toast after success, optional sync progress reader,
/// and the last captured markdown/kernel output from a wallet command.
/// Cached balance markdown for the main-menu sidebar (from `ShowBalance` / sidebar refresh).
#[derive(Debug, Clone)]
pub(crate) struct BalancePanelState {
    pub text: String,
    pub scroll: u16,
    pub loading: bool,
    pub error: Option<String>,
}

impl Default for BalancePanelState {
    fn default() -> Self {
        Self {
            text: String::new(),
            scroll: 0,
            loading: false,
            error: None,
        }
    }
}

pub(crate) struct UiState {
    pub screen: Screen,
    pub toast: Option<String>,
    pub sync_progress: Option<watch::Receiver<(usize, usize)>>,
    /// Terminal text rendered from [`Self::last_command_events`] for the output panel.
    pub last_command_output: String,
    /// Structured kernel events from the last wallet command (data layer).
    pub last_command_events: Vec<crate::wallet_outcome::WalletEvent>,
    /// Vertical scroll (wrapped lines) for the output panel.
    pub output_scroll: u16,
    /// Scroll position for menu [`List`](ratatui::widgets::List) widgets (long menus).
    pub list_state: ListState,
    /// Whether ↑/↓ scroll the main menu, balance sidebar, or the output panel.
    pub panel_focus: PanelFocus,
    pub balance_panel: BalancePanelState,
    /// Bumped when starting a sidebar balance fetch or any queued wallet command — stale sidebar completions compare against this.
    pub balance_job_nonce: u64,
    pub ui_fx: UiFx,
}

/// Backwards-compatible alias during migration to [`UIStore`](super::store::UIStore).
pub(crate) type AppState = UiState;

impl UiState {
    pub fn new(screen: Screen) -> Self {
        Self {
            screen,
            toast: None,
            sync_progress: None,
            last_command_output: String::new(),
            last_command_events: Vec::new(),
            output_scroll: 0,
            list_state: ListState::default(),
            panel_focus: PanelFocus::Menu,
            balance_panel: BalancePanelState::default(),
            balance_job_nonce: 0,
            ui_fx: UiFx::default(),
        }
    }
}
