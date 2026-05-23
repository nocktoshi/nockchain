//! Aggregates TUI screen plus ephemeral UI state (toast, sync progress watch).
//! Persisted connection/API settings live in [`crate::tui::wallet_api::WalletSessionState`]
//! (`session.json`, GET/POST `/v1/wallet/state`).

use std::time::Instant;

use ratatui::widgets::ListState;
use tokio::sync::watch;

use super::screens::Screen;
use super::view;
use crate::command::Commands;

/// Bottom status/output panel: visible while a command runs or meaningful output exists.
pub(crate) fn status_modal_visible(state: &UiState) -> bool {
    if let Screen::Running { restore, cmd, .. } = &state.screen {
        if matches!(**restore, Screen::Receive { .. })
            && matches!(cmd, Commands::ListActiveAddresses)
        {
            return false;
        }
        if matches!(**restore, Screen::NnsBuy { .. }) && matches!(cmd, Commands::CreateTx { .. }) {
            return false;
        }
        return true;
    }
    !state.last_command_output.is_empty()
        && state.last_command_output != view::NO_STRUCTURED_OUTPUT
}

/// CoinGecko USD price for the home hero.
#[derive(Debug, Clone, Default)]
pub(crate) struct PriceState {
    pub usd_per_coin: Option<f64>,
    pub loading: bool,
    pub error: Option<String>,
    pub fetched_at: Option<Instant>,
}

/// Presentation-only: animation frame clock, etc. (no wallet semantics).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UiFx {
    /// Drives braille / create-tx spinners; advanced by [`super::store::UiAction::Tick`].
    pub frame_clock: u64,
}

/// Cached balance markdown for the home wallet tab (from `ShowBalance` / sidebar refresh).
#[derive(Debug, Clone)]
pub(crate) struct BalancePanelState {
    pub text: String,
    pub scroll: u16,
    pub loading: bool,
    pub error: Option<String>,
    /// Latest balance snapshot events (for hero NOCK + USD math).
    pub events: Vec<crate::wallet_outcome::WalletEvent>,
    /// Active receive address + optional primary `.nock` name from the registry API.
    pub identity_loading: bool,
    pub address: Option<String>,
    pub nockname: Option<String>,
}

impl Default for BalancePanelState {
    fn default() -> Self {
        Self {
            text: String::new(),
            scroll: 0,
            loading: false,
            error: None,
            events: Vec::new(),
            identity_loading: false,
            address: None,
            nockname: None,
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
    /// Vertical scroll (wrapped lines) for the status/output panel.
    pub output_scroll: u16,
    /// Scroll position for menu [`List`](ratatui::widgets::List) widgets (long menus).
    pub list_state: ListState,
    pub balance_panel: BalancePanelState,
    /// Bumped when starting a sidebar balance fetch or any queued wallet command.
    pub balance_job_nonce: u64,
    pub ui_fx: UiFx,
    /// `0` = Wallet tab, `1` = Menu tab on [`Screen::Home`].
    pub home_tab: usize,
    /// Selected row on the Menu tab (`MAIN_MENU`).
    pub menu_sel: usize,
    pub price: PriceState,
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
            balance_panel: BalancePanelState::default(),
            balance_job_nonce: 0,
            ui_fx: UiFx::default(),
            home_tab: 0,
            menu_sel: 0,
            price: PriceState::default(),
        }
    }
}
