//! Redux-style actions for the wallet REPL UI.

use nockapp::NockAppError;
use tokio::sync::watch;

use crate::command::Commands;
use crate::repl::app_state::PanelFocus;
use crate::repl::screens::Screen;

/// All UI state transitions flow through [`super::apply_ui_action`](fn@super::apply_ui_action).
#[derive(Debug)]
pub(crate) enum UiAction {
    /// Advance spinner / animation clock (presentation-only).
    Tick,
    /// Dismiss toast on any key (consumes toast field).
    TakeToast,
    TogglePanelFocus,
    SetPanelFocus(PanelFocus),
    /// Clear command output and hide the status modal.
    DismissStatusOutput,
    /// Full screen swap (routes through [`super::apply_ui_action`]).
    ReplaceScreen(Screen),
    /// Leave splash for home + activity focus (balance refresh scheduled by caller).
    EnterMainFromSplash,
    /// Swap to [`Screen::Running`] and attach sync progress receiver.
    EnterRunningWalletJob {
        cmd: Commands,
        label: String,
        progress_rx: watch::Receiver<(usize, usize)>,
    },
    /// Home balance refresh (receiver only; sender held by spawned task).
    BeginBalanceSidebarFetch {
        progress_rx: watch::Receiver<(usize, usize)>,
    },
    JobCompleted {
        result: Result<(), NockAppError>,
        events: Vec<crate::wallet_outcome::WalletEvent>,
        markdown: String,
    },
    BalanceSidebarCompleted {
        nonce: u64,
        result: Result<(), NockAppError>,
        events: Vec<crate::wallet_outcome::WalletEvent>,
    },
    BeginHomeIdentityFetch,
    HomeIdentityCompleted {
        address: Option<String>,
        nockname: Option<String>,
    },
    NudgeOutputScroll {
        delta: i32,
    },
    SetOutputScroll(u16),
    SetHomeTab(usize),
    HomeTabNext,
    HomeTabPrev,
    SetMenuSel(usize),
    BeginPriceFetch,
    PriceFetched {
        usd_per_coin: f64,
    },
    PriceFetchFailed {
        msg: String,
    },
}
