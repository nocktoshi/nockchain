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
    /// Full screen swap (routes through [`super::apply_ui_action`]).
    ReplaceScreen(Screen),
    /// Leave splash for main menu + menu focus (balance refresh scheduled by caller).
    EnterMainFromSplash,
    /// Swap to [`Screen::Running`] and attach sync progress receiver (`sender` lives in dispatch hooks).
    EnterRunningWalletJob {
        cmd: Commands,
        label: String,
        progress_rx: watch::Receiver<(usize, usize)>,
    },
    /// Main-menu sidebar balance refresh (receiver only; sender held by spawned task).
    BeginBalanceSidebarFetch {
        progress_rx: watch::Receiver<(usize, usize)>,
    },
    JobCompleted {
        result: Result<(), NockAppError>,
        events: Vec<crate::wallet_outcome::WalletEvent>,
    },
    BalanceSidebarCompleted {
        nonce: u64,
        result: Result<(), NockAppError>,
        events: Vec<crate::wallet_outcome::WalletEvent>,
    },
    NudgeBalanceScroll {
        delta: i32,
    },
    NudgeOutputScroll {
        delta: i32,
    },
    SetBalanceScroll(u16),
    SetOutputScroll(u16),
}
