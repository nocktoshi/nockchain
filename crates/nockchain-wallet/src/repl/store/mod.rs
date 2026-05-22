//! Redux-style UI store: [`UIStore::dispatch`] drives all transitions via [`apply_ui_action`].

mod action;
mod apply;

pub(crate) use action::UiAction;
pub(crate) use apply::apply_ui_action;

use crate::repl::app_state::UiState;
use crate::repl::screens::Screen;

/// Holds [`UiState`] and exposes the single mutation entry point [`Self::dispatch`].
pub(crate) struct UIStore {
    pub(crate) state: UiState,
}

impl UIStore {
    pub(crate) fn new(initial_screen: Screen) -> Self {
        Self {
            state: UiState::new(initial_screen),
        }
    }

    pub(crate) fn dispatch(&mut self, action: UiAction) {
        apply_ui_action(&mut self.state, action);
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    use crate::command::Commands;
    use crate::repl::app_state::UiState;
    use crate::repl::screens::Screen;

    use super::{apply_ui_action, UiAction};

    #[test]
    fn tick_advances_frame_clock() {
        let mut s = UiState::new(Screen::Splash);
        apply_ui_action(&mut s, UiAction::Tick);
        assert_eq!(s.ui_fx.frame_clock, 1);
    }

    #[test]
    fn replace_screen_action() {
        let mut s = UiState::new(Screen::Splash);
        apply_ui_action(&mut s, UiAction::ReplaceScreen(Screen::Main { sel: 0 }));
        assert!(matches!(s.screen, Screen::Main { sel: 0 }));
    }

    #[test]
    fn balance_sidebar_completed_ignores_stale_nonce() {
        let mut s = UiState::new(Screen::Main { sel: 0 });
        s.balance_job_nonce = 5;
        s.balance_panel.text = "keep".into();
        apply_ui_action(
            &mut s,
            UiAction::BalanceSidebarCompleted {
                nonce: 4,
                result: Ok(()),
                markdown: "new".into(),
            },
        );
        assert_eq!(s.balance_panel.text, "keep");
    }

    #[test]
    fn balance_sidebar_completed_applies_matching_nonce() {
        let mut s = UiState::new(Screen::Main { sel: 0 });
        s.balance_job_nonce = 5;
        apply_ui_action(
            &mut s,
            UiAction::BalanceSidebarCompleted {
                nonce: 5,
                result: Ok(()),
                markdown: "fresh".into(),
            },
        );
        assert_eq!(s.balance_panel.text, "fresh");
    }

    #[test]
    fn enter_running_skips_when_already_running() {
        let mut s = UiState::new(Screen::Main { sel: 0 });
        let (tx, rx) = watch::channel((0usize, 5usize));
        apply_ui_action(
            &mut s,
            UiAction::EnterRunningWalletJob {
                cmd: Commands::ShowBalance,
                label: "first".into(),
                progress_rx: rx,
            },
        );
        drop(tx);
        let Screen::Running { label, .. } = &s.screen else {
            panic!("expected Running");
        };
        assert_eq!(label, "first");
        let (tx2, rx2) = watch::channel((0usize, 5usize));
        apply_ui_action(
            &mut s,
            UiAction::EnterRunningWalletJob {
                cmd: Commands::ShowBalance,
                label: "second".into(),
                progress_rx: rx2,
            },
        );
        drop(tx2);
        let Screen::Running { label: l2, .. } = &s.screen else {
            panic!("expected Running");
        };
        assert_eq!(l2, "first");
    }

    #[test]
    fn job_completed_ok_restores_screen() {
        let mut s = UiState::new(Screen::Main { sel: 2 });
        let (tx, rx) = watch::channel((0usize, 5usize));
        apply_ui_action(
            &mut s,
            UiAction::EnterRunningWalletJob {
                cmd: Commands::ListNotes,
                label: "run".into(),
                progress_rx: rx,
            },
        );
        drop(tx);
        apply_ui_action(&mut s, UiAction::JobCompleted(Ok(()), "md out".into()));
        assert!(matches!(s.screen, Screen::Main { sel: 2 }));
        assert_eq!(s.last_command_output, "md out");
    }
}
