//! Home screen: tabs, wallet CTAs, menu list.

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;

use super::input::{esc_back, list_activate};
use super::{replace_screen, schedule_cmd};
use crate::command::{Commands, WalletCli};
use crate::tui::command_runner::TuiRuntime;
use crate::tui::components::home::cta_key_to_index;
use crate::tui::components::menus::MAIN_MENU;
use crate::tui::screens::{TuiControl, Screen};
use crate::tui::store::{UIStore, UiAction};
use tokio::sync::mpsc;

pub(super) async fn handle_home(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &TuiRuntime,
    done_tx: &mpsc::UnboundedSender<crate::tui::command_runner::JobCompletion>,
    _balance_done_tx: &mpsc::UnboundedSender<crate::tui::command_runner::BalanceRefreshCompletion>,
    price_done_tx: &mpsc::UnboundedSender<Result<f64, String>>,
) -> Result<TuiControl, NockAppError> {
    match key.code {
        KeyCode::Left | KeyCode::Char('h') => {
            store.dispatch(UiAction::HomeTabPrev);
            return Ok(TuiControl::Continue);
        }
        KeyCode::Right | KeyCode::Char('l') => {
            store.dispatch(UiAction::HomeTabNext);
            return Ok(TuiControl::Continue);
        }
        KeyCode::Char('1') => {
            store.dispatch(UiAction::SetHomeTab(0));
            return Ok(TuiControl::Continue);
        }
        KeyCode::Char('2') => {
            store.dispatch(UiAction::SetHomeTab(1));
            return Ok(TuiControl::Continue);
        }
        KeyCode::Char('r') if store.state.home_tab == 0 => {
            super::super::command_runner::schedule_price_fetch(store, price_done_tx);
            replace_screen(store, Screen::receive_new(true));
            schedule_cmd(
                store,
                rt,
                done_tx,
                Commands::ListActiveAddresses,
                "ListActiveAddresses",
            );
            return Ok(TuiControl::Continue);
        }
        _ => {}
    }

    if store.state.home_tab == 0 {
        if let KeyCode::Char(c) = key.code {
            if esc_back(key.code) {
                return Ok(TuiControl::Quit);
            }
            match cta_key_to_index(c) {
                Some(0) => {
                    replace_screen(store, crate::tui::send_simple::new_screen());
                    return Ok(TuiControl::Continue);
                }
                Some(1) => {
                    super::super::command_runner::schedule_price_fetch(store, price_done_tx);
                    replace_screen(store, Screen::receive_new(true));
                    schedule_cmd(
                        store,
                        rt,
                        done_tx,
                        Commands::ListActiveAddresses,
                        "ListActiveAddresses",
                    );
                    return Ok(TuiControl::Continue);
                }
                Some(2) => {
                    replace_screen(store, Screen::nns_buy_new());
                    return Ok(TuiControl::Continue);
                }
                _ => {}
            }
        }
        if key.code == KeyCode::Char('r') {
            return Ok(TuiControl::Continue);
        }
        if esc_back(key.code) {
            return Ok(TuiControl::Quit);
        }
        return Ok(TuiControl::Continue);
    }

    // Menu tab
    let mut sel = store.state.menu_sel;
    match list_activate(&mut sel, MAIN_MENU.len(), key.code) {
        Err(()) => {
            store.dispatch(UiAction::SetMenuSel(sel));
            Ok(TuiControl::Continue)
        }
        Ok(None) => {
            if esc_back(key.code) {
                return Ok(TuiControl::Quit);
            }
            store.dispatch(UiAction::SetMenuSel(sel));
            Ok(TuiControl::Continue)
        }
        Ok(Some(i)) => {
            store.dispatch(UiAction::SetMenuSel(sel));
            super::menus::navigate_main_menu_item(store, i);
            Ok(TuiControl::Continue)
        }
    }
}

