//! Home screen: tabs, wallet CTAs, menu list.

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;

use super::input::{esc_back, list_activate};
use super::{replace_screen, schedule_cmd};
use crate::command::{Commands, WalletCli};
use crate::repl::command_runner::ReplRuntime;
use crate::repl::components::home::cta_key_to_index;
use crate::repl::components::menus::MAIN_MENU;
use crate::repl::screens::{ReplControl, Screen};
use crate::repl::store::{UIStore, UiAction};
use tokio::sync::mpsc;

pub(super) async fn handle_home(
    _cli: &WalletCli,
    store: &mut UIStore,
    key: KeyEvent,
    rt: &ReplRuntime,
    done_tx: &mpsc::UnboundedSender<crate::repl::command_runner::JobCompletion>,
    _balance_done_tx: &mpsc::UnboundedSender<crate::repl::command_runner::BalanceRefreshCompletion>,
    price_done_tx: &mpsc::UnboundedSender<Result<f64, String>>,
) -> Result<ReplControl, NockAppError> {
    match key.code {
        KeyCode::Left | KeyCode::Char('h') => {
            store.dispatch(UiAction::HomeTabPrev);
            return Ok(ReplControl::Continue);
        }
        KeyCode::Right | KeyCode::Char('l') => {
            store.dispatch(UiAction::HomeTabNext);
            return Ok(ReplControl::Continue);
        }
        KeyCode::Char('1') => {
            store.dispatch(UiAction::SetHomeTab(0));
            return Ok(ReplControl::Continue);
        }
        KeyCode::Char('2') => {
            store.dispatch(UiAction::SetHomeTab(1));
            return Ok(ReplControl::Continue);
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
            return Ok(ReplControl::Continue);
        }
        _ => {}
    }

    if store.state.home_tab == 0 {
        if let KeyCode::Char(c) = key.code {
            if esc_back(key.code) {
                return Ok(ReplControl::Quit);
            }
            match cta_key_to_index(c) {
                Some(0) => {
                    replace_screen(store, crate::repl::send_simple::new_screen());
                    return Ok(ReplControl::Continue);
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
                    return Ok(ReplControl::Continue);
                }
                Some(2) => {
                    replace_screen(store, Screen::nns_buy_new());
                    return Ok(ReplControl::Continue);
                }
                _ => {}
            }
        }
        if key.code == KeyCode::Char('r') {
            return Ok(ReplControl::Continue);
        }
        if esc_back(key.code) {
            return Ok(ReplControl::Quit);
        }
        return Ok(ReplControl::Continue);
    }

    // Menu tab
    let mut sel = store.state.menu_sel;
    match list_activate(&mut sel, MAIN_MENU.len(), key.code) {
        Err(()) => {
            store.dispatch(UiAction::SetMenuSel(sel));
            Ok(ReplControl::Continue)
        }
        Ok(None) => {
            if esc_back(key.code) {
                return Ok(ReplControl::Quit);
            }
            store.dispatch(UiAction::SetMenuSel(sel));
            Ok(ReplControl::Continue)
        }
        Ok(Some(i)) => {
            store.dispatch(UiAction::SetMenuSel(sel));
            super::menus::navigate_main_menu_item(store, i);
            Ok(ReplControl::Continue)
        }
    }
}

