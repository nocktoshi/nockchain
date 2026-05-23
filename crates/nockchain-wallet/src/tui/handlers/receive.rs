//! Receive screen: copy address to clipboard.

use crossterm::event::{KeyCode, KeyEvent};
use nockapp::NockAppError;

use super::input::esc_back;
use super::replace_screen;
use crate::tui::clipboard;
use crate::tui::screens::{TuiControl, Screen};
use crate::tui::store::{UIStore, UiAction};

pub(super) async fn handle_receive(
    store: &mut UIStore,
    key: KeyEvent,
) -> Result<TuiControl, NockAppError> {
    let Screen::Receive {
        address,
        loading,
        error,
        copy_focused,
    } = store.state.screen.clone()
    else {
        return Ok(TuiControl::Continue);
    };

    if loading {
        return Ok(TuiControl::Continue);
    }

    if esc_back(key.code) {
        replace_screen(store, Screen::Home);
        return Ok(TuiControl::Continue);
    }

    if key.code == KeyCode::Enter {
        if let Some(addr) = address {
            if error.is_none() {
                match clipboard::copy_to_clipboard(&addr) {
                    Ok(()) => {
                        store.dispatch(UiAction::TakeToast);
                        store.state.toast = Some("Address copied to clipboard".into());
                    }
                    Err(msg) => {
                        replace_screen(
                            store,
                            Screen::Receive {
                                address: Some(addr),
                                loading: false,
                                error: Some(msg),
                                copy_focused,
                            },
                        );
                        return Ok(TuiControl::Continue);
                    }
                }
            }
        }
        return Ok(TuiControl::Continue);
    }

    Ok(TuiControl::Continue)
}
