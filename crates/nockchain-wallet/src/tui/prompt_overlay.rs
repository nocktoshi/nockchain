//! Yes/no and text prompts render in the bottom bar; activity panel keeps the underlay screen.

use crate::tui::screens::{ConfirmThen, Screen, TextThen};

pub(crate) fn has_prompt_overlay(screen: &Screen) -> bool {
    matches!(
        screen,
        Screen::TextPrompt { .. } | Screen::Confirm { .. } | Screen::ExitConfirm { .. }
    )
}

/// Screen drawn in the activity panel while a prompt overlay is active.
pub(crate) fn activity_underlay(screen: &Screen) -> Screen {
    match screen {
        Screen::TextPrompt { underlay, .. }
        | Screen::Confirm { underlay, .. }
        | Screen::ExitConfirm { underlay, .. } => (**underlay).clone(),
        other => other.clone(),
    }
}

/// Screen to restore after a wallet job when the pre-job screen was a prompt overlay.
pub(crate) fn running_restore_screen(screen: Screen) -> Screen {
    activity_underlay(&screen)
}

pub(crate) fn prompt_underlay(screen: &Screen) -> Screen {
    activity_underlay(screen)
}

pub(crate) fn text_prompt_screen(
    underlay: Screen,
    title: impl Into<String>,
    value: String,
    then: TextThen,
) -> Screen {
    Screen::TextPrompt {
        underlay: Box::new(underlay),
        title: title.into(),
        value,
        then,
    }
}

pub(crate) fn confirm_prompt_screen(
    underlay: Screen,
    title: impl Into<String>,
    sel: usize,
    labels: &'static [&'static str],
    then: ConfirmThen,
) -> Screen {
    Screen::Confirm {
        underlay: Box::new(underlay),
        title: title.into(),
        sel,
        labels,
        then,
    }
}

pub(crate) fn exit_confirm(underlay: Screen, sel: usize) -> Screen {
    Screen::ExitConfirm {
        underlay: Box::new(underlay),
        sel,
    }
}
