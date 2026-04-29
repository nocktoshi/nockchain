//! Braille spinner and centered loading paragraph (kernel / command progress).

use ratatui::layout::Alignment;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Wrap};

use crate::repl::app_state::AppState;

use super::theme::SPLASH_BRAND;

pub(crate) fn braille_spinner_char(tick: u64) -> &'static str {
    const SPIN: &[&str] =
        &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    SPIN[tick as usize % SPIN.len()]
}

pub(crate) fn sync_attempt_message(app: &AppState) -> Option<String> {
    app.sync_progress.as_ref().and_then(|rx| {
        let (a, m) = *rx.borrow();
        if a > 0 {
            Some(format!("Sync attempt {a}/{m}"))
        } else {
            None
        }
    })
}

/// Single loading UI: brand line, braille spinner + white status label, then sync attempt or kernel hint.
pub(crate) fn loading_indicator_paragraph<'a>(
    app: &AppState,
    tick: u64,
    outer_block: Block<'a>,
    status_label: &'a str,
) -> Paragraph<'a> {
    let spin = braille_spinner_char(tick);
    let sync_line = sync_attempt_message(app);
    let sync_span = match sync_line {
        Some(s) => Span::styled(s, Style::default().fg(Color::Yellow)),
        None => Span::styled(
            "Running wallet kernel…",
            Style::default().fg(Color::DarkGray),
        ),
    };
    Paragraph::new(vec![
        Line::from(Span::styled(
            SPLASH_BRAND,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled(spin, Style::default().fg(Color::Green)),
            Span::raw("  "),
            Span::styled(status_label, Style::default().fg(Color::White)),
        ]),
        Line::from(""),
        Line::from(sync_span),
    ])
    .alignment(Alignment::Center)
    .wrap(Wrap { trim: true })
    .block(outer_block)
}
