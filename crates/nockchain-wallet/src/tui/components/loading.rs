//! Braille spinner and centered loading paragraph (kernel / command progress).

use ratatui::layout::Alignment;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Wrap};

use super::theme::{LOADING_BRAND_PALETTE, SPLASH_BRAND, THEME_SHADOW};
use crate::tui::app_state::AppState;

pub(crate) fn braille_spinner_char(tick: u64) -> &'static str {
    const SPIN: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    SPIN[tick as usize % SPIN.len()]
}

/// One animated line: each non-space character cycles through [`super::theme::LOADING_BRAND_PALETTE`]
/// with a traveling phase plus slow drift so the bright band pulses across “NOCKCHAIN”.
pub(crate) fn splash_brand_loading_line(tick: u64) -> Line<'static> {
    let palette = LOADING_BRAND_PALETTE;
    let n = palette.len().max(1);
    let travel = tick as usize;
    // Slow phase drift so the wave doesn’t repeat identically frame-to-frame (overall breathing).
    let breathe = (tick / 8) as usize % n;
    let mut letter_i = 0usize;
    let mut spans = Vec::new();
    for ch in SPLASH_BRAND.chars() {
        if ch.is_whitespace() {
            spans.push(Span::styled(
                ch.to_string(),
                Style::default().fg(THEME_SHADOW),
            ));
            continue;
        }
        let idx = (letter_i + travel + breathe) % n;
        spans.push(Span::styled(
            ch.to_string(),
            Style::default()
                .fg(palette[idx])
                .add_modifier(Modifier::BOLD),
        ));
        letter_i += 1;
    }
    Line::from(spans)
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
        splash_brand_loading_line(tick),
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
