//! Five-row block letter wordmarks (splash NOCKCHAIN, NNS buy screen).

use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::theme::{pulse_green_rgb, THEME_BG_DEEP};

/// Block “NOCKCHAIN” (53 columns).
pub(crate) const NOCKCHAIN: [&str; 5] = [
    "█   █  ███   ███  █   █  ███  █   █  ███   ███  █   █",
    "██  █ █   █ █     █  █  █     █   █ █   █   █   ██  █",
    "█ █ █ █   █ █     ███   █     █████ █████   █   █ █ █",
    "█  ██ █   █ █     █  █  █     █   █ █   █   █   █  ██",
    "█   █  ███   ███  █   █  ███  █   █ █   █  ███  █   █",
];

/// Block “NNS” (17 columns).
pub(crate) const NNS: [&str; 5] = [
    "█   █  █   █  ███ ",
    "██  █  ██  █ █    ",
    "█ █ █  █ █ █  ███ ",
    "█  ██  █  ██     █",
    "█   █  █   █  ███ ",
];

/// Render block rows centered; `█` uses [`pulse_green_rgb`], gaps use `gap_bg`.
pub(crate) fn render_block_wordmark(
    f: &mut Frame<'_>,
    area: Rect,
    rows: &[&str],
    frame_counter: usize,
    gap_bg: ratatui::style::Color,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let logo_fg = pulse_green_rgb(frame_counter);
    let lines: Vec<Line> = rows
        .iter()
        .map(|row| {
            let spans: Vec<Span> = row
                .chars()
                .map(|ch| {
                    let fg = if ch == '█' { logo_fg } else { gap_bg };
                    Span::styled(
                        ch.to_string(),
                        Style::new()
                            .fg(fg)
                            .bg(gap_bg)
                            .add_modifier(Modifier::BOLD),
                    )
                })
                .collect();
            Line::from(spans)
        })
        .collect();
    let body = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });
    f.render_widget(body, area);
}

/// Default gap color for wordmarks on the deep background.
pub(crate) const WORDMARK_GAP: ratatui::style::Color = THEME_BG_DEEP;
