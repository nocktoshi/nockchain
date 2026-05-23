//! Balance panel beside the main menu on the home screen.

use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph, Wrap};
use ratatui::Frame;

use super::loading::loading_indicator_paragraph;
use super::scroll::estimate_wrapped_source_lines;
use super::theme::THEME_ACCENT_GREEN;
use crate::repl::app_state::{AppState, PanelFocus};

pub(crate) fn draw_balance_sidebar(
    f: &mut Frame<'_>,
    app: &mut AppState,
    area: ratatui::layout::Rect,
    tick: u64,
) {
    let focused = matches!(app.panel_focus, PanelFocus::Balance);
    let mut balance_block = Block::default().borders(Borders::ALL);
    if focused {
        balance_block = balance_block
            .border_type(BorderType::Thick)
            .border_style(Style::default().fg(THEME_ACCENT_GREEN));
    }
    let balance_block = balance_block.title(Line::from(vec![Span::styled(
        if focused {
            " Balance ◆ "
        } else {
            " Balance "
        },
        Style::default().fg(Color::Cyan),
    )]));

    if app.balance_panel.loading {
        let body = loading_indicator_paragraph(app, tick, balance_block, "Refreshing balance…");
        f.render_widget(body, area);
        return;
    }

    let inner = balance_block.inner(area);
    let display = if let Some(ref e) = app.balance_panel.error {
        if app.balance_panel.text.trim().is_empty() {
            format!("Error\n\n{e}")
        } else {
            format!("{}\n\n--- error ---\n{e}", app.balance_panel.text)
        }
    } else if app.balance_panel.text.is_empty() {
        "<<< balance not loaded >>>".to_string()
    } else {
        app.balance_panel.text.clone()
    };

    let inner_w = inner.width.max(1);
    let base = estimate_wrapped_source_lines(&display, inner_w);
    let measure = base.saturating_add(base / 4).saturating_add(12);
    let visible = inner.height as usize;
    let max_scroll = measure.saturating_sub(visible);
    let max_u16 = u16::try_from(max_scroll).unwrap_or(u16::MAX);
    app.balance_panel.scroll = app.balance_panel.scroll.min(max_u16);
    let scroll_y = app.balance_panel.scroll;

    let para = Paragraph::new(display)
        .wrap(Wrap { trim: true })
        .block(balance_block)
        .scroll((scroll_y, 0));
    f.render_widget(para, area);
}
