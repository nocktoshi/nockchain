//! Receive screen: address in the main panel + giant copy button (extension-style).

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph, Wrap};
use ratatui::Frame;

use super::button::{Button, ButtonState, CONTINUE_THEME};
use super::home::large_label;
use super::loading::loading_indicator_paragraph;
use super::theme::{THEME_ACCENT_GREEN, THEME_BG_DEEP, THEME_BG_PANEL, THEME_MUTED};
use crate::tui::app_state::AppState;
use crate::tui::screens::Screen;

pub(crate) fn draw_receive(
    f: &mut Frame<'_>,
    app: &AppState,
    screen: &Screen,
    area: Rect,
    tick: u64,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(THEME_ACCENT_GREEN))
        .title(" Receive NOCK ")
        .style(Style::default().bg(THEME_BG_DEEP));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let Screen::Receive {
        address,
        loading,
        error,
        copy_focused,
    } = screen
    else {
        return;
    };

    if *loading {
        let body = loading_indicator_paragraph(app, tick, Block::default(), "Loading address");
        f.render_widget(body, inner);
        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(5),
            Constraint::Min(4),
        ])
        .split(inner);

    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            large_label("Your address"),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )))
        .alignment(Alignment::Center),
        layout[0],
    );

    draw_address_card(f, layout[1], address.as_deref(), error.as_deref());

    if address.is_some() && error.is_none() {
        let copy_state = if *copy_focused {
            ButtonState::Selected
        } else {
            ButtonState::Normal
        };
        f.render_widget(
            Button::new(Line::from(vec![
                Span::styled("⧉ ", Style::default().fg(CONTINUE_THEME.text)),
                Span::styled(
                    "Copy address",
                    Style::default()
                        .fg(CONTINUE_THEME.text)
                        .add_modifier(Modifier::BOLD),
                ),
            ]))
            .theme(CONTINUE_THEME)
            .state(copy_state),
            layout[2],
        );
    }

    draw_instructions(f, layout[3]);
}

fn draw_address_card(f: &mut Frame<'_>, area: Rect, address: Option<&str>, error: Option<&str>) {
    let card = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray))
        .style(Style::default().bg(THEME_BG_PANEL));
    let inner = card.inner(area);
    f.render_widget(card, area);

    if let Some(err) = error {
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                err,
                Style::default().fg(Color::Red),
            )))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }

    let Some(addr) = address else {
        f.render_widget(
            Paragraph::new("No active address found")
                .style(Style::default().fg(THEME_MUTED))
                .alignment(Alignment::Center),
            inner,
        );
        return;
    };

    let lines = address_lines(addr, inner.width.max(20) as usize);
    let styled: Vec<Line> = lines
        .into_iter()
        .map(|line| {
            Line::from(Span::styled(
                line,
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ))
        })
        .collect();
    f.render_widget(
        Paragraph::new(styled).alignment(Alignment::Center),
        inner,
    );
}

fn draw_instructions(f: &mut Frame<'_>, area: Rect) {
    let card = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" How to receive NOCK ")
        .style(Style::default().bg(THEME_BG_DEEP));
    let inner = card.inner(area);
    f.render_widget(card, area);
    let lines = vec![
        Line::from("• Share this address with the sender"),
        Line::from("• Transactions will appear in your wallet"),
    ];
    f.render_widget(
        Paragraph::new(lines)
            .style(Style::default().fg(THEME_MUTED))
            .wrap(Wrap { trim: true }),
        inner,
    );
}

/// Split a long base58 address across lines (extension-style).
fn address_lines(addr: &str, max_width: usize) -> Vec<String> {
    let width = max_width.max(24);
    if addr.len() <= width {
        return vec![addr.to_string()];
    }
    let split = addr.len() / 2;
    vec![addr[..split].to_string(), addr[split..].to_string()]
}
