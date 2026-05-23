//! Simple “Send NOCK” form UI (home CTA).

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use super::button::{Button, ButtonState, CANCEL_THEME, CONTINUE_THEME};
use super::home::large_label;
use super::loading::loading_indicator_paragraph;
use super::theme::{THEME_ACCENT_CTA, THEME_ACCENT_GREEN, THEME_BG_DEEP, THEME_MUTED};
use crate::tui::app_state::AppState;
use crate::tui::screens::{Screen, SendSimpleFocus, SendSimplePhase};
use crate::tui::send_simple::spendable_balance_line;

pub(crate) fn draw_send_simple(f: &mut Frame<'_>, app: &AppState, area: Rect) {
    let Screen::SendSimple {
        amount,
        recipient,
        focus,
        phase,
        status,
        review_scroll,
        ..
    } = &app.screen
    else {
        return;
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(THEME_ACCENT_GREEN))
        .title(match phase {
            SendSimplePhase::Review { .. } => " Confirm send ",
            SendSimplePhase::Planning => " Planning… ",
            SendSimplePhase::Form => " Send NOCK ",
        })
        .style(Style::default().bg(THEME_BG_DEEP));
    let inner = block.inner(area);
    f.render_widget(block, area);

    if matches!(phase, SendSimplePhase::Planning) {
        let tick = app.ui_fx.frame_clock;
        let planning_block = Block::default().borders(Borders::NONE);
        let body = loading_indicator_paragraph(app, tick, planning_block, "Planning transaction");
        f.render_widget(body, inner);
        return;
    }

    if let SendSimplePhase::Review { preview, .. } = phase {
        draw_review(f, inner, preview, *review_scroll, *focus);
        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(5),
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(5),
        ])
        .split(inner);

    let balance = spendable_balance_line(&app.balance_panel.events);
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            balance,
            Style::default().fg(THEME_MUTED),
        )))
        .alignment(Alignment::Center),
        layout[0],
    );

    let amount_area = layout[1];
    let recipient_area = layout[2];
    let amount_focused = *focus == SendSimpleFocus::Amount;
    let recipient_focused = *focus == SendSimpleFocus::Recipient;
    let amount_inner = draw_amount_field(f, amount_area, amount, amount_focused);
    let recipient_inner = draw_recipient_field(f, recipient_area, recipient, recipient_focused);

    if let Screen::SendSimple {
        amount_cursor,
        recipient_cursor,
        ..
    } = &app.screen
    {
        match focus {
            SendSimpleFocus::Amount => {
                let col = amount_cursor_x(amount_inner, amount, *amount_cursor, amount_focused);
                f.set_cursor_position(ratatui::layout::Position::new(col, amount_inner.y));
            }
            SendSimpleFocus::Recipient => {
                let col = text_cursor_x(recipient_inner, recipient, *recipient_cursor);
                f.set_cursor_position(ratatui::layout::Position::new(col, recipient_inner.y));
            }
            _ => {}
        }
    }

    if let Some(msg) = status {
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                msg.clone(),
                Style::default().fg(Color::Red),
            )))
            .alignment(Alignment::Center),
            layout[3],
        );
    }

    draw_action_buttons(f, layout[4], *focus, "Cancel", "Continue");
}

fn draw_review(f: &mut Frame<'_>, area: Rect, preview: &str, scroll_y: u16, focus: SendSimpleFocus) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(5)])
        .split(area);

    let review_block = Block::default()
        .borders(Borders::ALL)
        .title(" Transaction ")
        .style(Style::default().bg(THEME_BG_DEEP));
    let para = Paragraph::new(preview)
        .wrap(Wrap { trim: true })
        .block(review_block)
        .scroll((scroll_y, 0));
    f.render_widget(para, layout[0]);

    draw_action_buttons(
        f,
        layout[1],
        focus,
        "Back",
        "Send",
    );
}

fn draw_action_buttons(
    f: &mut Frame<'_>,
    area: Rect,
    focus: SendSimpleFocus,
    cancel_label: &str,
    continue_label: &str,
) {
    let btn_cols: Vec<Rect> = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(2), Constraint::Fill(1)])
        .split(area)
        .to_vec();

    let cancel_state = match focus {
        SendSimpleFocus::Cancel => ButtonState::Selected,
        _ => ButtonState::Normal,
    };
    let continue_state = match focus {
        SendSimpleFocus::Continue => ButtonState::Selected,
        _ => ButtonState::Normal,
    };

    f.render_widget(
        Button::new(Line::from(Span::styled(
            cancel_label,
            Style::default()
                .fg(CANCEL_THEME.text)
                .add_modifier(Modifier::BOLD),
        )))
        .theme(CANCEL_THEME)
        .state(cancel_state),
        btn_cols[0],
    );
    f.render_widget(
        Button::new(Line::from(Span::styled(
            continue_label,
            Style::default()
                .fg(CONTINUE_THEME.text)
                .add_modifier(Modifier::BOLD),
        )))
        .theme(CONTINUE_THEME)
        .state(continue_state),
        btn_cols[2],
    );
}

fn draw_amount_field(f: &mut Frame<'_>, area: Rect, amount: &str, focused: bool) -> Rect {
    let (display, align) = if focused {
        (amount.to_string(), Alignment::Left)
    } else if amount.is_empty() {
        ("0".to_string(), Alignment::Center)
    } else {
        (amount.to_string(), Alignment::Center)
    };
    let big = large_label(&display);
    let border = if focused {
        Style::default().fg(THEME_ACCENT_CTA)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border)
        .title(" Amount (NOCK) ")
        .style(Style::default().bg(THEME_BG_DEEP));
    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            big,
            Style::default()
                .fg(if focused {
                    Color::White
                } else {
                    THEME_MUTED
                })
                .add_modifier(Modifier::BOLD),
        )))
        .alignment(align),
        inner,
    );
    inner
}

fn draw_recipient_field(f: &mut Frame<'_>, area: Rect, recipient: &str, focused: bool) -> Rect {
    let border = if focused {
        Style::default().fg(THEME_ACCENT_GREEN)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let text = if recipient.is_empty() {
        "Enter Nockchain address".to_string()
    } else {
        recipient.to_string()
    };
    let style = if recipient.is_empty() {
        Style::default().fg(Color::DarkGray)
    } else if focused {
        Style::default().fg(Color::White)
    } else {
        Style::default().fg(THEME_MUTED)
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border)
        .title(" Receiver address ")
        .style(Style::default().bg(THEME_BG_DEEP));
    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(
        Paragraph::new(text).style(style).alignment(Alignment::Left),
        inner,
    );
    inner
}

fn amount_cursor_x(inner: Rect, amount: &str, char_index: usize, focused: bool) -> u16 {
    let prefix: String = amount.chars().take(char_index).collect();
    let prefix_w = line_width(&large_label(&prefix));
    if focused {
        return inner.x.saturating_add(prefix_w);
    }
    let full_w = line_width(&large_label(amount));
    let start = inner.x + (inner.width.saturating_sub(full_w)) / 2;
    start.saturating_add(prefix_w)
}

fn text_cursor_x(inner: Rect, text: &str, char_index: usize) -> u16 {
    let prefix: String = text.chars().take(char_index).collect();
    inner.x.saturating_add(line_width(&prefix))
}

fn line_width(s: &str) -> u16 {
    u16::try_from(Line::from(Span::raw(s)).width()).unwrap_or(0)
}
