//! NNS buy screen: name field + search, giant Cancel / Register buttons.

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use super::block_wordmark::{render_block_wordmark, NNS, WORDMARK_GAP};
use super::button::{Button, ButtonState, CANCEL_THEME, REGISTER_THEME};
use super::loading::loading_indicator_paragraph;
use super::theme::{THEME_ACCENT_GREEN, THEME_BG_DEEP, THEME_MUTED};
use crate::tui::app_state::AppState;
use crate::tui::nns;
use crate::tui::screens::{NnsBuyFocus, Screen};

pub(crate) fn draw_nns_buy(
    f: &mut Frame<'_>,
    app: &AppState,
    screen: &Screen,
    area: Rect,
    tick: u64,
) {
    let Screen::NnsBuy {
        value,
        cursor,
        focus,
        status,
        lookup_busy,
        verified_name,
    } = screen
    else {
        return;
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(THEME_ACCENT_GREEN))
        .title(" .nock name ")
        .style(Style::default().bg(THEME_BG_DEEP));
    let inner = block.inner(area);
    f.render_widget(block, area);

    if *lookup_busy {
        let tick = app.ui_fx.frame_clock;
        let label = if verified_name.is_some() {
            "Registering name"
        } else {
            "Searching name"
        };
        let body = loading_indicator_paragraph(
            app,
            tick,
            Block::default().borders(Borders::NONE),
            label,
        );
        f.render_widget(body, inner);
        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(2),
            Constraint::Length(5),
        ])
        .split(inner);

    render_block_wordmark(f, layout[0], &NNS, tick as usize, WORDMARK_GAP);

    draw_name_row(f, layout[2], value, *cursor, *focus);
    draw_status(
        f,
        layout[3],
        app,
        value,
        status.as_deref(),
        verified_name.is_some(),
    );

    draw_action_buttons(f, layout[4], *focus);
}

fn draw_name_row(f: &mut Frame<'_>, area: Rect, value: &str, cursor: usize, focus: NnsBuyFocus) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(2), Constraint::Length(14)])
        .split(area);

    let name_focused = focus == NnsBuyFocus::Name;
    let border = if name_focused {
        Style::default().fg(THEME_ACCENT_GREEN)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let text_style = if name_focused {
        Style::default().fg(Color::White)
    } else if value.is_empty() {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default().fg(THEME_MUTED)
    };

    let name_block = Block::default()
        .borders(Borders::ALL)
        .border_style(border)
        .title(if value.is_empty() {
            " Name (e.g. logan.nock) "
        } else {
            " Name "
        })
        .style(Style::default().bg(THEME_BG_DEEP));
    let name_inner = name_block.inner(cols[0]);
    f.render_widget(name_block, cols[0]);
    f.render_widget(
        Paragraph::new(value).style(text_style).alignment(Alignment::Left),
        name_inner,
    );

    if name_focused {
        let col = name_inner.x.saturating_add(line_width(&value.chars().take(cursor).collect::<String>()));
        f.set_cursor_position(ratatui::layout::Position::new(col, name_inner.y));
    }

    let search_state = match focus {
        NnsBuyFocus::Search => ButtonState::Selected,
        _ => ButtonState::Normal,
    };
    f.render_widget(
        Button::new(Line::from(Span::styled(
            "Search",
            Style::default()
                .fg(CANCEL_THEME.text)
                .add_modifier(Modifier::BOLD),
        )))
        .theme(CANCEL_THEME)
        .state(search_state),
        cols[2],
    );
}

fn draw_status(
    f: &mut Frame<'_>,
    area: Rect,
    app: &AppState,
    name_input: &str,
    status: Option<&str>,
    verified: bool,
) {
    let usd = app.price.usd_per_coin;
    let (text, color) = match status {
        Some(s) if s.starts_with("Error") => (s.to_string(), Color::Red),
        Some(s) => (
            s.to_string(),
            if verified {
                THEME_ACCENT_GREEN
            } else {
                THEME_MUTED
            },
        ),
        None if verified => (
            "Name verified — press Register".to_string(),
            THEME_ACCENT_GREEN,
        ),
        None => {
            let mut hint = String::from("Enter a name, then Search to check availability");
            if let Some(usd) = usd.filter(|u| u.is_finite() && *u > 0.0) {
                hint.push_str(&format!("  ·  @ ${usd:.4} / NOCK"));
            }
            if let Some(est) = nns::estimated_fee_hint(name_input, usd) {
                hint.push_str("\n");
                hint.push_str(&est);
            }
            (hint, THEME_MUTED)
        }
    };
    f.render_widget(
        Paragraph::new(text)
            .style(Style::default().fg(color))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn draw_action_buttons(f: &mut Frame<'_>, area: Rect, focus: NnsBuyFocus) {
    let btn_cols: Vec<Rect> = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(2), Constraint::Fill(1)])
        .split(area)
        .to_vec();

    let cancel_state = match focus {
        NnsBuyFocus::Cancel => ButtonState::Selected,
        _ => ButtonState::Normal,
    };
    let register_state = match focus {
        NnsBuyFocus::Register => ButtonState::Selected,
        _ => ButtonState::Normal,
    };

    f.render_widget(
        Button::new(Line::from(Span::styled(
            "Cancel",
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
            "Register",
            Style::default()
                .fg(REGISTER_THEME.text)
                .add_modifier(Modifier::BOLD),
        )))
        .theme(REGISTER_THEME)
        .state(register_state),
        btn_cols[2],
    );
}

fn line_width(s: &str) -> u16 {
    u16::try_from(Line::from(Span::raw(s)).width()).unwrap_or(0)
}
