//! Home wallet tab: balance button (NOCK + USD), Send / Receive / Buy .nock CTAs.

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use super::button::{balance_gradient_bg_for_widget, paint_balance_button_gradient, BALANCE_THEME};
use super::loading::{braille_spinner_char, splash_brand_loading_line, sync_attempt_message};
use super::price::format_usd_total;
use super::theme::{THEME_ACCENT_CTA, THEME_ACCENT_GREEN, THEME_BG_DEEP};
use crate::repl::app_state::AppState;
use crate::repl::format::format_nock_balance_display;
use crate::repl::view::total_assets_nicks;

/// Secondary copy on the home wallet tab (readable on deep background).
const HOME_HINT_TEXT: Color = Color::Rgb(210, 210, 210);
/// USD line on the balance button (bright on dark green).
const BALANCE_USD_TEXT: Color = Color::Rgb(235, 255, 235);

const BALANCE_TOP_MARGIN: u16 = 2;
const BALANCE_IDENTITY_ROWS: u16 = 2;
const BALANCE_IDENTITY_PAD: u16 = 4;
const BALANCE_BODY_ROWS: u16 = 5;
const BALANCE_BUTTON_HEIGHT: u16 = BALANCE_IDENTITY_ROWS + BALANCE_IDENTITY_PAD + BALANCE_BODY_ROWS + 2;
/// Cap width so the hero does not stretch edge-to-edge on wide terminals.
const BALANCE_BUTTON_MAX_WIDTH: u16 = 52;
/// Muted address line under a resolved `.nock` name.
const BALANCE_ADDRESS_SUBTEXT: Color = Color::Rgb(180, 220, 180);

const CTAS: &[(&str, char)] = &[
    ("Send", 's'),
    ("Receive", 'r'),
    (".nock Name", 'n'),
];

/// Mathematical sans-serif bold — reads much larger than normal terminal text.
pub(crate) fn large_label(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            'A'..='Z' => char::from_u32(0x1D5D4 + (c as u32 - 'A' as u32)).unwrap_or(c),
            'a'..='z' => char::from_u32(0x1D5EE + (c as u32 - 'a' as u32)).unwrap_or(c),
            '0'..='9' => char::from_u32(0x1D7EC + (c as u32 - '0' as u32)).unwrap_or(c),
            _ => c,
        })
        .collect()
}

/// Balance button rect in activity-panel coordinates (wallet tab, tab 0).
pub(crate) fn balance_button_abs_rect(activity_area: Rect) -> Option<Rect> {
    if activity_area.height < 8 || activity_area.width == 0 {
        return None;
    }
    let shell = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(3),
        ])
        .split(activity_area);
    let content = shell[2];
    let wallet = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(BALANCE_TOP_MARGIN + BALANCE_BUTTON_HEIGHT),
            Constraint::Length(5),
            Constraint::Length(1),
        ])
        .split(content);
    let balance_section = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(BALANCE_TOP_MARGIN),
            Constraint::Length(BALANCE_BUTTON_HEIGHT),
        ])
        .split(wallet[0]);
    Some(balance_button_rect(balance_section[1]))
}

pub(crate) fn draw_wallet_tab(f: &mut Frame<'_>, app: &AppState, area: Rect, tick: u64) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(BALANCE_TOP_MARGIN + BALANCE_BUTTON_HEIGHT),
            Constraint::Length(5),
            Constraint::Length(1),
        ])
        .split(area);

    let balance_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(BALANCE_TOP_MARGIN),
            Constraint::Length(BALANCE_BUTTON_HEIGHT),
        ])
        .split(chunks[0]);
    let balance_area = balance_button_rect(balance_chunks[1]);

    if app.balance_panel.loading {
        draw_balance_loading(f, app, balance_area, tick);
    } else {
        draw_balance_button(f, app, balance_area);
    }
    draw_cta_row(f, chunks[1]);
    draw_cta_hints(f, chunks[2]);
}

fn draw_balance_loading(f: &mut Frame<'_>, app: &AppState, area: Rect, tick: u64) {
    paint_balance_button_gradient(f.buffer_mut(), area);
    let inner = balance_button_inner(area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(BALANCE_IDENTITY_ROWS),
            Constraint::Length(BALANCE_IDENTITY_PAD),
            Constraint::Min(BALANCE_BODY_ROWS),
        ])
        .split(inner);

    draw_balance_identity(f, app, chunks[0], area);

    let bold_white = Style::default()
        .fg(BALANCE_THEME.text)
        .add_modifier(Modifier::BOLD);
    let spin = braille_spinner_char(tick);
    let mut lines = vec![
        Line::from(Span::styled("Balance", bold_white)),
        splash_brand_loading_line(tick),
        Line::from(vec![
            Span::styled(spin, Style::default().fg(BALANCE_THEME.highlight)),
            Span::styled(
                " Getting balance…",
                Style::default()
                    .fg(BALANCE_USD_TEXT)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];
    if let Some(sync) = sync_attempt_message(app) {
        lines.push(Line::from(Span::styled(
            sync,
            Style::default().fg(Color::Yellow),
        )));
    }
    draw_balance_body(f, chunks[2], lines, area);
}

/// Centered balance button rect, capped at [`BALANCE_BUTTON_MAX_WIDTH`].
fn balance_button_rect(area: Rect) -> Rect {
    let w = area.width.min(BALANCE_BUTTON_MAX_WIDTH);
    let x = area.x + area.width.saturating_sub(w) / 2;
    Rect::new(x, area.y, w, area.height)
}

/// Inner content rect below the top chrome line.
fn balance_button_inner(area: Rect) -> Rect {
    Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(2),
        ..area
    }
}

fn truncate_display(s: &str, max_cols: usize) -> String {
    if max_cols < 4 || s.chars().count() <= max_cols {
        return s.to_string();
    }
    let keep = (max_cols - 1) / 2;
    let chars: Vec<char> = s.chars().collect();
    let head: String = chars.iter().take(keep).collect();
    let tail: String = chars.iter().skip(chars.len().saturating_sub(keep)).collect();
    format!("{head}…{tail}")
}

fn draw_balance_identity(f: &mut Frame<'_>, app: &AppState, area: Rect, button: Rect) {
    let row_bg = balance_gradient_bg_for_widget(area, button);
    let bold = Style::default()
        .fg(BALANCE_THEME.text)
        .add_modifier(Modifier::BOLD);
    let sub = Style::default().fg(BALANCE_ADDRESS_SUBTEXT);

    let (primary, secondary) = if app.balance_panel.identity_loading {
        ("…".to_string(), None)
    } else if let Some(ref name) = app.balance_panel.nockname {
        let addr = app
            .balance_panel
            .address
            .as_deref()
            .map(|a| truncate_display(a, 15));
        (name.clone(), addr)
    } else if let Some(ref addr) = app.balance_panel.address {
        (truncate_display(addr, 15), None)
    } else {
        ("--- No address ---".to_string(), None)
    };

    let lines = if let Some(addr) = secondary {
        vec![
            Line::from(Span::styled(primary, bold)),
            Line::from(Span::styled(addr, sub)),
        ]
    } else {
        vec![Line::from(Span::styled(primary, bold))]
    };

    f.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .style(Style::default().bg(row_bg)),
        area,
    );
}

fn draw_balance_button(f: &mut Frame<'_>, app: &AppState, area: Rect) {
    paint_balance_button_gradient(f.buffer_mut(), area);
    let inner = balance_button_inner(area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(BALANCE_IDENTITY_ROWS),
            Constraint::Length(BALANCE_IDENTITY_PAD),
            Constraint::Min(BALANCE_BODY_ROWS),
        ])
        .split(inner);

    draw_balance_identity(f, app, chunks[0], area);

    let nicks = total_assets_nicks(&app.balance_panel.events);
    let nock_display = match nicks {
        Some(n) => format_nock_balance_display(n as u128),
        None if app.balance_panel.error.is_some() => "—".to_string(),
        None if app.balance_panel.text.is_empty() => "—".to_string(),
        None => "—".to_string(),
    };
    let usd_line = format_usd_line(app, nicks);

    let bold_white = Style::default()
        .fg(BALANCE_THEME.text)
        .add_modifier(Modifier::BOLD);

    let mut lines = vec![
        Line::from(Span::styled("Balance", bold_white)),
        Line::from(Span::styled(large_label(&nock_display), bold_white)),
        Line::from(Span::styled(
            usd_line,
            Style::default()
                .fg(BALANCE_USD_TEXT)
                .add_modifier(Modifier::BOLD),
        )),
    ];
    if let Some(ref err) = app.balance_panel.error {
        lines.push(Line::from(Span::styled(
            err.clone(),
            Style::default().fg(Color::Red),
        )));
    }
    draw_balance_body(f, chunks[2], lines, area);
}

fn draw_balance_body(f: &mut Frame<'_>, area: Rect, lines: Vec<Line<'_>>, button: Rect) {
    f.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .style(Style::default().bg(balance_gradient_bg_for_widget(area, button))),
        area,
    );
}

fn format_usd_line(app: &AppState, nicks: Option<u64>) -> String {
    if app.price.loading {
        return "USD …".to_string();
    }
    if let Some(ref err) = app.price.error {
        return format!("USD unavailable ({err})");
    }
    let Some(usd) = app.price.usd_per_coin else {
        return "USD —".to_string();
    };
    match nicks {
        Some(n) => format!("≈ {}", format_usd_total(n, usd)),
        None => format!("@ ${usd:.4} / NOCK"),
    }
}

fn draw_cta_row(f: &mut Frame<'_>, area: Rect) {
    let cols: Vec<Rect> = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Fill(1),
            Constraint::Fill(1),
            Constraint::Fill(1),
        ])
        .split(area)
        .to_vec();
    for (i, (label, key)) in CTAS.iter().enumerate() {
        let fg = if i == 0 {
            THEME_ACCENT_CTA
        } else {
            THEME_ACCENT_GREEN
        };
        let big = large_label(label);
        let lines = vec![
            Line::from(""),
            Line::from(vec![Span::styled(
                big,
                Style::default()
                    .fg(fg)
                    .bg(THEME_BG_DEEP)
                    .add_modifier(Modifier::BOLD),
            )]),
            Line::from(vec![Span::styled(
                format!("───"),
                Style::default().fg(fg).bg(THEME_BG_DEEP),
            )]),
            Line::from(vec![Span::styled(
                format!("[{key}]"),
                Style::default()
                    .fg(HOME_HINT_TEXT)
                    .bg(THEME_BG_DEEP)
                    .add_modifier(Modifier::BOLD),
            )]),
        ];
        let p = Paragraph::new(lines)
            .alignment(Alignment::Center)
            .style(Style::default().bg(THEME_BG_DEEP));
        f.render_widget(p, cols[i]);
    }
}

fn draw_cta_hints(f: &mut Frame<'_>, area: Rect) {
    let key_style = |fg: Color| Style::default().fg(fg).add_modifier(Modifier::BOLD);
    let label = Style::default().fg(HOME_HINT_TEXT);
    let hint = Line::from(vec![
        Span::styled("s", key_style(THEME_ACCENT_CTA)),
        Span::styled(" send  ", label),
        Span::styled("r", key_style(THEME_ACCENT_GREEN)),
        Span::styled(" receive  ", label),
        Span::styled("n", key_style(THEME_ACCENT_GREEN)),
        Span::styled(" .nock name", label),
    ]);
    f.render_widget(
        Paragraph::new(hint)
            .alignment(Alignment::Center)
            .style(Style::default().bg(THEME_BG_DEEP)),
        area,
    );
}

pub(crate) fn cta_key_to_index(c: char) -> Option<usize> {
    CTAS.iter().position(|(_, k)| *k == c)
}
