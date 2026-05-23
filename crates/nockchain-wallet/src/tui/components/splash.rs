//! Boot splash: animated border, scanline, and block “NOCKCHAIN” wordmark (dark palette + green accents).

use ratatui::layout::{Alignment, Constraint, Direction, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Padding, Paragraph};
use ratatui::Frame;

use super::ambient::{blink_glyph, render_scan_zone};
use super::block_wordmark::{render_block_wordmark, NOCKCHAIN};
use super::theme::{
    pulse_border_green, THEME_ACCENT_GREEN as ACCENT, THEME_BG_DEEP, THEME_BG_PANEL, THEME_SHADOW,
};

const LOGO_W: u16 = 53;

pub(crate) fn draw_splash(f: &mut Frame<'_>, _tick: u64) {
    let area = f.area();
    let fc = f.count();
    let border_fg = pulse_border_green(fc);

    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::new().fg(border_fg))
        .title_alignment(Alignment::Center)
        .title(Line::from(vec![Span::styled(
            " nockchain-wallet ",
            Style::new().fg(ACCENT).add_modifier(Modifier::BOLD),
        )]))
        .style(Style::new().bg(THEME_BG_DEEP));
    let inner = outer.inner(area);
    f.render_widget(outer, area);

    let v = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(22), Constraint::Length(12), Constraint::Min(2)])
        .split(inner);

    let mid = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Fill(1), Constraint::Length(LOGO_W + 6), Constraint::Fill(1)])
        .flex(Flex::Center)
        .split(v[1]);

    let bottom_split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(v[2]);

    let card_rect = mid[1];
    render_scan_zone(f, v[0], fc);
    render_scan_zone(f, mid[0], fc);
    render_scan_zone(f, mid[2], fc);
    render_scan_zone(f, bottom_split[0], fc);

    render_card_shadow(f, card_rect);

    let card = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Thick)
        .border_style(Style::new().fg(Color::Rgb(255, 255, 255)))
        .style(Style::new().bg(THEME_BG_PANEL))
        .padding(Padding::symmetric(2, 1));
    let card_inner = card.inner(card_rect);
    f.render_widget(card, card_rect);

    render_block_wordmark(f, card_inner, &NOCKCHAIN, fc, THEME_BG_PANEL);

    let tag = format!(
        " {} Programmable Gold {}",
        blink_glyph(fc, 0),
        blink_glyph(fc, 2),
    );
    let tag_area = Rect {
        y: card_inner.y + card_inner.height.saturating_sub(2),
        height: 1,
        ..card_inner
    };
    if tag_area.height > 0 && card_inner.height > 5 {
        let tag_line = Paragraph::new(Line::from(vec![
            Span::styled(
                "│",
                Style::new()
                    .fg(Color::Rgb(26, 95, 24))
                    .bg(THEME_BG_PANEL)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                tag,
                Style::new()
                    .fg(ACCENT)
                    .bg(THEME_BG_PANEL)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "│",
                Style::new()
                    .fg(Color::Rgb(26, 95, 24))
                    .bg(THEME_BG_PANEL)
                    .add_modifier(Modifier::BOLD),
            ),
        ]))
        .alignment(Alignment::Center);
        f.render_widget(tag_line, tag_area);
    }

    let hint = Paragraph::new(Line::from(vec![
        Span::styled(blink_glyph(fc, 1), Style::new().fg(ACCENT)),
        Span::styled(
            "  press any key to start  ",
            Style::new().fg(Color::Rgb(140, 140, 140)),
        ),
        Span::styled(blink_glyph(fc, 3), Style::new().fg(ACCENT)),
    ]))
    .alignment(Alignment::Center);
    f.render_widget(hint, bottom_split[1]);
}

fn render_card_shadow(f: &mut Frame<'_>, card: Rect) {
    const T: u16 = 3;
    const TL: u16 = 1;
    if card.width == 0 || card.height == 0 {
        return;
    }
    let right = Rect::new(card.x.saturating_add(card.width), card.y, T, card.height);
    let (bottom_x, bottom_w) = if card.x >= TL {
        (card.x - TL, TL.saturating_add(card.width).saturating_add(T))
    } else {
        (card.x, card.width.saturating_add(T))
    };
    let bottom = Rect::new(bottom_x, card.y.saturating_add(card.height), bottom_w, T);
    let (top_x, top_w) = if card.x >= TL {
        (card.x - TL, TL.saturating_add(card.width).saturating_add(T))
    } else {
        (card.x, card.width.saturating_add(T))
    };
    let top = Rect::new(top_x, card.y.saturating_sub(TL), top_w, TL);

    let shadow_fill = Style::new().fg(THEME_SHADOW).bg(THEME_SHADOW);
    let patch = Block::default().borders(Borders::NONE).style(shadow_fill);

    if card.x >= TL {
        let left = Rect::new(card.x - TL, card.y, TL, card.height);
        f.render_widget(patch.clone(), left);
    }
    f.render_widget(patch.clone(), right);
    f.render_widget(patch.clone(), bottom);
    if card.y >= TL {
        f.render_widget(patch, top);
    }
}
