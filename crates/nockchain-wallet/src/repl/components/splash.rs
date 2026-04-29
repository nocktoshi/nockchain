//! Boot splash: animated border, scanline, and block “NOCKCHAIN” wordmark (dark palette + green accents).

use ratatui::layout::{Alignment, Constraint, Direction, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Padding, Paragraph, Wrap};
use ratatui::Frame;

use super::theme::{
    pulse_color, THEME_ACCENT_GREEN as ACCENT, THEME_BG_DEEP, THEME_BG_PANEL,
    THEME_SHADOW,
};

const LOGO_W: u16 = 53;

/// Five-line block “NOCKCHAIN” (fixed width 53; nine letters × 5 + eight gaps).
const NOCKCHAIN: [&str; 5] = [
    "█   █  ███   ███  █   █  ███  █   █  ███   ███  █   █",
    "██  █ █   █ █     █  █  █     █   █ █   █   █   ██  █",
    "█ █ █ █   █ █     ███   █     █████ █████   █   █ █ █",
    "█  ██ █   █ █     █  █  █     █   █ █   █   █   █  ██",
    "█   █  ███   ███  █   █  ███  █   █ █   █  ███  █   █",
];

pub(crate) fn draw_splash(f: &mut Frame<'_>, _tick: u64) {
    let area = f.area();
    let fc = f.count();

    let pulse = (fc % 48) < 24;
    let border_fg = if pulse {
        ACCENT
    } else {
        Color::Rgb(26, 115, 25)
    };

    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::new().fg(border_fg))
        .title_alignment(Alignment::Center)
        .title(Line::from(vec![Span::styled(
            " nockchain-wallet ",
            Style::new()
                .fg(ACCENT)
                .add_modifier(Modifier::BOLD),
        )]))
        .style(Style::new().bg(THEME_BG_DEEP));
    let inner = outer.inner(area);
    f.render_widget(outer, area);

    let v = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(22),
            Constraint::Length(12),
            Constraint::Min(2),
        ])
        .split(inner);

    let mid = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(LOGO_W + 6),
            Constraint::Fill(1),
        ])
        .flex(Flex::Center)
        .split(v[1]);

    let bottom_split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(v[2]);

    let card_rect = mid[1];
    // Full-screen scanfield behind UI, with a hole where the logo card sits.
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

    let logo_fg = pulse_color(fc);
    let mut card_lines: Vec<Line> = NOCKCHAIN
        .iter()
        .map(|row| {
            let spans: Vec<Span> = row
                .chars()
                .map(|ch| {
                    let fg = if ch == '█' { logo_fg } else { THEME_BG_PANEL };
                    Span::styled(
                        ch.to_string(),
                        Style::new()
                            .fg(fg)
                            .bg(THEME_BG_PANEL)
                            .add_modifier(Modifier::BOLD),
                    )
                })
                .collect();
            Line::from(spans)
        })
        .collect();

    card_lines.push(Line::from(""));

    let tag = format!(
        " {} Programmable Gold {}",
        blink_glyph(fc, 0),
        blink_glyph(fc, 2),
    );
    card_lines.push(Line::from(vec![
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
    ]));

    let card_body = Paragraph::new(card_lines)
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });
    f.render_widget(card_body, card_inner);

    let hint = Paragraph::new(Line::from(vec![
        Span::styled(
            blink_glyph(fc, 1),
            Style::new().fg(ACCENT),
        ),
        Span::styled(
            "  press any key to start  ",
            Style::new().fg(Color::Rgb(140, 140, 140)),
        ),
        Span::styled(
            blink_glyph(fc, 3),
            Style::new().fg(ACCENT),
        ),
    ]))
    .alignment(Alignment::Center);
    f.render_widget(hint, bottom_split[1]);
}

/// Drop shadow: **`TL`** top/left rim + thick **`T`** right/bottom. Top/bottom spans are widened so
/// they meet the side arms (no gaps at top-right or bottom-left).
fn render_card_shadow(f: &mut Frame<'_>, card: Rect) {
    const T: u16 = 3;
    const TL: u16 = 1;
    if card.width == 0 || card.height == 0 {
        return;
    }
    let right = Rect::new(
        card.x.saturating_add(card.width),
        card.y,
        T,
        card.height,
    );
    // Under left rim + card + under right arm — fixes bottom-left notch.
    let (bottom_x, bottom_w) = if card.x >= TL {
        (
            card.x - TL,
            TL.saturating_add(card.width).saturating_add(T),
        )
    } else {
        (card.x, card.width.saturating_add(T))
    };
    let bottom = Rect::new(
        bottom_x,
        card.y.saturating_add(card.height),
        bottom_w,
        T,
    );
    // Above left rim + card + above right arm — fixes top-right notch.
    let (top_x, top_w) = if card.x >= TL {
        (
            card.x - TL,
            TL.saturating_add(card.width).saturating_add(T),
        )
    } else {
        (card.x, card.width.saturating_add(T))
    };
    let top = Rect::new(top_x, card.y.saturating_sub(TL), top_w, TL);

    let shadow_fill = Style::new().fg(THEME_SHADOW).bg(THEME_SHADOW);
    let patch = Block::default()
        .borders(Borders::NONE)
        .style(shadow_fill);

    if card.x >= TL {
        let left = Rect::new(card.x - TL, card.y, TL, card.height);
        f.render_widget(patch.clone(), left);
    }
    f.render_widget(patch.clone(), right);
    f.render_widget(patch.clone(), bottom);
    if card.y >= TL {
        f.render_widget(patch.clone(), top);
    }
}


fn blink_glyph(fc: usize, salt: usize) -> &'static str {
    const GLYPHS: &[&str] = &["·", "✧", "·", "⋆"];
    GLYPHS[(fc / 2 + salt) % GLYPHS.len()]
}

/// Paint CRT scanlines for every row in `zone` (uses global x/y so beams stay continuous).
fn render_scan_zone(f: &mut Frame<'_>, zone: Rect, fc: usize) {
    if zone.width == 0 || zone.height == 0 {
        return;
    }
    let mut lines: Vec<Line> = Vec::with_capacity(zone.height as usize);
    for dy in 0..zone.height {
        let gy = (zone.y + dy) as usize;
        let row_s = scanline_row_string(zone.width as usize, fc, gy, zone.x);
        let style = scan_style_for_global_row(gy);
        lines.push(Line::from(Span::styled(row_s, style)));
    }
    f.render_widget(Paragraph::new(lines), zone);
}

fn scan_style_for_global_row(global_y: usize) -> Style {
    let (fg, bg) = match global_y % 5 {
        0 => (Color::Rgb(78, 235, 74), THEME_BG_DEEP),
        1 => (ACCENT, THEME_BG_DEEP),
        2 => (Color::Rgb(52, 185, 48), THEME_BG_DEEP),
        3 => (Color::Rgb(38, 135, 36), THEME_BG_DEEP),
        _ => (Color::Rgb(28, 105, 26), THEME_BG_DEEP),
    };
    Style::new().fg(fg).bg(bg).add_modifier(Modifier::BOLD)
}

/// Thick sweeping beams; `start_x` keeps phase aligned across split zones.
fn scanline_row_string(w: usize, fc: usize, global_y: usize, start_x: u16) -> String {
    let period = 160usize.max(w.saturating_add(start_x as usize));
    let speed = 10usize;
    let b0 = fc.wrapping_mul(speed).wrapping_add(global_y.wrapping_mul(13)) % period;
    let b1 = fc.wrapping_mul(6)
        .wrapping_add(73)
        .wrapping_add(global_y.wrapping_mul(5))
        % period;
    let b2 = fc.wrapping_mul(14).wrapping_add(global_y.wrapping_mul(17)) % period.saturating_mul(2).max(8);

    (0..w)
        .map(|i| {
            let gx = start_x as usize + i;
            let d0 = gx.abs_diff(b0);
            let d1 = gx.abs_diff(b1);
            let d2 = gx.abs_diff(b2 % period);
            let glow = d0.min(d1).min(d2);
            // Thicker core + tail (repeat-friendly across full background).
            if glow < 10 {
                '='
            } else if glow < 20 {
                '━'
            } else if glow < 30 {
                '─'
            } else if glow < 38 {
                '·'
            } else if (gx.wrapping_add(global_y).wrapping_add(fc)) % 9 == 0 {
                '˙'
            } else {
                ' '
            }
        })
        .collect()
}
