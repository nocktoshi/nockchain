//! Splash-style scanlines, twinkle stars, and pulsing backgrounds for the home shell.

use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use super::theme::{THEME_ACCENT_GREEN, THEME_BG_DEEP};

/// Home activity-field scanlines (behind wallet tab).
const HOME_AMBIENT_FG: ratatui::style::Color = ratatui::style::Color::Rgb(18, 42, 17);

pub(crate) fn blink_glyph(fc: usize, salt: usize) -> &'static str {
    const GLYPHS: &[&str] = &["·", "✧", "·", "⋆"];
    GLYPHS[(fc / 2 + salt) % GLYPHS.len()]
}

/// Paint CRT scanlines for every row in `zone`.
pub(crate) fn render_scan_zone(f: &mut Frame<'_>, zone: Rect, fc: usize) {
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
    use super::theme::THEME_ACCENT_GREEN as ACCENT;
    let (fg, bg) = match global_y % 5 {
        0 => (ratatui::style::Color::Rgb(78, 235, 74), THEME_BG_DEEP),
        1 => (ACCENT, THEME_BG_DEEP),
        2 => (ratatui::style::Color::Rgb(52, 185, 48), THEME_BG_DEEP),
        3 => (ratatui::style::Color::Rgb(38, 135, 36), THEME_BG_DEEP),
        _ => (ratatui::style::Color::Rgb(28, 105, 26), THEME_BG_DEEP),
    };
    Style::new().fg(fg).bg(bg).add_modifier(Modifier::BOLD)
}

fn scanline_row_string(w: usize, fc: usize, global_y: usize, start_x: u16) -> String {
    let period = 160usize.max(w.saturating_add(start_x as usize));
    let speed = 10usize;
    let b0 = fc
        .wrapping_mul(speed)
        .wrapping_add(global_y.wrapping_mul(13))
        % period;
    let b1 = fc
        .wrapping_mul(6)
        .wrapping_add(73)
        .wrapping_add(global_y.wrapping_mul(5))
        % period;
    let b2 = fc.wrapping_mul(14).wrapping_add(global_y.wrapping_mul(17))
        % period.saturating_mul(2).max(8);

    (0..w)
        .map(|i| {
            let gx = start_x as usize + i;
            let d0 = gx.abs_diff(b0);
            let d1 = gx.abs_diff(b1);
            let d2 = gx.abs_diff(b2 % period);
            let glow = d0.min(d1).min(d2);
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

/// Ambient scanfield behind home activity panel — sparse so menus stay readable.
///
/// `mask` — screen rect to leave blank (e.g. the green balance button) so scanlines do not cross it.
pub(crate) fn render_home_ambient(f: &mut Frame<'_>, area: Rect, fc: usize, mask: Option<Rect>) {
    if area.height < 8 || area.width == 0 {
        return;
    }
    let mut lines: Vec<Line> = Vec::with_capacity(area.height as usize);
    for dy in 0..area.height {
        let row_y = area.y + dy;
        let gy = row_y as usize;
        let row_s = scanline_row_string_subtle(area.width as usize, fc, gy, area.x, row_y, mask);
        lines.push(Line::from(Span::styled(
            row_s,
            Style::new().fg(HOME_AMBIENT_FG).bg(THEME_BG_DEEP),
        )));
    }
    f.render_widget(Paragraph::new(lines), area);
}

/// Home-only scanlines: mostly empty, occasional dim dots (no bright beams).
fn scanline_row_string_subtle(
    w: usize,
    fc: usize,
    global_y: usize,
    start_x: u16,
    row_y: u16,
    mask: Option<Rect>,
) -> String {
    let period = 200usize.max(w.saturating_add(start_x as usize));
    let b0 = fc.wrapping_mul(4).wrapping_add(global_y.wrapping_mul(11)) % period;

    (0..w)
        .map(|i| {
            let gx = start_x as usize + i;
            if cell_masked(gx as u16, row_y, mask) {
                return ' ';
            }
            let d0 = gx.abs_diff(b0);
            if d0 < 4 {
                '·'
            } else if (gx.wrapping_add(global_y).wrapping_add(fc)) % 23 == 0 {
                '˙'
            } else {
                ' '
            }
        })
        .collect()
}

fn cell_masked(x: u16, y: u16, mask: Option<Rect>) -> bool {
    let Some(m) = mask else {
        return false;
    };
    x >= m.x && x < m.x + m.width && y >= m.y && y < m.y + m.height
}

/// Header stars flanking brand text.
pub(crate) fn header_star_line(fc: usize) -> Line<'static> {
    Line::from(vec![
        Span::styled(blink_glyph(fc, 0), Style::default().fg(THEME_ACCENT_GREEN)),
        Span::raw("  "),
        Span::styled(blink_glyph(fc, 2), Style::default().fg(THEME_ACCENT_GREEN)),
        Span::raw("  "),
        Span::styled(blink_glyph(fc, 1), Style::default().fg(THEME_ACCENT_GREEN)),
    ])
}
