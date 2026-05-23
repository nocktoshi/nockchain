//! Giant CTA buttons (Ratatui custom widget pattern).

use ratatui::buffer::Buffer;
use ratatui::layout::{Position, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::Widget;

use super::theme::{THEME_ACCENT_CTA, THEME_ACCENT_GREEN, THEME_BG_PANEL};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ButtonState {
    Normal,
    Selected,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ButtonTheme {
    pub text: Color,
    pub background: Color,
    pub highlight: Color,
    pub shadow: Color,
}

pub(crate) const CANCEL_THEME: ButtonTheme = ButtonTheme {
    text: Color::Rgb(200, 200, 200),
    background: THEME_BG_PANEL,
    highlight: Color::Rgb(90, 90, 90),
    shadow: Color::Rgb(40, 40, 40),
};

pub(crate) const CONTINUE_THEME: ButtonTheme = ButtonTheme {
    text: Color::Rgb(16, 16, 16),
    background: THEME_ACCENT_CTA,
    highlight: Color::Rgb(255, 213, 60),
    shadow: Color::Rgb(180, 140, 0),
};

/// Primary CTA (NNS register, etc.) — forest green.
pub(crate) const REGISTER_THEME: ButtonTheme = ButtonTheme {
    text: Color::Rgb(240, 252, 240),
    background: THEME_ACCENT_GREEN,
    highlight: Color::Rgb(78, 235, 74),
    shadow: Color::Rgb(26, 115, 25),
};

/// Home balance hero — darker forest green, high-contrast copy.
pub(crate) const BALANCE_THEME: ButtonTheme = ButtonTheme {
    text: Color::White,
    background: Color::Rgb(18, 72, 18),
    highlight: Color::Rgb(42, 128, 40),
    shadow: Color::Rgb(10, 42, 10),
};

/// Top 12.5% of the button — modest lift above accent, nothing neon.
const BALANCE_GRADIENT_TOP: &[Color] = &[
    Color::Rgb(44, 148, 40),
    Color::Rgb(40, 144, 38),
    Color::Rgb(38, 142, 36),
    Color::Rgb(36, 141, 34),
    THEME_ACCENT_GREEN,
];

/// Dense shape keyframes for the lower band (~1 stop per green channel step).
const LOWER_GRADIENT_ANCHORS: &[(u8, u8, u8)] = &[
    (30, 125, 28),
    (30, 124, 28),
    (30, 123, 28),
    (30, 122, 28),
    (29, 121, 27),
    (29, 120, 27),
    (29, 119, 27),
    (29, 118, 27),
    (28, 117, 26),
    (28, 116, 26),
    (28, 115, 26),
    (28, 114, 26),
    (28, 113, 26),
    (28, 112, 26),
    (27, 111, 25),
    (27, 110, 25),
    (27, 109, 25),
    (27, 108, 25),
    (26, 107, 24),
    (26, 106, 24),
    (26, 105, 24),
    (26, 104, 24),
    (26, 103, 24),
    (26, 102, 24),
    (25, 101, 23),
    (25, 100, 23),
    (25, 99, 23),
    (25, 98, 23),
    (24, 97, 22),
    (24, 96, 22),
    (24, 95, 22),
    (24, 94, 22),
    (23, 93, 21),
    (23, 92, 21),
    (23, 91, 21),
    (23, 90, 21),
    (22, 89, 20),
    (22, 88, 20),
    (22, 87, 20),
    (22, 86, 20),
    (21, 85, 19),
    (21, 84, 19),
    (21, 83, 19),
    (21, 82, 19),
    (20, 81, 18),
    (20, 80, 18),
    (20, 79, 18),
    (20, 78, 18),
    (19, 77, 17),
    (19, 76, 17),
    (19, 75, 17),
    (19, 74, 17),
    (18, 73, 18),
    (18, 72, 18),
    (18, 71, 18),
    (18, 70, 17),
    (17, 69, 17),
    (17, 68, 16),
    (17, 67, 16),
    (17, 66, 16),
    (16, 65, 15),
    (16, 64, 15),
    (16, 63, 15),
    (16, 62, 15),
    (15, 61, 14),
    (15, 60, 14),
    (15, 59, 14),
    (15, 58, 14),
    (14, 57, 13),
    (14, 56, 13),
    (14, 55, 13),
    (14, 54, 13),
    (13, 53, 12),
    (13, 52, 12),
    (13, 51, 12),
    (13, 50, 12),
    (12, 49, 12),
    (12, 48, 12),
    (12, 47, 12),
    (12, 46, 12),
    (11, 45, 11),
    (11, 44, 11),
    (11, 43, 11),
    (10, 42, 10),
];

fn sample_rgb_anchors(anchors: &[(u8, u8, u8)], u: f32) -> Color {
    let n = anchors.len();
    if n <= 1 {
        let (r, g, b) = anchors[0];
        return Color::Rgb(r, g, b);
    }

    let u = u.clamp(0.0, 1.0);
    let pos = u * (n - 1) as f32;
    let i0 = pos.floor() as usize;
    let i1 = if i0 + 1 >= n { n - 1 } else { i0 + 1 };
    let t = pos - i0 as f32;
    let (r0, g0, b0) = anchors[i0];
    let (r1, g1, b1) = anchors[i1];
    Color::Rgb(
        lerp_channel(r0, r1, t),
        lerp_channel(g0, g1, t),
        lerp_channel(b0, b1, t),
    )
}

const TOP_HIGHLIGHT: f32 = 0.125;

fn lerp_channel(a: u8, b: u8, t: f32) -> u8 {
    (a as f32 + (b as f32 - a as f32) * t).round().clamp(0.0, 255.0) as u8
}

fn lerp_color(a: Color, b: Color, t: f32) -> Color {
    match (a, b) {
        (Color::Rgb(r1, g1, b1), Color::Rgb(r2, g2, b2)) => Color::Rgb(
            lerp_channel(r1, r2, t),
            lerp_channel(g1, g2, t),
            lerp_channel(b1, b2, t),
        ),
        _ => a,
    }
}

fn sample_palette(palette: &[Color], u: f32) -> Color {
    let n = palette.len();
    match n {
        0 => BALANCE_THEME.background,
        1 => palette[0],
        _ => {
            let u = u.clamp(0.0, 1.0);
            let pos = u * (n - 1) as f32;
            let i0 = pos.floor() as usize;
            let i1 = (i0 + 1).min(n - 1);
            lerp_color(palette[i0], palette[i1], pos - i0 as f32)
        }
    }
}

/// Gradient background for row `local_y` in a button of `height` rows.
pub(crate) fn balance_gradient_bg(local_y: usize, height: usize) -> Color {
    let height = height.max(1);
    if height == 1 {
        return BALANCE_GRADIENT_TOP[0];
    }

    let t = local_y as f32 / (height - 1) as f32;
    if t <= TOP_HIGHLIGHT {
        sample_palette(BALANCE_GRADIENT_TOP, t / TOP_HIGHLIGHT)
    } else {
        sample_rgb_anchors(LOWER_GRADIENT_ANCHORS, (t - TOP_HIGHLIGHT) / (1.0 - TOP_HIGHLIGHT))
    }
}

/// Sample gradient at the vertical center of `widget` inside `button`.
pub(crate) fn balance_gradient_bg_for_widget(widget: Rect, button: Rect) -> Color {
    let center_y = widget.y + widget.height / 8;
    let local_y = center_y.saturating_sub(button.y) as usize;
    balance_gradient_bg(local_y, button.height as usize)
}

/// Home balance hero: vertical green gradient (light top, dark bottom) + chrome lines.
pub(crate) fn paint_balance_button_gradient(buf: &mut Buffer, area: Rect) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let h = area.height as usize;

    for dy in 0..area.height {
        let y = area.y + dy;
        let bg = balance_gradient_bg(dy as usize, h);
        let top_fg = Color::Rgb(44, 148, 40);
        let bottom_fg = BALANCE_THEME.shadow;

        for dx in 0..area.width {
            let x = area.x + dx;
            let (ch, fg) = if dy == 0 && area.height > 2 {
                ('▔', top_fg)
            } else if dy + 1 == area.height && area.height > 1 {
                ('▁', bottom_fg)
            } else {
                (' ', BALANCE_THEME.text)
            };

            if let Some(cell) = buf.cell_mut(Position::new(x, y)) {
                cell.set_char(ch);
                cell.set_fg(fg);
                cell.set_bg(bg);
            }
        }
    }
}

/// Full-width action button with top/bottom highlight lines.
pub(crate) struct Button<'a> {
    lines: Vec<Line<'a>>,
    theme: ButtonTheme,
    state: ButtonState,
}

impl<'a> Button<'a> {
    pub fn new<T: Into<Line<'a>>>(label: T) -> Self {
        Self {
            lines: vec![label.into()],
            theme: CANCEL_THEME,
            state: ButtonState::Normal,
        }
    }

    pub const fn theme(mut self, theme: ButtonTheme) -> Self {
        self.theme = theme;
        self
    }

    pub const fn state(mut self, state: ButtonState) -> Self {
        self.state = state;
        self
    }
}

/// Green/panel fill and top/bottom highlight lines (no label).
pub(crate) fn paint_button_chrome(buf: &mut Buffer, area: Rect, theme: ButtonTheme) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    buf.set_style(area, Style::new().bg(theme.background).fg(theme.text));
    if area.height > 2 {
        buf.set_string(
            area.x,
            area.y,
            "▔".repeat(area.width as usize),
            Style::new().fg(theme.highlight).bg(theme.background),
        );
    }
    if area.height > 1 {
        buf.set_string(
            area.x,
            area.y + area.height - 1,
            "▁".repeat(area.width as usize),
            Style::new().fg(theme.shadow).bg(theme.background),
        );
    }
}

impl Widget for Button<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        let (background, text, shadow, highlight) = match self.state {
            ButtonState::Normal => (
                self.theme.background,
                self.theme.text,
                self.theme.shadow,
                self.theme.highlight,
            ),
            ButtonState::Selected => (
                self.theme.highlight,
                self.theme.text,
                self.theme.shadow,
                self.theme.highlight,
            ),
        };
        let chrome = ButtonTheme {
            text,
            background,
            highlight,
            shadow,
        };
        paint_button_chrome(buf, area, chrome);
        let line_count = self.lines.len() as u16;
        let mut y = area.y + area.height.saturating_sub(line_count) / 2;
        for line in &self.lines {
            let label_w = line.width() as u16;
            let x = area.x + area.width.saturating_sub(label_w) / 2;
            buf.set_line(x, y, line, area.width);
            y = y.saturating_add(1);
        }
    }
}
