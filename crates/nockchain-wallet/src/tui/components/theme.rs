//! Theme colors, helpers, and branding for the TUI shell and splash.

use ratatui::style::Color;

/// Forest green accent (`#228B22` / rgb 34,139,32) — matches the boot splash border and wordmark.
pub(crate) const THEME_ACCENT_GREEN: Color = Color::Rgb(34, 139, 32);

/// Send CTA — gold/yellow (extension primary action).
pub(crate) const THEME_ACCENT_CTA: Color = Color::Rgb(255, 193, 7);

/// `#0B0B0B` — deep fill (splash page background).
pub(crate) const THEME_BG_DEEP: Color = Color::Rgb(11, 11, 11);

/// `#3C3C3C` — panel / card surfaces.
pub(crate) const THEME_BG_PANEL: Color = Color::Rgb(60, 60, 60);

/// Drop shadow — visibly distinct from [`THEME_BG_DEEP`].
pub(crate) const THEME_SHADOW: Color = Color::Rgb(36, 36, 40);

/// Muted hint text.
pub(crate) const THEME_MUTED: Color = Color::Rgb(140, 140, 140);

/// Unicode mathematical sans-serif bold — reuse for boot splash and loading state.
pub(crate) const SPLASH_BRAND: &str = " 𝐍 𝐎 𝐂 𝐊 𝐂 𝐇 𝐀 𝐈𝐍 ";

/// Ramp through forest greens into a bright peak and back.
pub(crate) const LOADING_BRAND_PALETTE: &[Color] = &[
    Color::Rgb(28, 105, 26),
    Color::Rgb(38, 135, 36),
    THEME_ACCENT_GREEN,
    Color::Rgb(52, 185, 48),
    Color::Rgb(78, 235, 74),
    Color::Rgb(140, 220, 136),
    Color::Rgb(210, 245, 208),
    Color::Rgb(240, 252, 240),
    Color::Rgb(210, 245, 208),
    Color::Rgb(140, 220, 136),
    Color::Rgb(78, 235, 74),
    Color::Rgb(52, 185, 48),
    THEME_ACCENT_GREEN,
    Color::Rgb(38, 135, 36),
];

/// Animated brand green from [`LOADING_BRAND_PALETTE`] (splash / home borders).
pub(crate) fn pulse_green_rgb(frame_counter: usize) -> Color {
    let palette = LOADING_BRAND_PALETTE;
    let n = palette.len().max(1);
    let idx = (frame_counter / 4) % n;
    palette[idx]
}

/// Pulsing border green (splash outer frame).
pub(crate) fn pulse_border_green(frame_counter: usize) -> Color {
    if (frame_counter % 48) < 24 {
        THEME_ACCENT_GREEN
    } else {
        Color::Rgb(26, 115, 25)
    }
}
