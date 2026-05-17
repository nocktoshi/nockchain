//! Theme colors, helpers, and branding for the REPL shell and splash.

use ratatui::style::Color;

/// Forest green accent (`#228B22` / rgb 34,139,32) — matches the boot splash border and wordmark.
pub(crate) const THEME_ACCENT_GREEN: Color = Color::Rgb(34, 139, 32);

/// `#0B0B0B` — deep fill (splash page background).
pub(crate) const THEME_BG_DEEP: Color = Color::Rgb(11, 11, 11);

/// `#3C3C3C` — panel / card surfaces.
pub(crate) const THEME_BG_PANEL: Color = Color::Rgb(60, 60, 60);

/// Drop shadow — visibly distinct from [`THEME_BG_DEEP`] (not the same as the page background).
pub(crate) const THEME_SHADOW: Color = Color::Rgb(36, 36, 40);

/// Unicode mathematical sans-serif bold — reuse for boot splash and loading state.
pub(crate) const SPLASH_BRAND: &str = " 𝐍 𝐎 𝐂 𝐊 𝐂 𝐇 𝐀 𝐈𝐍 ";

/// Ramp through forest greens into a bright peak and back — drives the loading brand “chase” so the
/// highlight reads as a pulse as it moves across the wordmark.
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

/// Subtle breathing grayscale around white — shared phase so the splash wordmark moves together.
pub(crate) fn pulse_color(frame_counter: usize) -> Color {
    const W: i32 = 255;
    let period = 56usize;
    let t = frame_counter % period;
    let h = period / 2;
    let wave = if t < h { t } else { period - t };
    // ±7 around white; clamp so it stays bright / slightly cool gray at trough.
    let d = (wave as i32 * 14 / h as i32) - 7;
    Color::Rgb(
        (W + d).clamp(222, 255) as u8,
        (W + d).clamp(222, 255) as u8,
        (W + d).clamp(222, 255) as u8,
    )
}
