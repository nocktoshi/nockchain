//! Shared branding strings for shell chrome and loading UI.

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
