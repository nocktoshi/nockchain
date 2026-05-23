//! Ratatui Tabs for Wallet / Menu on the home screen.

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::Tabs;
use ratatui::Frame;

use super::theme::pulse_green_rgb;

pub(crate) fn draw_home_tabs(f: &mut Frame<'_>, area: Rect, home_tab: usize, tick: u64) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let tabs = Tabs::new(vec!["Wallet", "Menu"])
        .style(Style::default().fg(Color::Rgb(140, 140, 140)))
        .highlight_style(
            Style::default()
                .fg(pulse_green_rgb(tick as usize))
                .add_modifier(Modifier::BOLD),
        )
        .select(home_tab.min(1))
        .divider(ratatui::symbols::DOT)
        .padding(" ", " ");
    f.render_widget(tabs, area);
}
