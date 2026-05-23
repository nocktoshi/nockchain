//! Two-panel layout: activity (top) + status/output (bottom).

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, HighlightSpacing, List, ListItem, Paragraph, Wrap,
};
use ratatui::Frame;

use super::ambient::{header_star_line, render_home_ambient};
use super::create_tx_panel::draw_create_tx;
use super::home::{balance_button_abs_rect, draw_wallet_tab};
use super::home_tabs::draw_home_tabs;
use super::loading::loading_indicator_paragraph;
use super::menus::{
    IMPORT_SRC, KEYS_MENU, MAIN_MENU, NOTES_MENU, SETTINGS_MENU, SIGN_MENU, TX_MENU,
    WATCH_MENU,
};
use super::nns_buy::draw_nns_buy;
use super::prompt_bar::{draw_prompt_bar, prompt_bar_height};
use super::receive::draw_receive;
use super::send_simple_panel::draw_send_simple;
use super::scroll::estimate_wrapped_source_lines;
use super::splash::draw_splash;
use super::theme::{
    pulse_border_green, SPLASH_BRAND, THEME_ACCENT_GREEN, THEME_BG_DEEP, THEME_MUTED,
};
use crate::repl::app_state::{status_modal_visible, AppState, PanelFocus};
use crate::repl::prompt_overlay::{activity_underlay, has_prompt_overlay};
use crate::repl::screens::{Screen, SendSimplePhase};

pub(crate) fn draw_ui(f: &mut Frame<'_>, store: &mut crate::repl::store::UIStore) {
    let app = &mut store.state;
    let tick = app.ui_fx.frame_clock;
    if matches!(app.screen, Screen::Splash) {
        draw_splash(f, tick);
        return;
    }

    let pulse = pulse_border_green(tick as usize);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(pulse))
        .title(Span::styled(SPLASH_BRAND, Style::default().fg(THEME_ACCENT_GREEN)))
        .style(Style::default().bg(THEME_BG_DEEP));
    let inner = block.inner(f.area());
    f.render_widget(block, f.area());

    let panel = match &app.screen {
        Screen::Running { restore, .. } => (**restore).clone(),
        s => s.clone(),
    };
    let is_running = matches!(app.screen, Screen::Running { .. });
    let status_visible = status_modal_visible(app);
    let prompt_h = prompt_bar_height(&app.screen);
    const HINT_LINES: u16 = 1;

    if status_visible {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
            .split(inner);
        draw_activity_panel(
            f,
            app,
            &store.session_display,
            chunks[0],
            &panel,
            tick,
            is_running,
            has_prompt_overlay(&app.screen),
        );
        draw_status_panel(f, app, chunks[1], is_running);
    } else if prompt_h > 0 {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),
                Constraint::Length(prompt_h.saturating_add(HINT_LINES)),
            ])
            .split(inner);
        draw_activity_panel(
            f,
            app,
            &store.session_display,
            chunks[0],
            &panel,
            tick,
            is_running,
            has_prompt_overlay(&app.screen),
        );
        draw_prompt_footer(f, app, chunks[1], prompt_h);
    } else {
        draw_activity_panel(
            f,
            app,
            &store.session_display,
            inner,
            &panel,
            tick,
            is_running,
            has_prompt_overlay(&app.screen),
        );
    }
}

fn draw_prompt_footer(f: &mut Frame<'_>, app: &AppState, area: ratatui::layout::Rect, prompt_h: u16) {
    if area.height == 0 {
        return;
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(prompt_h), Constraint::Length(1)])
        .split(area);
    draw_prompt_bar(f, app, chunks[0]);
    f.render_widget(Paragraph::new(activity_hint_line(app)), chunks[1]);
}

fn draw_activity_panel(
    f: &mut Frame<'_>,
    app: &mut AppState,
    session: &crate::repl::wallet_api::WalletSessionState,
    area: ratatui::layout::Rect,
    panel: &Screen,
    tick: u64,
    is_running: bool,
    overlay_active: bool,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    if matches!(panel, Screen::Home) && !is_running {
        let mask = if app.home_tab == 0 {
            balance_button_abs_rect(area)
        } else {
            None
        };
        render_home_ambient(f, area, tick as usize, mask);
    }

    let panel = if overlay_active {
        activity_underlay(panel)
    } else {
        panel.clone()
    };
    let panel = &panel;

    match panel {
        Screen::Home if !is_running => draw_home_shell(f, app, area, tick),
        Screen::Receive { .. } => draw_receive(f, app, panel, area, tick),
        Screen::NnsBuy { .. } => draw_nns_buy(f, app, panel, area, tick),
        Screen::SendSimple { .. } if !is_running => draw_send_simple(f, app, area),
        Screen::Notes { sel } => list_draw(f, app, area, "Balances", NOTES_MENU, *sel),
        Screen::Keys { sel } => list_draw(f, app, area, "Keys", KEYS_MENU, *sel),
        Screen::KeysImport { sel } => list_draw(f, app, area, "Import from", IMPORT_SRC, *sel),
        Screen::Transactions { sel } => list_draw(f, app, area, "Transactions", TX_MENU, *sel),
        Screen::Watch { sel } => list_draw(f, app, area, "Watch-only", WATCH_MENU, *sel),
        Screen::SignVerify { sel } => list_draw(f, app, area, "Sign / verify", SIGN_MENU, *sel),
        Screen::Settings { sel } => {
            let endpoint_line = format!(
                "Public gRPC: `{}`\nJSON API: `{}`",
                session.public_grpc_server_addr, session.api_listen,
            );
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(3)])
                .split(area);
            let endpoint = Paragraph::new(endpoint_line)
                .block(Block::default().borders(Borders::ALL).title("Connection"));
            f.render_widget(endpoint, split[0]);
            list_draw(f, app, split[1], "Settings & help", SETTINGS_MENU, *sel);
        }
        Screen::Quick { line } => {
            let t = format!("Quick command (help, exit, …)\n\n> {line}");
            let p = Paragraph::new(t)
                .wrap(Wrap { trim: true })
                .block(activity_block(
                    "Quick",
                    !status_modal_visible(app),
                ));
            f.render_widget(p, area);
        }
        Screen::CreateTx { w } => {
            draw_create_tx(
                f,
                area,
                w,
                tick,
                !status_modal_visible(app),
            );
        }
        Screen::ErrorScreen {
            msg, sel, actions, ..
        } => {
            let header = Paragraph::new(format!("Error\n\n{msg}\n"))
                .wrap(Wrap { trim: true })
                .block(Block::default().borders(Borders::BOTTOM));
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(80), Constraint::Percentage(20)])
                .split(area);
            f.render_widget(header, split[0]);
            list_draw(f, app, split[1], "Choose", actions, *sel);
        }
        Screen::Running { .. } | Screen::Splash => {}
        Screen::Home => {}
        Screen::SendSimple { .. } => {}
        Screen::TextPrompt { .. } | Screen::Confirm { .. } | Screen::ExitConfirm { .. } => {}
    }
}

fn draw_home_shell(f: &mut Frame<'_>, app: &mut AppState, area: ratatui::layout::Rect, tick: u64) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(3),
        ])
        .split(area);

    f.render_widget(
        Paragraph::new(header_star_line(tick as usize)).alignment(ratatui::layout::Alignment::Center),
        layout[0],
    );
    draw_home_tabs(f, layout[1], app.home_tab, tick);

    // Opaque panel so ambient does not show through balance, CTAs, or menu list.
    let content_bg = Block::default().style(Style::default().bg(THEME_BG_DEEP));
    let content = content_bg.inner(layout[2]);
    f.render_widget(content_bg, layout[2]);

    if app.home_tab == 0 {
        draw_wallet_tab(f, app, content, tick);
    } else {
        list_draw(f, app, content, "Menu", MAIN_MENU, app.menu_sel);
    }
}

fn draw_status_panel(
    f: &mut Frame<'_>,
    app: &mut AppState,
    area: ratatui::layout::Rect,
    is_running: bool,
) {
    if area.height == 0 {
        return;
    }
    let tick = app.ui_fx.frame_clock;
    let prompt_h = prompt_bar_height(&app.screen);
    let status_split = if prompt_h > 0 {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(0),
                Constraint::Length(prompt_h),
                Constraint::Length(1),
            ])
            .split(area)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(0), Constraint::Length(1)])
            .split(area)
    };
    let (output_area, prompt_area, hint_area) = if prompt_h > 0 {
        (status_split[0], Some(status_split[1]), status_split[2])
    } else {
        (status_split[0], None, status_split[1])
    };

    let title = Span::styled("Output", Style::default().fg(Color::Cyan));

    if is_running {
        let label = if let Screen::Running { label, .. } = &app.screen {
            label.as_str()
        } else {
            ""
        };
        let running_block = status_panel_block(true, title);
        let body = loading_indicator_paragraph(app, tick, running_block, label);
        f.render_widget(body, output_area);
    } else {
        let output_text = app.last_command_output.clone();
        let output_block = status_panel_block(true, title);
        let inner = output_block.inner(output_area);
        let scroll_y = if output_text.is_empty() {
            0
        } else {
            let inner_w = inner.width.max(1);
            let base = estimate_wrapped_source_lines(&output_text, inner_w);
            let measure = base.saturating_add(base / 4).saturating_add(12);
            let visible = inner.height as usize;
            let max_scroll = measure.saturating_sub(visible);
            let max_u16 = u16::try_from(max_scroll).unwrap_or(u16::MAX);
            app.output_scroll = app.output_scroll.min(max_u16);
            app.output_scroll
        };
        let output_para = Paragraph::new(output_text)
            .wrap(Wrap { trim: true })
            .block(output_block)
            .scroll((scroll_y, 0));
        f.render_widget(output_para, output_area);
    }

    if let Some(prompt_area) = prompt_area {
        draw_prompt_bar(f, app, prompt_area);
    }
    f.render_widget(Paragraph::new(status_modal_hint_line(app)), hint_area);
}

fn status_modal_hint_line(app: &AppState) -> Line<'static> {
    if matches!(app.screen, Screen::Running { .. }) {
        return Line::from(vec![
            Span::styled("Working… ", Style::default().fg(Color::Yellow)),
            Span::styled("(see spinner above)", Style::default().fg(Color::DarkGray)),
        ]);
    }
    if let Some(ref toast) = app.toast {
        return Line::from(vec![
            Span::styled(format!("✓ {toast}"), Style::default().fg(Color::Green)),
            Span::raw("  ·  "),
            Span::styled("any key", Style::default().fg(Color::DarkGray)),
            Span::raw(" dismiss"),
        ]);
    }
    if has_prompt_overlay(&app.screen) {
        return match &app.screen {
            Screen::TextPrompt { .. } => Line::from(vec![
                Span::styled("type ", Style::default().fg(Color::Yellow)),
                Span::raw("input  "),
                Span::styled("Enter ", Style::default().fg(Color::Yellow)),
                Span::raw("submit  "),
                Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
                Span::raw("cancel"),
            ]),
            Screen::Confirm { .. } | Screen::ExitConfirm { .. } => Line::from(vec![
                Span::styled("↑/↓ ", Style::default().fg(Color::Yellow)),
                Span::raw("choose  "),
                Span::styled("Enter ", Style::default().fg(Color::Yellow)),
                Span::raw("confirm  "),
                Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
                Span::raw("cancel"),
            ]),
            _ => Line::from(""),
        };
    }
    if status_modal_visible(app) && !matches!(app.screen, Screen::Running { .. }) {
        return Line::from(vec![
            Span::styled("↑/↓ j/k ", Style::default().fg(Color::Yellow)),
            Span::raw("scroll  "),
            Span::styled("Enter ", Style::default().fg(Color::Yellow)),
            Span::raw("dismiss"),
        ]);
    }
    activity_hint_line(app)
}

fn activity_hint_line(app: &AppState) -> Line<'static> {
    if matches!(app.screen, Screen::Home) {
        return Line::from(vec![
            Span::styled("←/→ ", Style::default().fg(Color::Yellow)),
            Span::raw("tabs  "),
            Span::styled("s/r/b ", Style::default().fg(Color::Yellow)),
            Span::raw("actions  "),
            Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
            Span::raw("quit"),
        ]);
    }
    if matches!(app.screen, Screen::Receive { .. }) {
        return Line::from(vec![
            Span::styled("Enter ", Style::default().fg(Color::Yellow)),
            Span::raw("copy  "),
            Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
            Span::raw("back"),
        ]);
    }
    if matches!(app.screen, Screen::NnsBuy { .. }) {
        return Line::from(vec![
            Span::styled("Tab/↑↓ ", Style::default().fg(Color::Yellow)),
            Span::raw("nav  "),
            Span::styled("Enter ", Style::default().fg(Color::Yellow)),
            Span::raw("search/register  "),
            Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
            Span::raw("back"),
        ]);
    }
    if let Screen::SendSimple { phase, .. } = &app.screen {
        return match phase {
            SendSimplePhase::Planning => Line::from(vec![
                Span::styled("Planning transaction…", Style::default().fg(Color::Yellow)),
            ]),
            SendSimplePhase::Review { .. } => Line::from(vec![
                Span::styled("↑/↓ ", Style::default().fg(Color::Yellow)),
                Span::raw("scroll  "),
                Span::styled("←/→ ", Style::default().fg(Color::Yellow)),
                Span::raw("buttons  "),
                Span::styled("Enter ", Style::default().fg(Color::Yellow)),
                Span::raw("send  "),
                Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
                Span::raw("back"),
            ]),
            SendSimplePhase::Form => Line::from(vec![
                Span::styled("Tab/↑↓ ", Style::default().fg(Color::Yellow)),
                Span::raw("fields  "),
                Span::styled("m ", Style::default().fg(Color::Yellow)),
                Span::raw("max  "),
                Span::styled("←/→ ", Style::default().fg(Color::DarkGray)),
                Span::raw("buttons  "),
                Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
                Span::raw("back"),
            ]),
        };
    }
    Line::from(vec![
        Span::styled("↑/↓ ", Style::default().fg(Color::DarkGray)),
        Span::raw("nav  "),
        Span::styled("Enter ", Style::default().fg(Color::DarkGray)),
        Span::raw("select  "),
        Span::styled("Esc ", Style::default().fg(THEME_MUTED)),
        Span::raw("back"),
    ])
}

fn activity_block<'a>(title: &'a str, focused: bool) -> Block<'a> {
    let mut b = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .style(Style::default().bg(THEME_BG_DEEP));
    if focused {
        b = b
            .border_type(BorderType::Thick)
            .border_style(Style::default().fg(THEME_ACCENT_GREEN));
    }
    b
}

fn status_panel_block(focused: bool, title: Span<'static>) -> Block<'static> {
    let mut b = Block::default().borders(Borders::ALL).title(title);
    if focused {
        b = b
            .border_type(BorderType::Thick)
            .border_style(Style::default().fg(THEME_ACCENT_GREEN));
    }
    b
}

fn list_draw(
    f: &mut Frame<'_>,
    app: &mut AppState,
    area: ratatui::layout::Rect,
    title: &str,
    items: &[&str],
    sel: usize,
) {
    let item_fg = Color::Rgb(210, 210, 210);
    let list_items: Vec<ListItem> = items
        .iter()
        .map(|s| {
            ListItem::new(Line::from(Span::styled(
                *s,
                Style::default().fg(item_fg).bg(THEME_BG_DEEP),
            )))
        })
        .collect();
    app.list_state.select(Some(sel));
    let list = List::new(list_items)
        .block(activity_block(title, !status_modal_visible(app)))
        .style(Style::default().bg(THEME_BG_DEEP))
        .highlight_style(
            Style::default()
                .fg(Color::White)
                .bg(THEME_ACCENT_GREEN)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_spacing(HighlightSpacing::Never)
        .highlight_symbol("▸ ");
    f.render_stateful_widget(list, area, &mut app.list_state);
}
