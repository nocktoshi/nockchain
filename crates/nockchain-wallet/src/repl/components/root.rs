//! Main three-strip layout: menu / output / hints.

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, HighlightSpacing, List, ListItem, Paragraph, Wrap,
};
use ratatui::Frame;

use crate::repl::app_state::{AppState, PanelFocus};
use crate::repl::screens::Screen;

use super::balance_sidebar::draw_balance_sidebar;
use super::create_tx_panel::draw_create_tx;
use super::loading::loading_indicator_paragraph;
use super::menus::{
    BOOL, IMPORT_SRC, KEYS_MENU, MAIN_MENU, NOTES_MENU, SETTINGS_MENU, SIGN_MENU, TX_MENU,
    WATCH_MENU,
};
use super::scroll::estimate_wrapped_source_lines;
use super::splash::draw_splash;
use super::theme::SPLASH_BRAND;

pub(crate) fn draw_ui(f: &mut Frame<'_>, app: &mut AppState, tick: u64) {
    if matches!(app.screen, Screen::Splash) {
        draw_splash(f, tick);
        return;
    }

    let block = Block::default().borders(Borders::ALL).title(Span::styled(
        SPLASH_BRAND,
        Style::default().fg(Color::Green),
    ));
    let inner = block.inner(f.area());
    f.render_widget(block, f.area());

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(14), Constraint::Length(2)])
        .split(inner);

    let panel = match &app.screen {
        Screen::Running { restore, .. } => (**restore).clone(),
        s => s.clone(),
    };
    let is_running = matches!(app.screen, Screen::Running { .. });

    let (menu_area, balance_area) =
        if matches!(panel, Screen::Main { .. }) && !is_running {
            let h = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Fill(1), Constraint::Fill(1)])
                .split(chunks[0]);
            (h[0], Some(h[1]))
        } else {
            (chunks[0], None)
        };

    match &panel {
        Screen::Splash => {}
        Screen::Notes { sel } => {
            list_draw(f, app, menu_area, "Balances", NOTES_MENU, *sel);
        }
        Screen::Main { sel } => {
            list_draw(f, app, menu_area, "Wallet", MAIN_MENU, *sel);
        }
        Screen::Keys { sel } => {
            list_draw(f, app, menu_area, "Keys", KEYS_MENU, *sel);
        }
        Screen::KeysImport { sel } => {
            list_draw(f, app, menu_area, "Import from", IMPORT_SRC, *sel);
        }
        Screen::Transactions { sel } => {
            list_draw(f, app, menu_area, "Transactions", TX_MENU, *sel);
        }
        Screen::Watch { sel } => {
            list_draw(f, app, menu_area, "Watch-only", WATCH_MENU, *sel);
        }
        Screen::SignVerify { sel } => {
            list_draw(f, app, menu_area, "Sign / verify", SIGN_MENU, *sel);
        }
        Screen::Settings { sel } => {
            list_draw(f, app, menu_area, "Settings & help", SETTINGS_MENU, *sel);
        }
        Screen::Quick { line } => {
            let t = format!("Quick command (help, exit, …)\n\n> {line}");
            let p = Paragraph::new(t).wrap(Wrap { trim: true });
            f.render_widget(p, menu_area);
        }
        Screen::TextPrompt { title, value, .. } => {
            let t = format!("{title}\n\n> {value}");
            let p = Paragraph::new(t).wrap(Wrap { trim: true });
            f.render_widget(p, menu_area);
        }
        Screen::Confirm {
            title, sel, labels, ..
        } => {
            list_draw(f, app, menu_area, title.as_str(), labels, *sel);
        }
        Screen::CreateTx { w } => {
            draw_create_tx(f, menu_area, w, tick);
        }
        Screen::ExitConfirm { sel } => {
            list_draw(f, app, menu_area, "Exit REPL?", BOOL, *sel);
        }
        Screen::ErrorScreen {
            msg, sel, actions, ..
        } => {
            let header = Paragraph::new(format!("Error\n\n{msg}\n"))
                .wrap(Wrap { trim: true })
                .block(Block::default().borders(Borders::BOTTOM));
            let area = menu_area;
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(80), Constraint::Percentage(20)])
                .split(area);
            f.render_widget(header, split[0]);
            list_draw(f, app, split[1], "Choose", actions, *sel);
        }
        Screen::Running { .. } => {}
    }

    if let Some(bal) = balance_area {
        draw_balance_sidebar(f, app, bal, tick);
    }

    if is_running {
        let label = if let Screen::Running { label, .. } = &app.screen {
            label.as_str()
        } else {
            ""
        };
        let running_block = Block::default()
            .borders(Borders::ALL)
            .title(Span::styled("Output", Style::default().fg(Color::Cyan)));
        let body = loading_indicator_paragraph(app, tick, running_block, label);
        f.render_widget(body, chunks[1]);
    } else {
        let output_text = if app.last_command_output.is_empty() {
            "<<< no output yet >>>".to_string()
        } else {
            app.last_command_output.clone()
        };
        let output_block = Block::default()
            .borders(Borders::ALL)
            .title(Span::styled("Output", Style::default().fg(Color::Cyan)));
        let inner = output_block.inner(chunks[1]);
        if app.last_command_output.is_empty() {
            app.output_scroll = 0;
        }
        let scroll_y = if app.last_command_output.is_empty() {
            0
        } else {
            let inner_w = inner.width.max(1);
            // Word-wrap + ANSI can exceed a naive char-width estimate; pad so scroll/clamp reaches the end.
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
        f.render_widget(output_para, chunks[1]);
    }

    let hint_line = if matches!(app.screen, Screen::Running { .. }) {
        Line::from(vec![
            Span::styled("Working… ", Style::default().fg(Color::Yellow)),
            Span::styled(
                "(status in Output panel below)",
                Style::default().fg(Color::DarkGray),
            ),
        ])
    } else if let Some(ref toast) = app.toast {
        Line::from(vec![
            Span::styled(format!("✓ {toast}"), Style::default().fg(Color::Green)),
            Span::raw("  ·  "),
            Span::styled("any key", Style::default().fg(Color::DarkGray)),
            Span::raw(" dismiss"),
        ])
    } else if app.panel_focus == PanelFocus::Balance {
        Line::from(vec![
            Span::styled("↑/↓ j/k ", Style::default().fg(Color::Yellow)),
            Span::raw("scroll balance  "),
            Span::styled("PgUp/PgDn ", Style::default().fg(Color::DarkGray)),
            Span::raw("page  "),
            Span::styled("Enter ", Style::default().fg(Color::Yellow)),
            Span::raw("menu  "),
            Span::styled("Tab ", Style::default().fg(Color::DarkGray)),
            Span::raw("panels"),
        ])
    } else if app.panel_focus == PanelFocus::Output {
        Line::from(vec![
            Span::styled("↑/↓ j/k ", Style::default().fg(Color::Yellow)),
            Span::raw("scroll output  "),
            Span::styled("PgUp/PgDn ", Style::default().fg(Color::DarkGray)),
            Span::raw("page  "),
            Span::styled("Enter ", Style::default().fg(Color::Yellow)),
            Span::raw("menu  "),
            Span::styled("Tab ", Style::default().fg(Color::DarkGray)),
            Span::raw("panels"),
        ])
    } else {
        let parts = vec![
            Span::styled("↑/↓ ", Style::default().fg(Color::DarkGray)),
            Span::raw("menu  "),
            Span::styled("Enter ", Style::default().fg(Color::DarkGray)),
            Span::raw("select  "),
            Span::styled("Tab ", Style::default().fg(Color::Yellow)),
            Span::raw("balance · output · menu  "),
            Span::styled("paste ", Style::default().fg(Color::DarkGray)),
            Span::raw("Cmd/Ctrl+V  "),
            Span::styled("q/Esc ", Style::default().fg(Color::DarkGray)),
            Span::raw("back/quit"),
        ];
        Line::from(parts)
    };
    let hint = Paragraph::new(hint_line);
    f.render_widget(hint, chunks[2]);
}

fn list_draw(
    f: &mut Frame<'_>,
    app: &mut AppState,
    area: ratatui::layout::Rect,
    title: &str,
    items: &[&str],
    sel: usize,
) {
    let list_items: Vec<ListItem> = items
        .iter()
        .map(|s| ListItem::new(Line::from(*s)))
        .collect();
    app.list_state.select(Some(sel));
    let list = List::new(list_items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_spacing(HighlightSpacing::Never)
        .highlight_symbol("");
    f.render_stateful_widget(list, area, &mut app.list_state);
}
