//! Ratatui terminal: layout, event pump, and suspend/resume around wallet I/O.
//!
//! **Input:** key events are read on a background thread and sent over an unbounded channel;
//! `tokio::select!` merges them with a tick for spinners. **Paste:** bracketed paste mode is
//! enabled so terminals emit `Event::Paste` with full clipboard text (needed for address fields
//! and other line editors).

use std::io::{self, stdout, Stdout};
use std::time::Duration;

use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste, Event, KeyEventKind};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use nockapp::NockAppError;
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, HighlightSpacing, List, ListItem, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tracing::info;

use super::app_state::{AppState, PanelFocus};
use super::command_runner::{self, JobCompletion, ReplRuntime};
use super::create_tx::{OptSub, Phase, RecSub};
use super::handlers;
use super::screens::Screen;
use crate::command::WalletCli;

pub(super) type Term = Terminal<ratatui::backend::CrosstermBackend<Stdout>>;

pub(super) const MAIN_MENU: &[&str] = &[
    "Keys & addresses", "Notes & balance", "Transactions", "Watch-only", "Sign / verify",
    "Settings & help", "Quick commands (repl line)", "Exit",
];

pub(super) const KEYS_MENU: &[&str] = &[
    "Keygen", "Derive child key", "Import keys (file / extended key / seed)", "Export keys",
    "Show seed phrase", "Show master zpub", "Show master zprv", "Show key tree",
    "List active addresses", "List master addresses", "Set active master address",
    "Import master pubkey", "Export master pubkey", "Back",
];

pub(super) const IMPORT_SRC: &[&str] = &["File", "Extended key", "Seed phrase", "Back"];

pub(super) const NOTES_MENU: &[&str] = &[
    "List all notes", "List notes by address (required)", "List notes by address (CSV)",
    "Show balance", "Back",
];

pub(super) const TX_MENU: &[&str] = &[
    "Create transaction (planner)", "Send transaction file", "Show transaction file",
    "Sign multisig transaction", "Migrate v0 notes", "Back",
];

pub(super) const WATCH_MENU: &[&str] = &["Address or pubkey", "Pubkey only", "Multisig", "Back"];

pub(super) const SIGN_MENU: &[&str] =
    &["Sign message", "Verify message", "Sign hash", "Verify hash", "Back"];

pub(super) const SETTINGS_MENU: &[&str] = &["Show help again", "Verbose / logging info", "Back"];

pub(super) const BOOL: &[&str] = &["Yes", "No"];

pub(super) const NOTE_ORDER: &[&str] = &["Ascending", "Descending"];

pub(super) const CT_ERR_ACTIONS: &[&str] = &[
    "Retry", "Edit planning options", "Start over (new recipients)", "Back to Transactions menu",
];

pub(super) const GENERIC_ERR: &[&str] = &["Retry", "Back"];

pub(super) async fn run_tui(cli: WalletCli, rt: ReplRuntime) -> Result<(), NockAppError> {
    LocalSet::new().run_until(run_tui_inner(cli, rt)).await
}

async fn run_tui_inner(cli: WalletCli, rt: ReplRuntime) -> Result<(), NockAppError> {
    stdout().execute(EnterAlternateScreen).map_err(io_err)?;
    enable_raw_mode().map_err(io_err)?;
    stdout().execute(EnableBracketedPaste).map_err(io_err)?;

    let mut terminal =
        Term::new(ratatui::backend::CrosstermBackend::new(stdout())).map_err(io_err)?;
    terminal.hide_cursor().map_err(io_err)?;
    let terminal = std::sync::Arc::new(tokio::sync::Mutex::new(terminal));

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<Event>();
    std::thread::spawn(move || loop {
        if crossterm::event::poll(Duration::from_millis(120)).unwrap_or(false) {
            if let Ok(ev) = crossterm::event::read() {
                let _ = ev_tx.send(ev);
            }
        }
    });

    let (job_done_tx, mut job_done_rx) = mpsc::unbounded_channel::<JobCompletion>();

    let mut app = AppState::new(Screen::Splash);
    let mut tick: u64 = 0;
    let mut interval = tokio::time::interval(Duration::from_millis(120));

    let result = loop {
        {
            let mut term_guard = terminal.lock().await;
            term_guard
                .draw(|f| draw_ui(f, &mut app, tick))
                .map_err(io_err)?;
        }

        tokio::select! {
            biased;
            maybe_job = job_done_rx.recv() => {
                if let Some((res, captured)) = maybe_job {
                    command_runner::apply_job_result(&mut app, res, captured);
                }
            }
            _ = interval.tick() => {
                tick = tick.wrapping_add(1);
            }
            Some(ev) = ev_rx.recv() => {
                match ev {
                    Event::Key(key) => {
                        if key.kind == KeyEventKind::Release {
                            continue;
                        }
                        match handlers::dispatch_key(
                            &cli,
                            &rt,
                            &mut app,
                            key,
                            &terminal,
                            &job_done_tx,
                        )
                        .await
                        {
                            Ok(super::screens::ReplControl::Continue) => {}
                            Ok(super::screens::ReplControl::Quit) => break Ok(()),
                            Err(e) => {
                                let mut term_guard = terminal.lock().await;
                                let _ = restore_terminal(&mut term_guard);
                                break Err(e);
                            }
                        }
                    }
                    Event::Paste(text) => {
                        match handlers::dispatch_paste(&cli, &mut app, text).await {
                            Ok(super::screens::ReplControl::Continue) => {}
                            Ok(super::screens::ReplControl::Quit) => break Ok(()),
                            Err(e) => {
                                let mut term_guard = terminal.lock().await;
                                let _ = restore_terminal(&mut term_guard);
                                break Err(e);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    };

    let mut term_guard = terminal.lock().await;
    let _ = restore_terminal(&mut term_guard);
    result
}

fn restore_terminal(terminal: &mut Term) -> io::Result<()> {
    let _ = stdout().execute(DisableBracketedPaste);
    disable_raw_mode()?;
    terminal.show_cursor()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

pub(crate) fn io_err(e: io::Error) -> NockAppError {
    NockAppError::OtherError(format!("terminal io: {e}"))
}

/// Unicode mathematical sans-serif bold — reuse for boot splash and loading state.
const SPLASH_BRAND: &str = " 𝐍 𝐎 𝐂 𝐊 𝐂 𝐇 𝐀 𝐈𝐍 ";

/// Rough wrapped-row upper bound for clamping scroll (must be ≥ ratatui word-wrap rows).
fn estimate_wrapped_source_lines(text: &str, inner_w: u16) -> usize {
    let w = inner_w.max(1) as usize;
    text.split('\n')
        .map(|line| {
            let n = line.chars().count().max(1);
            (n + w - 1) / w
        })
        .sum::<usize>()
        .max(1)
}

fn draw_ui(f: &mut Frame<'_>, app: &mut AppState, tick: u64) {
    let block = Block::default().borders(Borders::ALL).title(Span::styled(
        SPLASH_BRAND,
        Style::default().fg(Color::Green),
    ));
    let inner = block.inner(f.area());
    f.render_widget(block, f.area());

    if matches!(app.screen, Screen::Splash) {
        let vchunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(35),
                Constraint::Length(10),
                Constraint::Percentage(40),
            ])
            .split(inner);
        let splash = Paragraph::new(vec![
            Line::from(Span::styled(
                SPLASH_BRAND,
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "|----- Programmable Gold -----|",
                Style::default().fg(Color::Yellow),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "press any key to start",
                Style::default().fg(Color::Gray),
            )),
        ])
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });
        f.render_widget(splash, vchunks[1]);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(14), Constraint::Length(2)])
        .split(inner);

    let panel = match &app.screen {
        Screen::Running { restore, .. } => (**restore).clone(),
        s => s.clone(),
    };
    let is_running = matches!(app.screen, Screen::Running { .. });
    match &panel {
        Screen::Splash => {}
        Screen::Notes { sel } => {
            list_draw(f, app, chunks[0], "Balances", NOTES_MENU, *sel);
        }
        Screen::Main { sel } => {
            list_draw(f, app, chunks[0], "Wallet", MAIN_MENU, *sel);
        }
        Screen::Keys { sel } => {
            list_draw(f, app, chunks[0], "Keys", KEYS_MENU, *sel);
        }
        Screen::KeysImport { sel } => {
            list_draw(f, app, chunks[0], "Import from", IMPORT_SRC, *sel);
        }
        Screen::Transactions { sel } => {
            list_draw(f, app, chunks[0], "Transactions", TX_MENU, *sel);
        }
        Screen::Watch { sel } => {
            list_draw(f, app, chunks[0], "Watch-only", WATCH_MENU, *sel);
        }
        Screen::SignVerify { sel } => {
            list_draw(f, app, chunks[0], "Sign / verify", SIGN_MENU, *sel);
        }
        Screen::Settings { sel } => {
            list_draw(f, app, chunks[0], "Settings & help", SETTINGS_MENU, *sel);
        }
        Screen::Quick { line } => {
            let t = format!("Quick command (help, exit, …)\n\n> {line}");
            let p = Paragraph::new(t).wrap(Wrap { trim: true });
            f.render_widget(p, chunks[0]);
        }
        Screen::TextPrompt { title, value, .. } => {
            let t = format!("{title}\n\n> {value}");
            let p = Paragraph::new(t).wrap(Wrap { trim: true });
            f.render_widget(p, chunks[0]);
        }
        Screen::Confirm {
            title, sel, labels, ..
        } => {
            list_draw(f, app, chunks[0], title.as_str(), labels, *sel);
        }
        Screen::CreateTx { w } => {
            draw_create_tx(f, chunks[0], w, tick);
        }
        Screen::ExitConfirm { sel } => {
            list_draw(f, app, chunks[0], "Exit REPL?", BOOL, *sel);
        }
        Screen::ErrorScreen {
            msg, sel, actions, ..
        } => {
            let header = Paragraph::new(format!("Error\n\n{msg}\n"))
                .wrap(Wrap { trim: true })
                .block(Block::default().borders(Borders::BOTTOM));
            let area = chunks[0];
            let split = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(80), Constraint::Percentage(20)])
                .split(area);
            f.render_widget(header, split[0]);
            list_draw(f, app, split[1], "Choose", actions, *sel);
        }
        Screen::Running { .. } => {}
    }

    if is_running {
        let label = if let Screen::Running { label, .. } = &app.screen {
            label.as_str()
        } else {
            ""
        };
        let spin_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        let spin = spin_chars[tick as usize % spin_chars.len()];
        let sync_line = app.sync_progress.as_ref().and_then(|rx| {
            let (a, m) = *rx.borrow();
            if a > 0 {
                Some(format!("Sync attempt {a}/{m}"))
            } else {
                None
            }
        });
        let running_block = Block::default()
            .borders(Borders::ALL)
            .title(Span::styled("Output", Style::default().fg(Color::Cyan)));
        let body = Paragraph::new(vec![
            Line::from(Span::styled(
                SPLASH_BRAND,
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(vec![
                Span::styled(spin, Style::default().fg(Color::Green)),
                Span::raw("  "),
                Span::styled(label, Style::default().fg(Color::White)),
            ]),
            Line::from(""),
            Line::from(match &sync_line {
                Some(s) => Span::styled(s.as_str(), Style::default().fg(Color::Yellow)),
                None => Span::styled(
                    "Running wallet kernel…",
                    Style::default().fg(Color::DarkGray),
                ),
            }),
        ])
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true })
        .block(running_block);
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
    } else if app.panel_focus == PanelFocus::Output {
        Line::from(vec![
            Span::styled("↑/↓ j/k ", Style::default().fg(Color::Yellow)),
            Span::raw("scroll output  "),
            Span::styled("PgUp/PgDn ", Style::default().fg(Color::DarkGray)),
            Span::raw("page  "),
            Span::styled("Enter ", Style::default().fg(Color::Yellow)),
            Span::raw("menu  "),
            Span::styled("Tab ", Style::default().fg(Color::DarkGray)),
            Span::raw("menu"),
        ])
    } else {
        let parts = vec![
            Span::styled("↑/↓ ", Style::default().fg(Color::DarkGray)),
            Span::raw("menu  "),
            Span::styled("Enter ", Style::default().fg(Color::DarkGray)),
            Span::raw("select  "),
            Span::styled("Tab ", Style::default().fg(Color::Yellow)),
            Span::raw("output  "),
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

fn draw_create_tx(
    f: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    w: &super::create_tx::CreateTxWizard,
    tick: u64,
) {
    let spin = ["|", "/", "-", "\\"][tick as usize % 4];
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            w.title_line(),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];
    if let Some(s) = &w.status {
        lines.push(Line::from(Span::styled(
            s.clone(),
            Style::default().fg(Color::Red),
        )));
        lines.push(Line::from(""));
    }
    match &w.phase {
        Phase::Recipients { list, sub } => {
            lines.push(Line::from(format!("Recipients added: {}", list.len())));
            match sub {
                RecSub::Address { line } => {
                    lines.push(Line::from("Recipient address (empty when done):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::Amount { addr, line } => {
                    lines.push(Line::from(format!("Address: {addr}")));
                    lines.push(Line::from("Amount (>0):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::Memo { addr, amount, line } => {
                    lines.push(Line::from(format!("{addr}  amount={amount}")));
                    lines.push(Line::from("Memo (optional):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::Blob {
                    addr,
                    amount,
                    memo,
                    line,
                } => {
                    lines.push(Line::from(format!(
                        "{addr}  amount={amount}  memo={:?}",
                        memo
                    )));
                    lines.push(Line::from("Blob path (optional):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::AddAnother { sel } => {
                    lines.push(Line::from("Add another recipient?"));
                    list_inline(&mut lines, BOOL, *sel);
                }
            }
        }
        Phase::Options { sub, .. } => match sub {
            OptSub::Names { line } => {
                lines.push(Line::from("Manual note names (comma-separated, optional):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::Fee { line } => {
                lines.push(Line::from("Fee override (empty for auto):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::AllowLowFee { sel } => {
                lines.push(Line::from("Allow fee below estimated minimum?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::Refund { line } => {
                lines.push(Line::from("Refund PKH (optional):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::Index { line } => {
                lines.push(Line::from("Signing key index (optional):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::Hardened { sel } => {
                lines.push(Line::from("Hardened signing key?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::IncludeData { sel } => {
                lines.push(Line::from("Include note data in output?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::SignKeys { line } => {
                lines.push(Line::from("Extra --sign-key entries (comma-separated):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::SaveRaw { sel } => {
                lines.push(Line::from("Save raw tx jam (debug)?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::NoteSelection { sel } => {
                lines.push(Line::from(
                    "Note selection order — Enter submits transaction",
                ));
                list_inline(&mut lines, NOTE_ORDER, *sel);
                lines.push(Line::from(format!("  {spin} Ready to plan & execute")));
            }
        },
    }
    let p = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

pub(super) fn log_help(verbose: bool) {
    info!(
        "REPL help: use Wallet menu or quick commands (help, exit, menu). \
         Pass --verbose or set RUST_LOG for more detail."
    );
    if verbose {
        info!("This session was started with --verbose.");
    }
}

pub(super) fn log_verbose_info() {
    info!("Restart with `nockchain-wallet -v repl` or set RUST_LOG before launch.");
}

fn list_inline(lines: &mut Vec<Line>, items: &[&str], sel: usize) {
    for (i, s) in items.iter().enumerate() {
        let style = if i == sel {
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        lines.push(Line::from(Span::styled(format!("  {s}"), style)));
    }
}
