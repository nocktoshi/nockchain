//! Ratatui terminal: async event loop and suspend/resume around wallet I/O.
//!
//! **Input:** key events are read on a background thread and sent over an unbounded channel;
//! `tokio::select!` merges them with a tick for spinners. **Paste:** bracketed paste mode is
//! enabled so terminals emit `Event::Paste` with full clipboard text (needed for address fields
//! and other line editors). Drawing lives in [`super::components`].

use std::io::{self, stdout};
use std::time::Duration;

use crossterm::event::{EnableBracketedPaste, Event, KeyEventKind};
use crossterm::terminal::{enable_raw_mode, EnterAlternateScreen};
use crossterm::ExecutableCommand;
use nockapp::NockAppError;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

use super::command_runner::{self, BalanceRefreshCompletion, JobCompletion, ReplRuntime};
use super::store::{UIStore, UiAction};
use super::components::root::draw_ui;
use super::handlers;
use super::hooks::events::spawn_crossterm_channel;
use super::hooks::terminal::{restore_terminal, Term};
use super::screens::Screen;
use crate::command::WalletCli;

pub(crate) fn io_err(e: io::Error) -> NockAppError {
    NockAppError::OtherError(format!("terminal io: {e}"))
}

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

    let mut ev_rx = spawn_crossterm_channel();

    let (job_done_tx, mut job_done_rx) = mpsc::unbounded_channel::<JobCompletion>();
    let (balance_done_tx, mut balance_done_rx) =
        mpsc::unbounded_channel::<BalanceRefreshCompletion>();

    let mut store = UIStore::new(Screen::Splash);
    let mut interval = tokio::time::interval(Duration::from_millis(120));

    let result = loop {
        {
            let mut term_guard = terminal.lock().await;
            term_guard
                .draw(|f| draw_ui(f, &mut store.state))
                .map_err(io_err)?;
        }

        tokio::select! {
            biased;
            maybe_job = job_done_rx.recv() => {
                if let Some((res, captured)) = maybe_job {
                    command_runner::apply_job_result(&mut store, res, captured);
                }
            }
            maybe_bal = balance_done_rx.recv() => {
                if let Some((nonce, res, captured)) = maybe_bal {
                    command_runner::apply_balance_sidebar_result(&mut store, nonce, res, captured);
                }
            }
            _ = interval.tick() => {
                store.dispatch(UiAction::Tick);
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
                            &mut store,
                            key,
                            &terminal,
                            &job_done_tx,
                            &balance_done_tx,
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
                        match handlers::dispatch_paste(&cli, &mut store, text, &rt, &balance_done_tx)
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
                    _ => {}
                }
            }
        }
    };

    let mut term_guard = terminal.lock().await;
    let _ = restore_terminal(&mut term_guard);
    result
}
