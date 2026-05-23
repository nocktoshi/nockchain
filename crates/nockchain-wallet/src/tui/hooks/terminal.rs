//! Alternate screen, raw mode, bracketed paste, and restore.

use std::io::{self, stdout, Stdout};

use crossterm::event::DisableBracketedPaste;
use crossterm::terminal::{disable_raw_mode, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

pub(crate) type Term = Terminal<CrosstermBackend<Stdout>>;

pub(crate) fn restore_terminal(terminal: &mut Term) -> io::Result<()> {
    let _ = stdout().execute(DisableBracketedPaste);
    disable_raw_mode()?;
    terminal.show_cursor()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
