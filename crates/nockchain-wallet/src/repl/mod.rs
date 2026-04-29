//! Interactive REPL (ratatui + crossterm).
//!
//! Layout: **dispatch** / [`crate::Wallet`] / CLI stay independent of this tree; the REPL calls
//! [`crate::dispatch::execute_wallet_command`] only through [`command_runner`].

mod app_state;
mod command_runner;
mod components;
mod create_tx;
mod ct_dispatch;
mod handlers;
mod hooks;
mod paste;
mod screens;
mod store;
mod tui;

use std::sync::Arc;

use command_runner::ReplRuntime;
use nockapp::NockAppError;
use tokio::sync::Mutex;
use wallet_tx_builder::adapter::NormalizedSnapshot;

use crate::command::WalletCli;
use crate::Wallet;

/// Normalize optional leading `/` for slash-style commands (e.g. `/help` → `help`).
pub(crate) fn normalize_slash_cmd(line: &str) -> &str {
    let t = line.trim();
    t.strip_prefix('/').unwrap_or(t).trim()
}

/// Main REPL entry: full-screen TUI.
pub async fn run(
    cli: &WalletCli,
    wallet: Wallet,
    synced_snapshot_for_planner: Option<NormalizedSnapshot>,
) -> Result<(), NockAppError> {
    let wallet = Arc::new(Mutex::new(wallet));
    let snapshot = Arc::new(Mutex::new(synced_snapshot_for_planner));
    let rt = ReplRuntime {
        wallet: Arc::clone(&wallet),
        snapshot: Arc::clone(&snapshot),
        cli: cli.clone(),
        markdown_sink: Arc::new(std::sync::Mutex::new(String::new())),
    };
    tui::run_tui(cli.clone(), rt).await
}

#[cfg(test)]
mod tests {
    use super::normalize_slash_cmd;

    #[test]
    fn slash_normalization() {
        assert_eq!(normalize_slash_cmd("/help"), "help");
        assert_eq!(normalize_slash_cmd("  /exit  "), "exit");
        assert_eq!(normalize_slash_cmd("verbose"), "verbose");
    }
}
