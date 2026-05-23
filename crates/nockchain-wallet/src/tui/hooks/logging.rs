//! Side-effect logging invoked from keyboard handlers (help / verbose hints).

use tracing::info;

pub(crate) fn log_help(verbose: bool) {
    info!(
        "TUI help: use Wallet menu or quick commands (help, exit, menu). \
         Pass --verbose or set RUST_LOG for more detail."
    );
    if verbose {
        info!("This session was started with --verbose.");
    }
}

pub(crate) fn log_verbose_info() {
    info!("Restart with `nockchain-wallet -v tui` or set RUST_LOG before launch.");
}
