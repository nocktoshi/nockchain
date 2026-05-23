//! Shared formatting helpers for the REPL view layer.

const NICKS_PER_NOCK: u128 = 65_536;

/// Convert a nick count to a human NOCK amount (`65536` nicks = `1` NOCK).
pub(crate) fn format_nock_from_nicks(nicks: u128) -> String {
    let n = nicks as f64 / NICKS_PER_NOCK as f64;
    let mut s = format!("{n:.8}");
    while s.contains('.') && (s.ends_with('0') || s.ends_with('.')) {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    format!("{s} NOCK")
}
