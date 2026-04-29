//! Structured success payload from [`crate::dispatch::execute_wallet_command`] (Phase 2 decoupling).
//!
//! Callers may ignore [`WalletSuccess`] fields until HTTP / richer TUI wiring consumes events.

#![allow(dead_code)]

use nockapp::NockAppError;

/// One kernel-emitted unit of output. Extend with balance rows, note lists, etc. as the kernel
/// gains structured effects.
#[derive(Debug, Clone)]
pub(crate) enum WalletEvent {
    /// Raw markdown cord from a `%markdown` effect (before any terminal skin).
    KernelMarkdown { raw: String },
}

/// Successful command completion: presentation layers choose how to render [`WalletEvent`]s.
#[derive(Debug, Clone)]
pub(crate) struct WalletSuccess {
    pub events: Vec<WalletEvent>,
    /// Same bytes the legacy capture driver concatenated (`\n\n` between effects).
    pub raw_markdown: String,
}

impl WalletSuccess {
    pub(crate) fn empty() -> Self {
        Self {
            events: Vec::new(),
            raw_markdown: String::new(),
        }
    }
}

pub(crate) type WalletCommandOutcome = Result<WalletSuccess, NockAppError>;
