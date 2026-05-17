//! Structured success payload from [`crate::dispatch::execute_wallet_command`] (Phase 2 decoupling).
//!
//! Callers may ignore [`WalletSuccess`] fields until HTTP / richer TUI wiring consumes events.

#![allow(dead_code)]

use nockapp::NockAppError;

/// One row in [`WalletEvent::NotesListV1`], from `[%raw [%wnote-v1 ...]]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalletNoteRowV1 {
    pub name_first_b58: String,
    pub name_last_b58: String,
    /// `0` = v0 note, `1` = v1 note (matches Hoon `?^  -.note  0  1`).
    pub version: u64,
    pub assets: u64,
}

/// One kernel-emitted unit of output. Extend with balance rows, note lists, etc. as the kernel
/// gains structured effects.
#[derive(Debug, Clone)]
pub(crate) enum WalletEvent {
    /// Raw markdown cord from a `%markdown` effect (before any terminal skin).
    KernelMarkdown { raw: String },
    /// `[%raw [%wbal-v1 ...]]` — additive alongside `%markdown` for `show-balance`.
    BalanceSnapshotV1 {
        wallet_version: u64,
        block_id_b58: String,
        height: u64,
        note_count: u64,
        total_assets: u64,
    },
    /// `[%raw [%wnote-v1 ...]]` — additive alongside `%markdown` for `list-notes`.
    NotesListV1 {
        height: u64,
        block_id_b58: String,
        rows: Vec<WalletNoteRowV1>,
    },
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
