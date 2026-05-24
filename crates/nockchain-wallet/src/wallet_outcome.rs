//! Structured success payload from [`crate::dispatch::execute_wallet_command`].
//!
//! **JSON-serializable [`WalletEvent`]s** are the canonical machine contract (TUI, HTTP, tests).
//! Kernel emits `[%wallet kind [%v1 …]]`; CLI one-shot commands keep `%markdown` only.
//!
//! ## Version coupling (`nockchain-wallet` ↔ `nockchain-wallet-tui`)
//!
//! - [`WALLET_OUTCOME_SCHEMA`] labels the top-level JSON envelope (`wallet-outcome-v1` today).
//! - Each [`WalletEvent`] variant carries its own `*_v1` tag in serde (e.g. `balance_v1`, `notes_list_v1`).
//! - Kernel wire tags use `[%wallet <kind> [%v1 …]]` (kinds: `%balance`, `%notes`, …).
//!

use nockapp::NockAppError;
use serde::{Deserialize, Serialize};

use crate::create_tx::MigrateV0NotesSummary;

/// Top-level JSON schema id for [`WalletCommandJsonResponse`] / HTTP API bodies.
pub const WALLET_OUTCOME_SCHEMA: &str = "wallet-outcome-v1";

/// One row in [`WalletEvent::NotesListV1`], from `[%wallet-effect %notes [%v1 …]]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletNoteRowV1 {
    pub name_first_b58: String,
    pub name_last_b58: String,
    /// `0` = v0 note, `1` = v1 note (matches Hoon `?^  -.note  0  1`).
    pub version: u64,
    pub assets: u64,
}

/// One signer row in [`WalletEvent::MigrateSummaryV1`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletMigrateSignerRowV1 {
    pub label: String,
    pub address_b58: String,
    pub version: u64,
    pub note_count: usize,
    pub selected_total: u64,
    pub fee: Option<u64>,
    pub migrated_amount: Option<u64>,
    pub tx_path: Option<String>,
    pub skip_reason: Option<String>,
}

/// One address row in [`WalletEvent::AddressListV1`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletAddressRowV1 {
    pub address_b58: String,
    pub version: u64,
}

/// One node in [`WalletEvent::KeyTreeV1`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletKeyTreeNodeV1 {
    pub path: String,
    pub label: String,
    pub pubkey_b58: Option<String>,
}

/// Keygen / derive structured summary (no secrets).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalletKeygenV1 {
    pub message: String,
    pub paths: Vec<String>,
    pub pubkeys_b58: Vec<String>,
}

/// Kernel-emitted structured unit (`[%wallet-effect kind [%v1 …]]`). TUI view layer renders these.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WalletEvent {
    /// `[%wallet-effect %balance [%v1 …]]`.
    BalanceSnapshotV1 {
        wallet_version: u64,
        block_id_b58: String,
        height: u64,
        note_count: u64,
        total_assets: u64,
    },
    /// `[%wallet-effect %notes [%v1 …]]` or `[%wallet-effect %notes-advanced [%v1 …]]`.
    NotesListV1 {
        height: u64,
        block_id_b58: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        filter_address: Option<String>,
        rows: Vec<WalletNoteRowV1>,
    },
    /// `[%wallet-effect %addresses [%v1 …]]`.
    AddressListV1 {
        list_kind: String,
        rows: Vec<WalletAddressRowV1>,
    },
    /// `[%wallet-effect %key-tree [%v1 …]]`.
    KeyTreeV1 {
        include_values: bool,
        nodes: Vec<WalletKeyTreeNodeV1>,
    },
    /// `[%wallet-effect %keygen [%v1 …]]`.
    KeygenV1(WalletKeygenV1),
    /// Rust-built migrate-v0-notes summary.
    MigrateSummaryV1 {
        destination: String,
        block_id: String,
        height: u64,
        examined_signers: usize,
        created_count: usize,
        skipped_count: usize,
        signers: Vec<WalletMigrateSignerRowV1>,
    },
    /// Public-node transaction acceptance query.
    TxAcceptedV1 { tx_id: String, accepted: bool },
    /// TUI NNS name registration create-tx outcome.
    NnsRegistrationV1 {
        name: String,
        fee_nicks: u64,
        blob: String,
        tx_paths: Vec<String>,
    },
    /// Kernel `%markdown` from create-tx (saved path + transaction display).
    CreateTxV1 {
        tx_paths: Vec<String>,
        summary: String,
    },
}

/// Machine-readable command result (TUI store, HTTP API).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletCommandData {
    pub events: Vec<WalletEvent>,
}

pub type WalletCommandOutcome = Result<WalletCommandData, NockAppError>;

/// Wire/API envelope shared by TUI JSON tooling and the HTTP command endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletCommandJsonResponse {
    pub schema_version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<WalletCommandData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl WalletCommandJsonResponse {
    pub fn from_outcome(outcome: WalletCommandOutcome) -> Self {
        match outcome {
            Ok(data) => Self {
                schema_version: WALLET_OUTCOME_SCHEMA,
                success: Some(data),
                error: None,
            },
            Err(e) => Self {
                schema_version: WALLET_OUTCOME_SCHEMA,
                success: None,
                error: Some(e.to_string()),
            },
        }
    }
}

pub fn migrate_summary_event(summary: &MigrateV0NotesSummary) -> WalletEvent {
    WalletEvent::MigrateSummaryV1 {
        destination: summary.destination.clone(),
        block_id: summary.block_id.clone(),
        height: summary.height,
        examined_signers: summary.examined_signers,
        created_count: summary.created_count,
        skipped_count: summary.skipped_count,
        signers: summary
            .signers
            .iter()
            .map(|s| WalletMigrateSignerRowV1 {
                label: crate::create_tx::migrate_signer_label(&s.signer),
                address_b58: s.signer.address_b58.clone(),
                version: s.signer.version,
                note_count: s.note_count,
                selected_total: s.selected_total,
                fee: s.fee,
                migrated_amount: s.migrated_amount,
                tx_path: s.tx_path.clone(),
                skip_reason: s.skip_reason.clone(),
            })
            .collect(),
    }
}

pub fn tx_accepted_markdown(tx_id: &str, accepted: bool) -> String {
    let status_line = if accepted {
        "- status: **accepted by node**"
    } else {
        "- status: **not yet accepted**"
    };

    [
        "## Transaction Acceptance".to_string(),
        format!("- tx id: `{}`", tx_id),
        status_line.to_string(),
    ]
    .join("\n")
}

pub fn tx_accepted_event(tx_id: &str, accepted: bool) -> WalletEvent {
    WalletEvent::TxAcceptedV1 {
        tx_id: tx_id.to_string(),
        accepted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wallet_event_json_roundtrip() {
        let event = WalletEvent::BalanceSnapshotV1 {
            wallet_version: 1,
            block_id_b58: "abc".into(),
            height: 42,
            note_count: 3,
            total_assets: 65536,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let back: WalletEvent = serde_json::from_str(&json).expect("deserialize");
        assert!(matches!(
            back,
            WalletEvent::BalanceSnapshotV1 { height: 42, .. }
        ));
    }

    #[test]
    fn wallet_command_json_response_ok() {
        let resp = WalletCommandJsonResponse::from_outcome(Ok(WalletCommandData {
            events: vec![WalletEvent::TxAcceptedV1 {
                tx_id: "tx".into(),
                accepted: true,
            }],
        }));
        assert_eq!(resp.schema_version, WALLET_OUTCOME_SCHEMA);
        assert!(resp.success.is_some());
        assert!(resp.error.is_none());
        let success = resp.success.unwrap();
        assert_eq!(success.events.len(), 1);
    }
}
