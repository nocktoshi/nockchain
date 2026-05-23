//! REPL view layer: render structured [`WalletEvent`]s for TUI panels (no kernel markdown).

use super::format::format_nock_from_nicks;
use crate::wallet_outcome::{
    WalletEvent, WalletKeygenV1, WalletMigrateSignerRowV1, WalletNoteRowV1,
};

const NICKS_PER_NOCK: u128 = 65_536;

pub(crate) const NO_STRUCTURED_OUTPUT: &str =
    "No structured output for this command yet.\n\nKernel `%markdown` is not used in the REPL; \
     extend `[%raw …]` effects in the wallet kernel for machine-readable output.";

/// Render events for the output panel (plain text, no termimad).
pub(crate) fn render_events_for_output(events: &[WalletEvent]) -> String {
    if events.is_empty() {
        return NO_STRUCTURED_OUTPUT.to_string();
    }
    let parts: Vec<String> = events.iter().map(render_one_event).collect();
    parts.join("\n\n")
}

/// Compact balance sidebar text from events (falls back to full output render).
pub(crate) fn render_balance_sidebar(events: &[WalletEvent]) -> String {
    for event in events {
        if let WalletEvent::BalanceSnapshotV1 {
            wallet_version,
            block_id_b58,
            height,
            note_count,
            total_assets,
        } = event
        {
            return render_balance_snapshot(
                *wallet_version, block_id_b58, *height, *note_count, *total_assets,
            );
        }
    }
    render_events_for_output(events)
}

fn render_one_event(event: &WalletEvent) -> String {
    match event {
        WalletEvent::BalanceSnapshotV1 {
            wallet_version,
            block_id_b58,
            height,
            note_count,
            total_assets,
        } => render_balance_snapshot(
            *wallet_version, block_id_b58, *height, *note_count, *total_assets,
        ),
        WalletEvent::NotesListV1 {
            height,
            block_id_b58,
            filter_address,
            rows,
        } => render_notes_list(*height, block_id_b58, filter_address.as_deref(), rows),
        WalletEvent::AddressListV1 { list_kind, rows } => render_address_list(list_kind, rows),
        WalletEvent::KeyTreeV1 {
            include_values,
            nodes,
        } => render_key_tree(*include_values, nodes),
        WalletEvent::KeygenV1(k) => render_keygen(k),
        WalletEvent::MigrateSummaryV1 {
            destination,
            block_id,
            height,
            examined_signers,
            created_count,
            skipped_count,
            signers,
        } => render_migrate_summary(
            destination, block_id, *height, *examined_signers, *created_count, *skipped_count,
            signers,
        ),
        WalletEvent::TxAcceptedV1 { tx_id, accepted } => render_tx_accepted(tx_id, *accepted),
        WalletEvent::NnsRegistrationV1 {
            name,
            fee_nicks,
            blob,
            tx_paths,
        } => render_nns_registration(name, *fee_nicks, blob, tx_paths),
    }
}

fn render_balance_snapshot(
    wallet_version: u64,
    block_id_b58: &str,
    height: u64,
    note_count: u64,
    total_assets: u64,
) -> String {
    [
        "## Balance".to_string(),
        format!("- wallet version: {wallet_version}"),
        format!("- block: {block_id_b58}"),
        format!("- height: {height}"),
        format!("- notes: {note_count}"),
        format!(
            "- total: {} ({total_assets} nicks)",
            format_nock_from_nicks(total_assets as u128)
        ),
    ]
    .join("\n")
}

fn render_notes_list(
    height: u64,
    block_id_b58: &str,
    filter_address: Option<&str>,
    rows: &[WalletNoteRowV1],
) -> String {
    let mut lines = vec![
        "## Notes".to_string(),
        format!("- height: {height}"),
        format!("- block: {block_id_b58}"),
    ];
    if let Some(addr) = filter_address {
        lines.push(format!("- filter address: {addr}"));
    }
    lines.push(format!("- count: {}", rows.len()));
    lines.push(String::new());
    lines.push("| version | assets (NOCK) | name first | name last |".to_string());
    lines.push("| --- | --- | --- | --- |".to_string());
    for row in rows {
        let nock = format_nock_from_nicks(row.assets as u128);
        lines.push(format!(
            "| {} | {} | {} | {} |",
            row.version, nock, row.name_first_b58, row.name_last_b58
        ));
    }
    lines.join("\n")
}

fn render_address_list(
    list_kind: &str,
    rows: &[crate::wallet_outcome::WalletAddressRowV1],
) -> String {
    let mut lines = vec![
        format!("## Addresses ({list_kind})"),
        format!("- count: {}", rows.len()),
        String::new(),
    ];
    for row in rows {
        lines.push(format!("- v{}: {}", row.version, row.address_b58));
    }
    lines.join("\n")
}

fn render_key_tree(
    include_values: bool,
    nodes: &[crate::wallet_outcome::WalletKeyTreeNodeV1],
) -> String {
    let mut lines = vec![
        "## Key tree".to_string(),
        format!("- include values: {include_values}"),
        String::new(),
    ];
    for node in nodes {
        if let Some(pk) = &node.pubkey_b58 {
            lines.push(format!("- {} ({}) → {}", node.path, node.label, pk));
        } else {
            lines.push(format!("- {} ({})", node.path, node.label));
        }
    }
    lines.join("\n")
}

fn render_keygen(k: &WalletKeygenV1) -> String {
    let mut lines = vec![format!("## {}", k.message), String::new()];
    for (path, pk) in k.paths.iter().zip(k.pubkeys_b58.iter()) {
        lines.push(format!("- {path}: {pk}"));
    }
    lines.join("\n")
}

fn render_migrate_summary(
    destination: &str,
    block_id: &str,
    height: u64,
    examined_signers: usize,
    created_count: usize,
    skipped_count: usize,
    signers: &[WalletMigrateSignerRowV1],
) -> String {
    let mut lines = vec![
        "## V0 Migration Sweep".to_string(),
        format!("- destination: {destination}"),
        format!("- block id: {block_id}"),
        format!("- height: {height}"),
        format!("- active signing keys examined: {examined_signers}"),
        format!("- migration txs created: {created_count}"),
        format!("- signing keys skipped: {skipped_count}"),
    ];
    if created_count == 0 {
        lines.push(
            "- batch create poke: not emitted because every signer bucket was skipped".to_string(),
        );
    }
    for signer in signers {
        lines.push(String::new());
        lines.push(format!("### {}", signer.label));
        lines.push(format!("- signer address: {}", signer.address_b58));
        lines.push(format!("- signer version: {}", signer.version));
        lines.push(format!("- selected notes: {}", signer.note_count));
        lines.push(format!(
            "- selected total: {}",
            format_nock_from_nicks(signer.selected_total as u128)
        ));
        match (&signer.migrated_amount, &signer.tx_path) {
            (Some(migrated_amount), Some(tx_path)) => {
                lines.push("- result: created".to_string());
                if let Some(fee) = signer.fee {
                    lines.push(format!("- fee: {}", format_nock_from_nicks(fee as u128)));
                }
                lines.push(format!(
                    "- migrated amount: {}",
                    format_nock_from_nicks(*migrated_amount as u128)
                ));
                lines.push(format!("- tx path: {tx_path}"));
                lines.push(format!(
                    "- submit with: nockchain-wallet send-tx \"{tx_path}\""
                ));
            }
            _ => {
                lines.push("- result: skipped".to_string());
                if let Some(fee) = signer.fee {
                    lines.push(format!(
                        "- fee estimate: {}",
                        format_nock_from_nicks(fee as u128)
                    ));
                }
                if let Some(reason) = &signer.skip_reason {
                    lines.push(format!("- skip reason: {reason}"));
                }
            }
        }
    }
    lines.join("\n")
}

fn render_tx_accepted(tx_id: &str, accepted: bool) -> String {
    let status = if accepted {
        "accepted by node"
    } else {
        "not yet accepted"
    };
    [
        "## Transaction Acceptance".to_string(),
        format!("- tx id: {tx_id}"),
        format!("- status: {status}"),
    ]
    .join("\n")
}

fn render_nns_registration(name: &str, fee_nicks: u64, blob: &str, tx_paths: &[String]) -> String {
    let fee_nocks = fee_nicks as f64 / NICKS_PER_NOCK as f64;
    let mut lines = vec![
        "## NNS name registration".to_string(),
        format!("- name: {name}"),
        format!("- fee: {fee_nicks} nicks (~{fee_nocks:.4} NOCK)"),
        format!("- blob: {blob}"),
    ];
    if tx_paths.is_empty() {
        lines.push("- tx: (pending create-tx)".to_string());
    } else {
        for path in tx_paths {
            lines.push(format!("- tx path: {path}"));
        }
    }
    lines.join("\n")
}
