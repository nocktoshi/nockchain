//! Simple send form (home CTA) — amount + recipient + giant buttons.

use nockapp::NockAppError;
use nockchain_types::common::Hash;

use crate::command::{Commands, NoteSelectionStrategyCli};
use crate::recipient::RecipientSpecToken;
use crate::tui::format::{format_nock_from_nicks, parse_nock_amount_to_nicks};
use crate::recipient::recipient_tokens_to_specs;
use crate::tui::screens::{Screen, SendSimpleFocus, SendSimplePhase};
use crate::tui::view;
use crate::Wallet;
use crate::tui::view::total_assets_nicks;

const NICKS_PER_NOCK: u128 = 65_536;

pub(crate) fn new_screen() -> Screen {
    Screen::SendSimple {
        amount: String::new(),
        recipient: String::new(),
        amount_cursor: 0,
        recipient_cursor: 0,
        focus: SendSimpleFocus::Amount,
        phase: SendSimplePhase::Form,
        status: None,
        review_scroll: 0,
    }
}

/// Plan the transaction (no kernel poke) and return markdown for the review panel.
pub(crate) async fn plan_send_preview(
    wallet: &mut Wallet,
    synced_snapshot: Option<wallet_tx_builder::adapter::NormalizedSnapshot>,
    cmd: &Commands,
) -> Result<String, NockAppError> {
    let Commands::CreateTx {
        names,
        recipients,
        fee,
        allow_low_fee,
        refund_pkh,
        index,
        hardened,
        include_data,
        sign_keys,
        save_raw_tx,
        note_selection_strategy,
    } = cmd
    else {
        return Err(nockapp::NockAppError::OtherError(
            "expected CreateTx command".into(),
        ));
    };
    let recipient_specs = recipient_tokens_to_specs(recipients.clone())?;
    let signing_keys = Wallet::collect_signing_keys(*index, *hardened, sign_keys)?;
    let send_nicks = recipients
        .iter()
        .filter_map(|r| match r {
            RecipientSpecToken::P2pkh { amount, .. } => Some(*amount),
            _ => None,
        })
        .sum::<u64>();
    let planned = wallet
        .plan_create_tx_with_planner(
            synced_snapshot,
            names.clone(),
            *fee,
            recipient_specs,
            *allow_low_fee,
            refund_pkh.clone(),
            signing_keys,
            *include_data,
            *save_raw_tx,
            *note_selection_strategy,
        )
        .await?;
    Ok(view::render_create_tx_plan_preview(
        &planned.plan,
        recipients
            .iter()
            .find_map(|r| match r {
                RecipientSpecToken::P2pkh { address, .. } => Some(address.as_str()),
                _ => None,
            })
            .unwrap_or(""),
        send_nicks,
        &planned.block_id_b58,
        planned.height,
    ))
}

pub(crate) fn build_create_tx_command(
    amount: &str,
    recipient: &str,
) -> Result<Commands, String> {
    let nicks = parse_nock_amount_to_nicks(amount)?;
    let addr = recipient.trim().to_string();
    if addr.is_empty() {
        return Err("Receiver address is required".into());
    }
    if Hash::from_base58(&addr).is_err() {
        return Err("Invalid Nockchain address (base58)".into());
    }
    Ok(Commands::CreateTx {
        names: None,
        recipients: vec![RecipientSpecToken::P2pkh {
            address: addr,
            amount: nicks,
            memo: None,
            blob: None,
        }],
        fee: None,
        allow_low_fee: false,
        refund_pkh: None,
        index: None,
        hardened: false,
        include_data: true,
        sign_keys: Vec::new(),
        save_raw_tx: false,
        note_selection_strategy: NoteSelectionStrategyCli::Ascending,
    })
}

pub(crate) fn spendable_balance_line(events: &[crate::wallet_outcome::WalletEvent]) -> String {
    match total_assets_nicks(events) {
        Some(n) => format!("Spendable: {}", format_nock_from_nicks(n as u128)),
        None => "Spendable: —".to_string(),
    }
}

pub(crate) fn max_amount_string(events: &[crate::wallet_outcome::WalletEvent]) -> Option<String> {
    total_assets_nicks(events).map(|n| {
        let nocks = n as f64 / NICKS_PER_NOCK as f64;
        format_nock_display(nocks)
    })
}

fn format_nock_display(nocks: f64) -> String {
    let mut s = format!("{nocks:.8}");
    while s.contains('.') && (s.ends_with('0') || s.ends_with('.')) {
        s.pop();
    }
    s
}
