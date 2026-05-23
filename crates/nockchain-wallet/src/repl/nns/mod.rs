//! REPL-only NNS (`.nock` name) registration helpers.

use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

use crate::recipient::RecipientSpecToken;

/// NNS registry payee from [nockchain#116](https://github.com/nockchain/nockchain/pull/116).
pub(crate) const REGISTRY_P2PKH: &str = "8s29XUK8Do7QWt2MHfPdd1gDSta6db4c3bQrxP1YdJNfXpL3WPzTT5";

const NOCKNAMES_SEARCH_URL: &str = "https://api.nocknames.com/search";
const NICKS_PER_NOCK: u64 = 65_536;

#[derive(Debug, Deserialize)]
struct SearchResponse {
    name: String,
    price: Option<u64>,
    status: String,
}

/// Normalize user input (`myname` or `myname.nock`) to canonical `{stem}.nock`.
pub(crate) fn normalize_nns_name(raw: &str) -> Result<String, String> {
    let t = raw.trim().to_ascii_lowercase();
    if t.is_empty() {
        return Err("Name cannot be empty".into());
    }
    let stem = t.strip_suffix(".nock").unwrap_or(&t);
    if stem.is_empty() {
        return Err("Invalid name".into());
    }
    if stem.len() > 63 {
        return Err("Name stem must be at most 63 characters".into());
    }
    if !stem
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("Name may only contain lowercase letters, digits, and hyphens".into());
    }
    if stem.starts_with('-') || stem.ends_with('-') {
        return Err("Name cannot start or end with a hyphen".into());
    }
    Ok(format!("{stem}.nock"))
}

/// Fee tier in **nicks** from stem length ([nns.id](https://nns.id/) tiers).
pub(crate) fn fee_nicks_for_stem(stem: &str) -> u64 {
    let len = stem.len();
    let nocks: u64 = if len <= 4 {
        5000
    } else if len <= 9 {
        500
    } else {
        100
    };
    nocks.saturating_mul(NICKS_PER_NOCK)
}

pub(crate) fn claim_blob_for_name(canonical_name: &str) -> String {
    format!("nns/v1/claim/{canonical_name}")
}

pub(crate) fn build_registry_recipient(canonical_name: &str) -> Result<RecipientSpecToken, String> {
    let stem = canonical_name
        .strip_suffix(".nock")
        .ok_or_else(|| "expected .nock suffix".to_string())?;
    let fee = fee_nicks_for_stem(stem);
    Ok(RecipientSpecToken::P2pkh {
        address: REGISTRY_P2PKH.to_string(),
        amount: fee,
        memo: None,
        blob: Some(claim_blob_for_name(canonical_name)),
    })
}

/// Returns `Ok(())` when the name appears available; `Err` with user-facing message otherwise.
pub(crate) async fn ensure_name_available(canonical_name: &str) -> Result<(), String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .get(NOCKNAMES_SEARCH_URL)
        .query(&[("name", canonical_name)])
        .send()
        .await
        .map_err(|e| format!("Name lookup failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "Name lookup returned {} (try again later)",
            resp.status()
        ));
    }
    let body: SearchResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid lookup response: {e}"))?;
    let status = body.status.to_ascii_lowercase();
    if status.contains("available") || status == "free" {
        return Ok(());
    }
    if status.contains("register") || status.contains("taken") || status.contains("pending") {
        return Err(format!(
            "Name `{}` is not available (status: {})",
            body.name, body.status
        ));
    }
    Ok(())
}

pub(crate) fn preview_lines(canonical_name: &str) -> Vec<String> {
    let stem = canonical_name
        .strip_suffix(".nock")
        .unwrap_or(canonical_name);
    let fee = fee_nicks_for_stem(stem);
    let blob = claim_blob_for_name(canonical_name);
    vec![
        format!("Name: {canonical_name}"),
        format!("Registry: {REGISTRY_P2PKH}"),
        format!("Fee: {fee} nicks"),
        format!("Blob: {blob}"),
    ]
}

pub(crate) fn schedule_create_tx_command(
    recipient: RecipientSpecToken,
) -> crate::command::Commands {
    crate::command::Commands::CreateTx {
        names: None,
        recipients: vec![recipient],
        fee: None,
        allow_low_fee: false,
        refund_pkh: None,
        index: None,
        hardened: false,
        include_data: true,
        sign_keys: Vec::new(),
        save_raw_tx: false,
        note_selection_strategy: crate::command::NoteSelectionStrategyCli::Ascending,
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_accepts_bare_stem() {
        assert_eq!(normalize_nns_name("alice").unwrap(), "alice.nock");
    }

    #[test]
    fn normalize_accepts_suffix() {
        assert_eq!(normalize_nns_name("bob.nock").unwrap(), "bob.nock");
    }

    #[test]
    fn fee_tiers() {
        assert_eq!(fee_nicks_for_stem("a"), 5000 * 65_536);
        assert_eq!(fee_nicks_for_stem("abcde"), 500 * 65_536);
        assert_eq!(fee_nicks_for_stem("abcdefghij"), 100 * 65_536);
    }

    #[test]
    fn claim_blob_format() {
        assert_eq!(claim_blob_for_name("foo.nock"), "nns/v1/claim/foo.nock");
    }

    #[test]
    fn recipient_has_blob_not_memo() {
        let r = build_registry_recipient("x.nock").unwrap();
        match r {
            RecipientSpecToken::P2pkh {
                blob, memo, amount, ..
            } => {
                assert!(memo.is_none());
                assert_eq!(blob.as_deref(), Some("nns/v1/claim/x.nock"));
                assert_eq!(amount, 5000 * 65_536);
            }
            _ => panic!("expected p2pkh"),
        }
    }
}
