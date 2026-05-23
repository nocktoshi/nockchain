//! REPL-only NNS (`.nock` name) registration helpers.

use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

use crate::recipient::RecipientSpecToken;
use crate::repl::components::price::format_usd_total;
use crate::repl::format::format_nock_from_nicks;

/// NNS registry payee from [nockchain#116](https://github.com/nockchain/nockchain/pull/116).
pub(crate) const REGISTRY_P2PKH: &str = "8s29XUK8Do7QWt2MHfPdd1gDSta6db4c3bQrxP1YdJNfXpL3WPzTT5";

const NOCKNAMES_SEARCH_URL: &str = "https://api.nocknames.com/search";
const NOCKNAMES_RESOLVE_URL: &str = "https://api.nocknames.com/resolve";
const NICKS_PER_NOCK: u64 = 65_536;

#[derive(Debug, Deserialize)]
struct ResolveByAddressResponse {
    name: String,
}

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

/// Registration fee tier in **NOCK** from stem length ([nns.id](https://nns.id/) tiers).
pub(crate) fn fee_nocks_for_stem(stem: &str) -> u64 {
    let len = stem.len();
    if len <= 4 {
        5000
    } else if len <= 9 {
        500
    } else {
        100
    }
}

/// Chain amount in nicks for the registration fee tier.
pub(crate) fn fee_nicks_for_stem(stem: &str) -> u64 {
    fee_nocks_for_stem(stem).saturating_mul(NICKS_PER_NOCK)
}

/// Format a nick amount as NOCK with optional USD (CoinGecko rate).
pub(crate) fn format_nock_amount_with_usd(nicks: u128, usd_per_nock: Option<f64>) -> String {
    let nock = format_nock_from_nicks(nicks);
    match usd_per_nock.filter(|u| u.is_finite() && *u > 0.0) {
        Some(usd) => {
            let nicks_u64 = u64::try_from(nicks).unwrap_or(u64::MAX);
            format!("{nock} ({})", format_usd_total(nicks_u64, usd))
        }
        None => nock,
    }
}

/// Format a whole-NOCK listing/fee with optional USD.
pub(crate) fn format_nocks_with_usd(nocks: u64, usd_per_nock: Option<f64>) -> String {
    format_nock_amount_with_usd(nocks as u128 * NICKS_PER_NOCK as u128, usd_per_nock)
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

#[derive(Debug, Clone)]
pub(crate) struct NnsLookupOk {
    pub canonical_name: String,
    pub fee_nicks: u64,
    /// Listing price from the registry API, in whole NOCK (when present).
    pub listed_nocks: Option<u64>,
}

/// User-facing availability line for the NNS buy screen.
pub(crate) fn availability_message(ok: &NnsLookupOk, usd_per_nock: Option<f64>) -> String {
    let fee = format_nock_amount_with_usd(ok.fee_nicks as u128, usd_per_nock);
    let listed = ok.listed_nocks.map(|n| {
        format!(
            " · listed {}",
            format_nocks_with_usd(n, usd_per_nock)
        )
    }).unwrap_or_default();
    format!(
        "`{}` is available — registration fee {fee}{listed}",
        ok.canonical_name
    )
}

/// Estimated registration fee while typing (before search).
pub(crate) fn estimated_fee_hint(raw: &str, usd_per_nock: Option<f64>) -> Option<String> {
    let canonical = normalize_nns_name(raw).ok()?;
    let stem = canonical.strip_suffix(".nock")?;
    let fee_nicks = fee_nicks_for_stem(stem);
    Some(format!(
        "Est. registration: {}",
        format_nock_amount_with_usd(fee_nicks as u128, usd_per_nock)
    ))
}

/// Reverse lookup: primary `.nock` name for a wallet address (`GET /resolve?address=`).
pub(crate) async fn resolve_primary_name(address: &str) -> Result<Option<String>, String> {
    let address = address.trim();
    if address.is_empty() {
        return Ok(None);
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .get(NOCKNAMES_RESOLVE_URL)
        .query(&[("address", address)])
        .send()
        .await
        .map_err(|e| format!("Name resolve failed: {e}"))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        return Err(format!(
            "Name resolve returned {} (try again later)",
            resp.status()
        ));
    }
    let body: ResolveByAddressResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid resolve response: {e}"))?;
    let name = body.name.trim();
    if name.is_empty() {
        Ok(None)
    } else {
        Ok(Some(name.to_string()))
    }
}

/// Normalize input, query the registry API, and return availability + fee when free.
pub(crate) async fn lookup_name(raw: &str) -> Result<NnsLookupOk, String> {
    let canonical = normalize_nns_name(raw)?;
    lookup_name_canonical(&canonical).await
}

/// Returns `Ok(())` when the name appears available; `Err` with user-facing message otherwise.
pub(crate) async fn ensure_name_available(canonical_name: &str) -> Result<(), String> {
    lookup_name_canonical(canonical_name).await.map(|_| ())
}

async fn lookup_name_canonical(canonical_name: &str) -> Result<NnsLookupOk, String> {
    let _ = normalize_nns_name(canonical_name)?;
    let stem = canonical_name
        .strip_suffix(".nock")
        .ok_or_else(|| "expected .nock suffix".to_string())?;
    let fee_nicks = fee_nicks_for_stem(stem);

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
        return Ok(NnsLookupOk {
            canonical_name: canonical_name.to_string(),
            fee_nicks,
            listed_nocks: body.price,
        });
    }
    if status.contains("register") || status.contains("taken") || status.contains("pending") {
        return Err(format!(
            "`{}` is not available ({})",
            body.name, body.status
        ));
    }
    Err(format!(
        "`{}` could not be registered (status: {})",
        body.name, body.status
    ))
}

pub(crate) fn preview_lines(canonical_name: &str, usd_per_nock: Option<f64>) -> Vec<String> {
    let stem = canonical_name
        .strip_suffix(".nock")
        .unwrap_or(canonical_name);
    let fee_nicks = fee_nicks_for_stem(stem);
    let blob = claim_blob_for_name(canonical_name);
    vec![
        format!("Name: {canonical_name}"),
        format!("Registry: {REGISTRY_P2PKH}"),
        format!(
            "Fee: {}",
            format_nock_amount_with_usd(fee_nicks as u128, usd_per_nock)
        ),
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
        assert_eq!(fee_nocks_for_stem("a"), 5000);
        assert_eq!(fee_nicks_for_stem("a"), 5000 * 65_536);
        assert_eq!(fee_nocks_for_stem("abcde"), 500);
        assert_eq!(fee_nicks_for_stem("abcdefghij"), 100 * 65_536);
    }

    #[test]
    fn format_nock_with_usd() {
        let s = format_nock_amount_with_usd(65_536, Some(2.0));
        assert!(s.contains("1 NOCK"));
        assert!(s.contains("$2.00"));
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
