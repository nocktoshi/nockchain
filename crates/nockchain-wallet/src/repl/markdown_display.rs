//! Post-process kernel markdown before it is shown in the REPL (output + balance panels).
//!
//! - **Nicks → NOCK:** `65536` nicks = `1` NOCK; patterns like `25000 nicks` / `1 nick` are rewritten.
//! - **Addresses → .nock names:** base58 v1-style tokens are resolved via
//!   [`NOCKNAMES_RESOLVE_URL`] (`https://api.nocknames.com/resolve?address=…`).

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use termimad::MadSkin;
use tracing::warn;

/// Public HTTP API (see product docs); returns JSON `{ "name": "<label>.nock" }`.
const NOCKNAMES_RESOLVE_URL: &str = "https://api.nocknames.com/resolve";

const NICKS_PER_NOCK: u128 = 65_536;

#[derive(Debug, Deserialize)]
struct NocknamesResolveBody {
    name: String,
}

fn http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .timeout(Duration::from_secs(8))
            .connect_timeout(Duration::from_secs(4))
            .build()
            .expect("reqwest client for nocknames")
    })
}

fn nicks_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\b(\d+)\s+nicks?\b").expect("nicks rewrite regex")
    })
}

fn base58_token_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[1-9A-HJ-NP-Za-km-z]{40,52}").expect("base58 token regex"))
}

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

fn translate_nicks_in_markdown(raw: &str) -> String {
    let re = nicks_pattern();
    re.replace_all(raw, |caps: &regex::Captures<'_>| {
        let Ok(nicks) = caps[1].parse::<u128>() else {
            return caps[0].to_string();
        };
        format_nock_from_nicks(nicks)
    })
    .into_owned()
}

fn looks_like_address_token(token: &str) -> bool {
    if token.len() < 40 || token.len() > 52 {
        return false;
    }
    bs58::decode(token).into_vec().is_ok()
}

fn collect_address_candidates(text: &str) -> Vec<String> {
    let re = base58_token_pattern();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for m in re.find_iter(text) {
        let t = m.as_str();
        if !looks_like_address_token(t) {
            continue;
        }
        if seen.insert(t.to_string()) {
            out.push(t.to_string());
        }
    }
    out.sort_by_key(|s| std::cmp::Reverse(s.len()));
    out
}

async fn resolve_address_to_name(client: &Client, address: &str) -> Option<String> {
    let resp = client
        .get(NOCKNAMES_RESOLVE_URL)
        .query(&[("address", address)])
        .send()
        .await
        .map_err(|e| {
            warn!(address_len = address.len(), "nocknames resolve request failed: {e}");
        })
        .ok()?;
    if !resp.status().is_success() {
        warn!(
            status = %resp.status(),
            "nocknames resolve non-success"
        );
        return None;
    }
    let body: NocknamesResolveBody = resp.json().await.map_err(|e| {
        warn!("nocknames resolve JSON: {e}");
    }).ok()?;
    let name = body.name.trim();
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

async fn translate_addresses_in_markdown(raw: &str) -> String {
    let candidates = collect_address_candidates(raw);
    if candidates.is_empty() {
        return raw.to_string();
    }
    let client = http_client();
    let mut map: HashMap<String, String> = HashMap::new();
    for addr in candidates {
        if let Some(name) = resolve_address_to_name(client, &addr).await {
            map.insert(addr, name);
        }
    }
    if map.is_empty() {
        return raw.to_string();
    }
    let mut sorted: Vec<(String, String)> = map.into_iter().collect();
    sorted.sort_by_key(|(addr, _)| std::cmp::Reverse(addr.len()));
    let mut out = raw.to_string();
    for (addr, name) in sorted {
        out = out.replace(&addr, &name);
    }
    out
}

/// Full pipeline for any markdown captured from the wallet kernel before UI storage.
pub(crate) async fn format_repl_markdown_for_display(raw: &str) -> String {
    let after_nicks = translate_nicks_in_markdown(raw);
    translate_addresses_in_markdown(&after_nicks).await
}

/// Apply semantic adapters then [`MadSkin`] terminal rendering for Ratatui output panels.
pub(crate) async fn repl_tui_text_from_captured_raw(raw: &str) -> String {
    let adapted = format_repl_markdown_for_display(raw).await;
    render_raw_markdown_for_tui(&adapted)
}

/// Kernel/raw markdown → terminal-friendly text (ANSI as produced by termimad).
pub(crate) fn render_raw_markdown_for_tui(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    let skin = MadSkin::default_dark();
    format!("{}", skin.term_text(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nicks_to_nock_integer() {
        assert_eq!(format_nock_from_nicks(65_536), "1 NOCK");
    }

    #[test]
    fn nicks_to_nock_half() {
        assert_eq!(format_nock_from_nicks(32_768), "0.5 NOCK");
    }

    #[test]
    fn translate_nicks_line() {
        let s = translate_nicks_in_markdown("Balance 131072 nicks pending.");
        assert!(s.contains("2 NOCK"), "got {s:?}");
        assert!(!s.contains("nicks"));
    }

    #[test]
    fn translate_nicks_case_insensitive() {
        let s = translate_nicks_in_markdown("100000 NICKS");
        assert!(s.contains("NOCK"));
        assert!(!s.to_lowercase().contains("nicks"));
    }
}
