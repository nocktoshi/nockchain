//! HTTP client for [`crate::tui::wallet_api`] session GET/POST (TUI settings).

use crate::tui::wallet_api::WalletSessionState;

pub fn api_base_url(listen: &str) -> String {
    let t = listen.trim();
    if t.starts_with("http://") || t.starts_with("https://") {
        t.to_string()
    } else {
        format!("http://{t}")
    }
}

pub(crate) async fn get_session(
    api_listen: &str,
    api_token: &str,
) -> Result<WalletSessionState, String> {
    let url = format!("{}/v1/wallet/state", api_base_url(api_listen));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .get(&url)
        .bearer_auth(api_token)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url}: HTTP {}", resp.status()));
    }
    resp.json()
        .await
        .map_err(|e| format!("GET {url} JSON: {e}"))
}

pub(crate) async fn post_session(
    api_listen: &str,
    api_token: &str,
    state: WalletSessionState,
) -> Result<WalletSessionState, String> {
    let url = format!("{}/v1/wallet/state", api_base_url(api_listen));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .post(&url)
        .bearer_auth(api_token)
        .json(&state)
        .send()
        .await
        .map_err(|e| format!("POST {url}: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("POST {url}: HTTP {status} {body}"));
    }
    resp.json()
        .await
        .map_err(|e| format!("POST {url} JSON: {e}"))
}
