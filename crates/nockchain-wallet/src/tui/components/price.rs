//! CoinGecko USD price for the home hero.

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;

const COINGECKO_PRICE_URL: &str = "https://api.coingecko.com/api/v3/simple/price";
const COIN_ID: &str = "nockchain";
const DEMO_KEY_HEADER: &str = "x-cg-demo-api-key";

static DOTENV_LOADED: OnceLock<()> = OnceLock::new();

/// Load `.env` from the working directory (or parents) so `COINGECKO_API_KEY` is set.
fn ensure_dotenv() {
    DOTENV_LOADED.get_or_init(|| {
        let _ = dotenvy::dotenv();
    });
}

fn coingecko_api_key() -> Option<String> {
    ensure_dotenv();
    let key = std::env::var("COINGECKO_API_KEY").ok()?;
    let key = key.trim().to_string();
    if key.is_empty() {
        None
    } else {
        Some(key)
    }
}

#[derive(Debug, Deserialize)]
struct PriceResponse {
    nockchain: Option<UsdOnly>,
}

#[derive(Debug, Deserialize)]
struct UsdOnly {
    usd: Option<f64>,
}

/// Fetch NOCK/USD from CoinGecko (demo key via `x-cg-demo-api-key` header).
pub(crate) async fn fetch_nock_usd() -> Result<f64, String> {
    let api_key = coingecko_api_key();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|e| e.to_string())?;
    let mut req = client.get(COINGECKO_PRICE_URL).query(&[
        ("ids", COIN_ID),
        ("vs_currencies", "usd"),
    ]);
    if let Some(key) = api_key.as_deref() {
        let name = HeaderName::from_static(DEMO_KEY_HEADER);
        let value = HeaderValue::from_str(key)
            .map_err(|e| format!("Invalid COINGECKO_API_KEY: {e}"))?;
        req = req.header(name, value);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Price fetch failed: {e}"))?;
    if !resp.status().is_success() {
        let hint = if api_key.is_none() {
            " — set COINGECKO_API_KEY in .env or the environment"
        } else {
            ""
        };
        return Err(format!("CoinGecko returned {}{}", resp.status(), hint));
    }
    let body: PriceResponse = resp
        .json()
        .await
        .map_err(|e| format!("Invalid price JSON: {e}"))?;
    body.nockchain
        .and_then(|n| n.usd)
        .filter(|p| p.is_finite() && *p > 0.0)
        .ok_or_else(|| format!("No USD price for {COIN_ID}"))
}

/// Format portfolio USD total for the home hero.
pub(crate) fn format_usd_total(nicks: u64, usd_per_coin: f64) -> String {
    let nocks = nicks as f64 / 65_536.0;
    let total = nocks * usd_per_coin;
    if total >= 1_000_000.0 {
        format!("${total:.2}")
    } else if total >= 1.0 {
        format!("${total:.2}")
    } else if total > 0.0 {
        format!("${total:.4}")
    } else {
        "$0.00".to_string()
    }
}
