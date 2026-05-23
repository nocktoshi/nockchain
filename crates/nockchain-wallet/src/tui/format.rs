//! Shared formatting helpers for the TUI view layer.

const NICKS_PER_NOCK: u128 = 65_536;

/// Parse a decimal NOCK amount (e.g. `100` or `100.5`) into nicks.
pub(crate) fn parse_nock_amount_to_nicks(s: &str) -> Result<u64, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("Amount is required".into());
    }
    let nocks: f64 = t
        .parse()
        .map_err(|e| format!("Invalid amount: {e}"))?;
    if !nocks.is_finite() || nocks <= 0.0 {
        return Err("Amount must be greater than zero".into());
    }
    let nicks = (nocks * NICKS_PER_NOCK as f64).round() as u64;
    if nicks == 0 {
        return Err("Amount is too small".into());
    }
    Ok(nicks)
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

/// Home hero balance: `39,925.56 NOCK` (comma grouping, two decimals).
pub(crate) fn format_nock_balance_display(nicks: u128) -> String {
    let nocks = nicks as f64 / NICKS_PER_NOCK as f64;
    format!("{} NOCK", format_nocks_decimal(nocks, 2))
}

fn format_nocks_decimal(nocks: f64, decimals: usize) -> String {
    if !nocks.is_finite() {
        return "—".to_string();
    }
    let sign = if nocks < 0.0 { "-" } else { "" };
    let nocks = nocks.abs();
    let factor = 10_f64.powi(decimals as i32);
    let rounded = (nocks * factor).round() / factor;
    let int_part = rounded.floor() as u128;
    let frac = ((rounded - int_part as f64) * factor).round() as u64;
    if decimals == 0 {
        format!("{sign}{}", format_integer_with_commas(int_part))
    } else {
        format!(
            "{sign}{}.{:0width$}",
            format_integer_with_commas(int_part),
            frac,
            width = decimals
        )
    }
}

fn format_integer_with_commas(n: u128) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::new();
    for (i, ch) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(*ch as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_nock_amount() {
        assert_eq!(parse_nock_amount_to_nicks("1").unwrap(), 65_536);
        assert_eq!(parse_nock_amount_to_nicks("100").unwrap(), 100 * 65_536);
    }

    #[test]
    fn balance_display_commas() {
        let nicks = (39925.56 * 65_536.0) as u128;
        assert_eq!(format_nock_balance_display(nicks), "39,925.56 NOCK");
    }
}
