//! Session-scoped bearer token for the local JSON API (not persisted).

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

/// Random bearer token length in bytes (hex-encoded in the `Authorization` header).
const API_TOKEN_BYTES: usize = 32;

/// Generate a secret token for one REPL session (held in [`crate::repl::command_runner::ReplRuntime`] only).
pub(crate) fn generate_api_token() -> String {
    let mut bytes = [0u8; API_TOKEN_BYTES];
    getrandom::fill(&mut bytes).expect("getrandom");
    hex::encode(bytes)
}

pub(crate) fn bearer_matches(header_value: &str, expected: &str) -> bool {
    const PREFIX: &str = "Bearer ";
    let Some(rest) = header_value.strip_prefix(PREFIX) else {
        return false;
    };
    constant_time_eq(rest.trim().as_bytes(), expected.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Axum middleware: require `Authorization: Bearer <token>` matching this REPL session.
pub(crate) async fn require_api_auth(
    State(expected): State<Arc<str>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let authorized = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| bearer_matches(v, expected.as_ref()));
    if !authorized {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_parsing() {
        assert!(bearer_matches("Bearer abc", "abc"));
        assert!(bearer_matches("Bearer  abc  ", "abc"));
        assert!(!bearer_matches("Bearer abc", "abd"));
        assert!(!bearer_matches("Basic abc", "abc"));
    }
}
