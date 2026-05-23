//! HTTP listener and routes (`/v1/wallet/state`, `/v1/wallet/command`).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{middleware, Json, Router};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use super::auth::require_api_auth;
use super::state::{save_session_state, validate_session_state, WalletSessionState};
use crate::wallet_outcome::WalletCommandJsonResponse;

/// Work item executed on the REPL local task set (wallet is not `Send`).
#[derive(Debug)]
pub(crate) struct ReplApiJob {
    pub argv: Vec<String>,
    pub resp: oneshot::Sender<WalletCommandJsonResponse>,
}

#[derive(Clone)]
struct HttpState {
    jobs: mpsc::Sender<ReplApiJob>,
    session: Arc<RwLock<WalletSessionState>>,
    session_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CommandRequest {
    pub argv: Vec<String>,
}

/// Handle to stop the background HTTP listener (e.g. when Settings changes the bind address).
pub(crate) struct ApiServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl ApiServerHandle {
    pub(crate) fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Spawn the HTTP server on a background thread.
pub(crate) fn spawn_http_server(
    listen: String,
    job_tx: mpsc::Sender<ReplApiJob>,
    session_path: PathBuf,
    session: Arc<RwLock<WalletSessionState>>,
    api_auth_token: Arc<str>,
) -> Result<ApiServerHandle, String> {
    let addr: SocketAddr = listen
        .parse()
        .map_err(|e| format!("Invalid API listen address '{listen}': {e}"))?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let join = thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                warn!("wallet API runtime failed: {e}");
                return;
            }
        };
        rt.block_on(async move {
            let state = HttpState {
                jobs: job_tx,
                session,
                session_path,
            };
            let auth_token = Arc::clone(&api_auth_token);
            let app = Router::new()
                .route("/health", get(health))
                .route("/v1/wallet/state", get(get_session).post(post_session))
                .route("/v1/wallet/command", post(run_command))
                .route_layer(middleware::from_fn_with_state(auth_token, require_api_auth))
                .with_state(state);

            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    warn!(%addr, "wallet API bind failed: {e}");
                    return;
                }
            };
            info!(%addr, "wallet JSON API listening (REPL session)");
            let serve = axum::serve(listener, app);
            let shutdown = async {
                let _ = shutdown_rx.await;
            };
            if let Err(e) = serve.with_graceful_shutdown(shutdown).await {
                warn!("wallet API server stopped: {e}");
            }
        });
    });

    Ok(ApiServerHandle {
        shutdown: Some(shutdown_tx),
        join: Some(join),
    })
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "schema_version": crate::wallet_outcome::WALLET_OUTCOME_SCHEMA,
        "session_schema_version": super::WALLET_SESSION_SCHEMA,
        "wallet_event_kinds": [
            "balance_snapshot_v1",
            "notes_list_v1",
            "address_list_v1",
            "key_tree_v1",
            "keygen_v1",
            "migrate_summary_v1",
            "tx_accepted_v1",
            "nns_registration_v1"
        ],
    }))
}

async fn get_session(State(ctx): State<HttpState>) -> Json<WalletSessionState> {
    Json(ctx.session.read().unwrap().clone())
}

async fn post_session(
    State(ctx): State<HttpState>,
    Json(body): Json<WalletSessionState>,
) -> Result<Json<WalletSessionState>, (StatusCode, String)> {
    validate_session_state(&body).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    save_session_state(&ctx.session_path, &body)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    *ctx.session.write().unwrap() = body.clone();
    Ok(Json(body))
}

async fn run_command(
    State(state): State<HttpState>,
    Json(body): Json<CommandRequest>,
) -> (StatusCode, Json<WalletCommandJsonResponse>) {
    if body.argv.is_empty() {
        return bad_request("argv must contain at least one command token".into());
    }

    let (resp_tx, resp_rx) = oneshot::channel();
    let job = ReplApiJob {
        argv: body.argv,
        resp: resp_tx,
    };
    if state.jobs.send(job).await.is_err() {
        return server_error("REPL wallet executor unavailable (is the REPL running?)");
    }

    match resp_rx.await {
        Ok(json) => {
            let status = if json.success.is_some() {
                StatusCode::OK
            } else {
                StatusCode::UNPROCESSABLE_ENTITY
            };
            (status, Json(json))
        }
        Err(_) => server_error("REPL wallet executor dropped response"),
    }
}

fn bad_request(msg: String) -> (StatusCode, Json<WalletCommandJsonResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(WalletCommandJsonResponse {
            schema_version: crate::wallet_outcome::WALLET_OUTCOME_SCHEMA,
            success: None,
            error: Some(msg),
        }),
    )
}

fn server_error(msg: &str) -> (StatusCode, Json<WalletCommandJsonResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(WalletCommandJsonResponse {
            schema_version: crate::wallet_outcome::WALLET_OUTCOME_SCHEMA,
            success: None,
            error: Some(msg.to_string()),
        }),
    )
}
