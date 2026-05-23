//! TUI-only orchestration around [`crate::dispatch::execute_wallet_command`].
//! CLI entry continues to call dispatch directly with owned [`crate::Wallet`] — unaffected by this module.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nockapp::NockAppError;
use tokio::sync::{mpsc, watch, Mutex};
use wallet_tx_builder::adapter::NormalizedSnapshot;

use super::screens::Screen;
use super::store::{UIStore, UiAction};
use crate::command::{Commands, WalletCli};
use crate::create_tx::WrittenTxSnapshot;
use crate::dispatch::{execute_wallet_command, DispatchHooks};
use crate::tui::wallet_api::{TuiApiJob, WalletSessionState};
use crate::wallet_outcome::{WalletCommandData, WalletEvent};
use crate::Wallet;

const TX_DIR: &str = "txs";

/// [`NockApp::run`] can return before the structured effect driver has polled; yield briefly.
async fn snapshot_wallet_events(
    sink: &Arc<std::sync::Mutex<Vec<WalletEvent>>>,
) -> Vec<WalletEvent> {
    let mut events = sink.lock().unwrap().clone();
    if !events.is_empty() {
        return events;
    }
    for _ in 0..96 {
        tokio::task::yield_now().await;
        events = sink.lock().unwrap().clone();
        if !events.is_empty() {
            return events;
        }
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    sink.lock().unwrap().clone()
}

/// Job completion: command result, structured events, and captured kernel `%markdown`.
pub(crate) type JobCompletion = (Result<(), NockAppError>, Vec<WalletEvent>, String);

/// Background balance sidebar refresh (same `ShowBalance` path as the menu; does not use [`Screen::Running`]).
pub(crate) type BalanceRefreshCompletion = (u64, Result<(), NockAppError>, Vec<WalletEvent>);

/// Simple-send planner preview (no kernel poke).
pub(crate) type SendSimplePlanCompletion =
    Result<(String, Commands), String>;

/// NNS name availability lookup (HTTP).
pub(crate) type NnsLookupCompletion = Result<crate::tui::nns::NnsLookupOk, String>;

/// Home identity: active address + optional primary `.nock` name.
pub(crate) type HomeIdentityCompletion = (Option<String>, Option<String>);

/// Shared wallet + snapshot for spawned TUI jobs (`tui::run` wraps with [`Arc`]).
#[derive(Clone)]
pub(crate) struct TuiRuntime {
    pub wallet: Arc<Mutex<Wallet>>,
    pub snapshot: Arc<Mutex<Option<NormalizedSnapshot>>>,
    /// Session CLI (connection may be updated from Settings).
    pub cli: Arc<std::sync::Mutex<WalletCli>>,
    /// Structured kernel/API events from `[%raw …]` effects.
    pub wallet_event_sink: Arc<std::sync::Mutex<Vec<WalletEvent>>>,
    /// Captured kernel `%markdown` (create-tx, send-tx, …) for TUI formatters.
    pub tui_markdown_sink: Arc<std::sync::Mutex<String>>,
    /// Session settings persisted in `session.json` and exposed via GET/POST `/v1/wallet/state`.
    pub session_config: Arc<RwLock<WalletSessionState>>,
    pub session_path: PathBuf,
    /// Secret bearer token for this TUI session only (never written to disk).
    pub api_auth_token: Arc<str>,
    /// Channel to the background HTTP server (jobs executed on this TUI [`LocalSet`]).
    pub api_job_tx: mpsc::Sender<TuiApiJob>,
    /// Background HTTP listener (restarted when session `api_listen` changes).
    pub api_server: Arc<std::sync::Mutex<Option<crate::tui::wallet_api::ApiServerHandle>>>,
}

/// Run a wallet command on the shared TUI runtime (TUI jobs and JSON API).
pub(crate) async fn run_command_on_runtime(
    rt: &TuiRuntime,
    cli: &WalletCli,
    command: Commands,
    sync_attempt: Option<watch::Sender<(usize, usize)>>,
    tx_snapshot_before: Option<WrittenTxSnapshot>,
) -> Result<WalletCommandData, NockAppError> {
    rt.tui_markdown_sink.lock().unwrap().clear();
    let mut hooks = DispatchHooks::structured_with_markdown(
        Arc::clone(&rt.wallet_event_sink),
        Arc::clone(&rt.tui_markdown_sink),
    );
    if let Some(tx) = sync_attempt {
        hooks = hooks.with_sync_attempt(tx);
    }
    let is_create_tx = matches!(command, Commands::CreateTx { .. });
    let outcome = {
        let mut w = rt.wallet.lock().await;
        let mut s = rt.snapshot.lock().await;
        execute_wallet_command(cli, &mut *w, &command, &mut *s, false, hooks).await
    };
    match finalize_outcome(outcome, &rt.wallet_event_sink).await {
        Ok(mut data) => {
            if is_create_tx {
                append_create_tx_event(
                    &mut data.events,
                    &rt.tui_markdown_sink.lock().unwrap(),
                    tx_snapshot_before,
                )
                .await?;
            }
            Ok(data)
        }
        Err(e) => Err(e),
    }
}

async fn finalize_outcome(
    outcome: Result<WalletCommandData, NockAppError>,
    wallet_event_sink: &Arc<std::sync::Mutex<Vec<WalletEvent>>>,
) -> Result<WalletCommandData, NockAppError> {
    match outcome {
        Ok(mut data) => {
            if data.events.is_empty() {
                data.events = snapshot_wallet_events(wallet_event_sink).await;
            }
            Ok(data)
        }
        Err(e) => Err(e),
    }
}

/// Queue a wallet command: [`Screen::Running`] + in-TUI progress; work runs without leaving the alternate screen.
pub(crate) fn schedule_wallet_command(
    store: &mut UIStore,
    rt: &TuiRuntime,
    done_tx: mpsc::UnboundedSender<JobCompletion>,
    cmd: Commands,
    label: impl Into<String>,
) {
    if matches!(store.state.screen, Screen::Running { .. }) {
        return;
    }
    rt.wallet_event_sink.lock().unwrap().clear();
    let (progress_tx, progress_rx) = watch::channel((0usize, 5usize));
    let cmd_clone = cmd.clone();
    let label_s = label.into();
    store.dispatch(UiAction::EnterRunningWalletJob {
        cmd: cmd_clone.clone(),
        label: label_s,
        progress_rx,
    });

    let rt = rt.clone();
    tokio::task::spawn_local(async move {
        let cli = rt.cli.lock().unwrap().clone();
        let outcome =
            run_command_on_runtime(&rt, &cli, cmd_clone, Some(progress_tx), None).await;
        let events = outcome
            .as_ref()
            .map(|d| d.events.clone())
            .unwrap_or_default();
        let markdown = rt.tui_markdown_sink.lock().unwrap().clone();
        let exec_result = outcome.map(|_| ());
        let _ = done_tx.send((exec_result, events, markdown));
    });
}

/// Refresh balance text for the main-menu sidebar (does not swap to [`Screen::Running`]).
pub(crate) fn schedule_balance_sidebar_refresh(
    store: &mut UIStore,
    rt: &TuiRuntime,
    done_tx: &mpsc::UnboundedSender<BalanceRefreshCompletion>,
) {
    if !matches!(store.state.screen, Screen::Home) {
        return;
    }
    if store.state.balance_panel.loading {
        return;
    }
    rt.wallet_event_sink.lock().unwrap().clear();
    let (progress_tx, progress_rx) = watch::channel((0usize, 5usize));
    store.dispatch(UiAction::BeginBalanceSidebarFetch { progress_rx });

    let nonce = store.state.balance_job_nonce;

    let rt = rt.clone();
    let tx = done_tx.clone();
    tokio::task::spawn_local(async move {
        let cli = rt.cli.lock().unwrap().clone();
        let outcome = run_command_on_runtime(
            &rt,
            &cli,
            Commands::ShowBalance,
            Some(progress_tx),
            None,
        )
        .await;
        let events = outcome
            .as_ref()
            .map(|d| d.events.clone())
            .unwrap_or_default();
        let exec_result = outcome.map(|_| ());
        let _ = tx.send((nonce, exec_result, events));
    });
}

pub(crate) fn apply_balance_sidebar_result(
    store: &mut UIStore,
    nonce: u64,
    result: Result<(), NockAppError>,
    events: Vec<WalletEvent>,
) {
    store.dispatch(UiAction::BalanceSidebarCompleted {
        nonce,
        result,
        events,
    });
}

/// Resolve primary `.nock` name for an address already shown on home/receive.
pub(crate) fn schedule_nockname_resolve(
    address: String,
    done_tx: &mpsc::UnboundedSender<HomeIdentityCompletion>,
) {
    let tx = done_tx.clone();
    tokio::task::spawn_local(async move {
        let nockname = crate::tui::nns::resolve_primary_name(&address)
            .await
            .ok()
            .flatten();
        let _ = tx.send((Some(address), nockname));
    });
}

/// Load active address (same path as Receive) and resolve primary `.nock` name for home.
pub(crate) fn schedule_home_identity_fetch(
    store: &mut UIStore,
    rt: &TuiRuntime,
    done_tx: &mpsc::UnboundedSender<HomeIdentityCompletion>,
) {
    if !matches!(store.state.screen, Screen::Home) {
        return;
    }
    if store.state.balance_panel.identity_loading {
        return;
    }

    if let Some(addr) = store.state.balance_panel.address.clone() {
        store.dispatch(UiAction::BeginHomeIdentityFetch);
        schedule_nockname_resolve(addr, done_tx);
        return;
    }

    store.dispatch(UiAction::BeginHomeIdentityFetch);

    let rt = rt.clone();
    let tx = done_tx.clone();
    tokio::task::spawn_local(async move {
        let cli = rt.cli.lock().unwrap().clone();
        let outcome = run_command_on_runtime(
            &rt,
            &cli,
            Commands::ListActiveAddresses,
            None,
            None,
        )
        .await;
        let events = outcome
            .as_ref()
            .map(|d| d.events.clone())
            .unwrap_or_default();
        let markdown = rt.tui_markdown_sink.lock().unwrap().clone();
        let address = super::view::first_active_address_from_output(&events, &markdown);
        let nockname = match address.as_deref() {
            Some(a) => crate::tui::nns::resolve_primary_name(a).await.ok().flatten(),
            None => None,
        };
        let _ = tx.send((address, nockname));
    });
}

pub(crate) fn apply_home_identity_result(
    store: &mut UIStore,
    address: Option<String>,
    nockname: Option<String>,
) {
    store.dispatch(UiAction::HomeIdentityCompleted {
        address,
        nockname,
    });
}

pub(crate) fn apply_job_result(
    store: &mut UIStore,
    result: Result<(), NockAppError>,
    events: Vec<WalletEvent>,
    markdown: String,
    identity_done_tx: &mpsc::UnboundedSender<HomeIdentityCompletion>,
) {
    let receive_fetch = matches!(
        &store.state.screen,
        Screen::Running {
            cmd: Commands::ListActiveAddresses,
            restore,
            ..
        } if matches!(**restore, Screen::Receive { .. })
    );
    let ok = result.is_ok();
    store.dispatch(UiAction::JobCompleted {
        result,
        events,
        markdown,
    });
    if receive_fetch && ok {
        if let Some(addr) = store.state.balance_panel.address.clone() {
            schedule_nockname_resolve(addr, identity_done_tx);
        }
    }
}

/// Query NNS registry for name availability (background HTTP).
pub(crate) fn schedule_nns_lookup(
    raw: String,
    done_tx: mpsc::UnboundedSender<NnsLookupCompletion>,
) {
    tokio::task::spawn_local(async move {
        let result = crate::tui::nns::lookup_name(&raw).await;
        let _ = done_tx.send(result);
    });
}

/// Plan a simple-send transaction and return preview text for the main-panel review screen.
pub(crate) fn schedule_send_simple_plan(
    rt: TuiRuntime,
    cmd: Commands,
    done_tx: mpsc::UnboundedSender<SendSimplePlanCompletion>,
) {
    tokio::task::spawn_local(async move {
        let snap = rt.snapshot.lock().await.clone();
        let mut wallet = rt.wallet.lock().await;
        let result = super::send_simple::plan_send_preview(&mut wallet, snap, &cmd)
            .await
            .map(|preview| (preview, cmd))
            .map_err(|e| e.to_string());
        let _ = done_tx.send(result);
    });
}

/// Create the transaction file, then broadcast each with `send-tx`.
pub(crate) fn schedule_create_and_send(
    store: &mut UIStore,
    rt: &TuiRuntime,
    done_tx: mpsc::UnboundedSender<JobCompletion>,
    create_cmd: Commands,
    label: impl Into<String>,
) {
    if matches!(store.state.screen, Screen::Running { .. }) {
        return;
    }
    rt.wallet_event_sink.lock().unwrap().clear();
    rt.tui_markdown_sink.lock().unwrap().clear();
    let (progress_tx, progress_rx) = watch::channel((0usize, 5usize));
    let cmd_clone = create_cmd.clone();
    store.dispatch(UiAction::EnterRunningWalletJob {
        cmd: cmd_clone,
        label: label.into(),
        progress_rx,
    });

    let rt = rt.clone();
    tokio::task::spawn_local(async move {
        let cli = rt.cli.lock().unwrap().clone();
        let before = match Wallet::snapshot_written_txs(Path::new(TX_DIR)).await {
            Ok(s) => s,
            Err(e) => {
                let _ = done_tx.send((Err(e), Vec::new(), String::new()));
                return;
            }
        };

        let create_outcome = run_command_on_runtime(
            &rt,
            &cli,
            create_cmd,
            Some(progress_tx),
            Some(before.clone()),
        )
        .await;

        let mut events = create_outcome
            .as_ref()
            .map(|d| d.events.clone())
            .unwrap_or_default();

        if create_outcome.is_err() {
            let markdown = rt.tui_markdown_sink.lock().unwrap().clone();
            let _ = done_tx.send((create_outcome.map(|_| ()), events, markdown));
            return;
        }

        let tx_paths = create_tx_paths_from_events_or_disk(&events, &before).await;
        if tx_paths.is_empty() {
            let markdown = rt.tui_markdown_sink.lock().unwrap().clone();
            let _ = done_tx.send((
                Err(NockAppError::OtherError(
                    "create-tx finished but no transaction file was written under ./txs/"
                        .into(),
                )),
                events,
                markdown,
            ));
            return;
        }

        let mut combined = Ok(());
        for path in tx_paths {
            let send_outcome = run_command_on_runtime(
                &rt,
                &cli,
                Commands::SendTx {
                    transaction: path.clone(),
                },
                None,
                None,
            )
            .await;
            match send_outcome {
                Ok(data) => events.extend(data.events),
                Err(e) => {
                    combined = Err(e);
                    break;
                }
            }
        }

        let markdown = rt.tui_markdown_sink.lock().unwrap().clone();
        refresh_create_tx_summary(&mut events, &markdown);
        let _ = done_tx.send((combined, events, markdown));
    });
}

/// Simple-send review **Send** — same as [`schedule_create_and_send`].
pub(crate) fn schedule_send_simple_create_and_send(
    store: &mut UIStore,
    rt: &TuiRuntime,
    done_tx: mpsc::UnboundedSender<JobCompletion>,
    create_cmd: Commands,
) {
    schedule_create_and_send(store, rt, done_tx, create_cmd, "Create & send");
}

/// NNS **Register** — build registry payment tx, create file, then `send-tx`.
pub(crate) fn schedule_nns_register(
    store: &mut UIStore,
    rt: &TuiRuntime,
    done_tx: mpsc::UnboundedSender<JobCompletion>,
    canonical_name: &str,
) -> Result<(), String> {
    let recipient = crate::tui::nns::build_registry_recipient(canonical_name)?;
    let cmd = crate::tui::nns::schedule_create_tx_command(recipient);
    schedule_create_and_send(store, rt, done_tx, cmd, "Register & send");
    Ok(())
}

async fn append_create_tx_event(
    events: &mut Vec<WalletEvent>,
    markdown: &str,
    before: Option<WrittenTxSnapshot>,
) -> Result<(), NockAppError> {
    if events
        .iter()
        .any(|e| matches!(e, WalletEvent::CreateTxV1 { .. }))
    {
        return Ok(());
    }
    let tx_paths = if let Some(before) = before {
        let after = Wallet::snapshot_written_txs(Path::new(TX_DIR)).await?;
        Wallet::detect_written_tx_paths(&before, &after).ok()
    } else {
        None
    };
    let tx_paths = tx_paths.unwrap_or_else(|| parse_tx_paths_from_markdown(markdown));
    if markdown.trim().is_empty() && tx_paths.is_empty() {
        return Ok(());
    }
    events.push(WalletEvent::CreateTxV1 {
        tx_paths,
        summary: markdown.to_string(),
    });
    Ok(())
}

fn refresh_create_tx_summary(events: &mut Vec<WalletEvent>, markdown: &str) {
    for event in events.iter_mut() {
        if let WalletEvent::CreateTxV1 { summary, .. } = event {
            *summary = markdown.to_string();
            return;
        }
    }
}

async fn create_tx_paths_from_events_or_disk(
    events: &[WalletEvent],
    before: &WrittenTxSnapshot,
) -> Vec<String> {
    if let Some(WalletEvent::CreateTxV1 { tx_paths, .. }) = events
        .iter()
        .find(|e| matches!(e, WalletEvent::CreateTxV1 { .. }))
    {
        if !tx_paths.is_empty() {
            return tx_paths.clone();
        }
    }
    if let Ok(after) = Wallet::snapshot_written_txs(Path::new(TX_DIR)).await {
        if let Ok(paths) = Wallet::detect_written_tx_paths(before, &after) {
            return paths;
        }
    }
    Vec::new()
}

fn parse_tx_paths_from_markdown(markdown: &str) -> Vec<String> {
    markdown
        .lines()
        .filter_map(|line| {
            line.split("Saved transaction to ")
                .nth(1)
                .map(|rest| rest.trim().trim_start_matches('`').trim_end_matches('`').to_string())
        })
        .collect()
}

/// Spawn CoinGecko price fetch (result delivered on `price_done_tx`).
pub(crate) fn schedule_price_fetch(
    store: &mut UIStore,
    price_done_tx: &mpsc::UnboundedSender<Result<f64, String>>,
) {
    use super::store::price_fetch_stale;

    if store.state.price.loading {
        return;
    }
    if !price_fetch_stale(&store.state) {
        return;
    }
    store.dispatch(UiAction::BeginPriceFetch);
    let tx = price_done_tx.clone();
    tokio::task::spawn_local(async move {
        let result = super::components::price::fetch_nock_usd().await;
        let _ = tx.send(result);
    });
}
