//! Shared wallet command execution for one-shot CLI and interactive TUI.

use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// gRPC / network hiccups that are worth retrying before surfacing to the user.
fn is_transient_sync_error(err: &NockAppError) -> bool {
    let s = err.to_string().to_lowercase();
    s.contains("timeout")
        || s.contains("timed out")
        || s.contains("unavailable")
        || s.contains("connection reset")
        || s.contains("stream")
        || s.contains("temporarily")
        || s.contains("try again")
        || s.contains("deadline")
}

use indicatif::ProgressBar;
use nockapp::driver::{make_driver, IODriverFn};
use nockapp::noun::slab::NounSlab;
use nockapp::utils::make_tas as make_tas_util;
use nockapp::wire::{SystemWire, Wire};
use nockapp::{
    complete_run_on_exit_driver, exit_driver, file_driver, markdown_driver, one_punch_driver,
    CrownError, NockAppError,
};
use nockvm::noun::{Noun, NounAllocator, NounSpace, D, SIG, T};
use nockvm_macros::tas;
use noun_serde::{NounDecode, NounDecodeError};
use termimad::MadSkin;
use tracing::{error, info};
use wallet_tx_builder::adapter::NormalizedSnapshot;

use crate::command::{CommandNoun, Commands, WalletCli, WatchSubcommand};
use crate::recipient::recipient_tokens_to_specs;
use crate::wallet_outcome::{
    migrate_summary_event, WalletAddressRowV1, WalletCommandData, WalletCommandOutcome,
    WalletEvent, WalletKeyTreeNodeV1, WalletKeygenV1, WalletNoteRowV1,
};
use crate::wallet::{normalize_watch_address, Wallet};
use crate::connection::ConnectionCli;

/// Optional progress hooks for callers. CLI uses [`DispatchHooks::cli`]; TUI/API use [`DispatchHooks::structured_with_markdown`].
#[derive(Clone, Default)]
pub struct DispatchHooks {
    /// Notified with `(attempt, max_attempts)` before each balance-sync RPC attempt.
    pub sync_attempt: Option<tokio::sync::watch::Sender<(usize, usize)>>,
    /// When set, `%markdown` effects are captured for CLI presentation (termimad during `app.run()`).
    pub markdown_capture: Option<Arc<Mutex<String>>>,
    /// When set, `[%wallet …]` effects are decoded into structured [`WalletEvent`]s (TUI/API).
    pub wallet_events: Option<Arc<Mutex<Vec<WalletEvent>>>>,
}

impl DispatchHooks {
    /// One-shot CLI: kernel `%markdown` → nockapp `markdown_driver` (stdout).
    pub fn cli() -> Self {
        Self::default()
    }

    /// TUI / JSON API: structured `[%wallet …]` plus captured `%markdown`.
    pub fn structured_with_markdown(
        events: Arc<Mutex<Vec<WalletEvent>>>,
        markdown: Arc<Mutex<String>>,
    ) -> Self {
        Self {
            sync_attempt: None,
            markdown_capture: Some(markdown),
            wallet_events: Some(events),
        }
    }

    pub fn with_sync_attempt(
        mut self,
        tx: tokio::sync::watch::Sender<(usize, usize)>,
    ) -> Self {
        self.sync_attempt = Some(tx);
        self
    }
}

/// Decode kernel `[%wallet kind [%v1 …]]` effects.
fn try_wallet_effect_event(noun: Noun, space: &NounSpace) -> Option<WalletEvent> {
    let cell = noun.in_space(space).as_cell().ok()?;
    let head = cell.head().noun();
    let tail = cell.tail().noun();
    if !unsafe { head.raw_equals(&D(tas!(b"wallet"))) } {
        return None;
    }
    let kind_cell = tail.in_space(space).as_cell().ok()?;
    let kind = kind_cell.head().noun();
    let payload = kind_cell.tail().noun();
    if unsafe { kind.raw_equals(&D(tas!(b"balance"))) } {
        return decode_balance_v1(payload, space);
    }
    if unsafe { kind.raw_equals(&D(tas!(b"notes"))) } {
        return decode_notes_v1(payload, space, None);
    }
    if unsafe { kind.raw_equals(&D(tas!(b"not-adv"))) } {
        return decode_notes_advanced_v1(payload, space);
    }
    if unsafe { kind.raw_equals(&D(tas!(b"address"))) } {
        return decode_addresses_v1(payload, space);
    }
    if unsafe { kind.raw_equals(&D(tas!(b"key-tre"))) } {
        return decode_key_tree_v1(payload, space);
    }
    if unsafe { kind.raw_equals(&D(tas!(b"keygen"))) } {
        return decode_keygen_v1(payload, space);
    }
    None
}

fn expect_v1_payload(noun: Noun, space: &NounSpace) -> Option<Noun> {
    let cell = noun.in_space(space).as_cell().ok()?;
    let head = cell.head().noun();
    if !unsafe { head.raw_equals(&D(tas!(b"v1"))) } {
        return None;
    }
    Some(cell.tail().noun())
}

fn decode_balance_v1(noun: Noun, space: &NounSpace) -> Option<WalletEvent> {
    let tail = expect_v1_payload(noun, space)?;
    let (wallet_version, block_id_b58, height, note_count, total_assets) =
        <(u64, String, u64, u64, u64)>::from_noun(&tail, space).ok()?;
    Some(WalletEvent::BalanceSnapshotV1 {
        wallet_version,
        block_id_b58,
        height,
        note_count,
        total_assets,
    })
}

fn decode_notes_v1(
    noun: Noun,
    space: &NounSpace,
    filter_address: Option<String>,
) -> Option<WalletEvent> {
    let tail = expect_v1_payload(noun, space)?;
    let (height, block_id_b58, rows) =
        <(u64, String, Vec<(String, String, u64, u64)>)>::from_noun(&tail, space).ok()?;
    let rows: Vec<WalletNoteRowV1> = rows
        .into_iter()
        .map(
            |(name_first_b58, name_last_b58, version, assets)| WalletNoteRowV1 {
                name_first_b58,
                name_last_b58,
                version,
                assets,
            },
        )
        .collect();
    Some(WalletEvent::NotesListV1 {
        height,
        block_id_b58,
        filter_address,
        rows,
    })
}

fn decode_notes_advanced_v1(noun: Noun, space: &NounSpace) -> Option<WalletEvent> {
    let tail = expect_v1_payload(noun, space)?;
    let (filter_address, height, block_id_b58, rows) =
        <(String, u64, String, Vec<(String, String, u64, u64)>)>::from_noun(&tail, space).ok()?;
    let rows: Vec<WalletNoteRowV1> = rows
        .into_iter()
        .map(
            |(name_first_b58, name_last_b58, version, assets)| WalletNoteRowV1 {
                name_first_b58,
                name_last_b58,
                version,
                assets,
            },
        )
        .collect();
    Some(WalletEvent::NotesListV1 {
        height,
        block_id_b58,
        filter_address: Some(filter_address),
        rows,
    })
}

fn decode_addresses_v1(noun: Noun, space: &NounSpace) -> Option<WalletEvent> {
    let tail = expect_v1_payload(noun, space)?;
    let (list_kind, rows) = <(String, Vec<(String, u64)>)>::from_noun(&tail, space).ok()?;
    Some(WalletEvent::AddressListV1 {
        list_kind,
        rows: rows
            .into_iter()
            .map(|(address_b58, version)| WalletAddressRowV1 {
                address_b58,
                version,
            })
            .collect(),
    })
}

fn decode_key_tree_v1(noun: Noun, space: &NounSpace) -> Option<WalletEvent> {
    let tail = expect_v1_payload(noun, space)?;
    let (include_values, nodes) =
        <(u64, Vec<(String, String, Option<String>)>)>::from_noun(&tail, space).ok()?;
    Some(WalletEvent::KeyTreeV1 {
        include_values: include_values != 0,
        nodes: nodes
            .into_iter()
            .map(|(path, label, pubkey_b58)| WalletKeyTreeNodeV1 {
                path,
                label,
                pubkey_b58,
            })
            .collect(),
    })
}

fn decode_keygen_v1(noun: Noun, space: &NounSpace) -> Option<WalletEvent> {
    let tail = expect_v1_payload(noun, space)?;
    let (message, paths, pubkeys_b58) =
        <(String, Vec<String>, Vec<String>)>::from_noun(&tail, space).ok()?;
    Some(WalletEvent::KeygenV1(WalletKeygenV1 {
        message,
        paths,
        pubkeys_b58,
    }))
}

fn wallet_data_from_hooks(hooks: &DispatchHooks) -> WalletCommandData {
    let events = hooks
        .wallet_events
        .as_ref()
        .map(|e| e.lock().unwrap().clone())
        .unwrap_or_default();
    WalletCommandData { events }
}

fn print_cli_markdown(markdown: &str) {
    let skin = MadSkin::default_dark();
    println!("{}", skin.term_text(markdown));
}

fn present_migrate_summary(
    hooks: &DispatchHooks,
    summary: &crate::create_tx::MigrateV0NotesSummary,
    markdown: &str,
) {
    if hooks.wallet_events.is_some() {
        if let Some(ref ev) = hooks.wallet_events {
            ev.lock().unwrap().push(migrate_summary_event(summary));
        }
    } else {
        print_cli_markdown(markdown);
    }
}

/// Capture `[%wallet …]` into structured [`WalletEvent`]s (TUI / API). Ignores `%markdown`.
pub(crate) fn wallet_effect_driver(wallet_events: Arc<Mutex<Vec<WalletEvent>>>) -> IODriverFn {
    make_driver(move |handle| {
        let wallet_events = Arc::clone(&wallet_events);
        async move {
            loop {
                match handle.next_effect().await {
                    Ok(effect) => {
                        let space = effect.noun_space();
                        let root = unsafe { effect.root() };
                        if let Some(structured) = try_wallet_effect_event(*root, &space) {
                            wallet_events.lock().unwrap().push(structured);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error in structured effect driver: {:?}", e);
                        continue;
                    }
                }
            }
        }
    })
}

/// Append **raw** markdown kernel cords to `sink` (legacy CLI capture path).
pub(crate) fn markdown_capture_driver(sink: Arc<Mutex<String>>) -> IODriverFn {
    make_driver(move |handle| {
        let sink = Arc::clone(&sink);
        async move {
            loop {
                match handle.next_effect().await {
                    Ok(effect) => {
                        let space = effect.noun_space();
                        let root = unsafe { effect.root() };
                        let Ok(effect_cell) = root.in_space(&space).as_cell() else {
                            continue;
                        };
                        if unsafe { effect_cell.head().noun().raw_equals(&D(tas!(b"markdown"))) } {
                            let markdown_text = effect_cell.tail().noun();
                            let text = if let Ok(atom) = markdown_text.in_space(&space).as_atom() {
                                String::from_utf8_lossy(&atom.to_bytes_until_nul()?).to_string()
                            } else {
                                tracing::error!("Failed to convert markdown text to string");
                                continue;
                            };
                            tracing::debug!("Markdown text (captured raw): {}", text);
                            let mut g = sink.lock().unwrap();
                            if !g.is_empty() && !text.is_empty() {
                                g.push_str("\n\n");
                            }
                            g.push_str(&text);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error in markdown capture driver: {:?}", e);
                        continue;
                    }
                }
            }
        }
    })
}

async fn add_markdown_io_driver(wallet: &mut Wallet, hooks: &DispatchHooks) {
    if let Some(events) = hooks.wallet_events.clone() {
        wallet
            .app
            .add_io_driver(wallet_effect_driver(events))
            .await;
        if let Some(sink) = hooks.markdown_capture.clone() {
            wallet
                .app
                .add_io_driver(markdown_capture_driver(sink))
                .await;
        }
    } else if let Some(sink) = hooks.markdown_capture.clone() {
        wallet
            .app
            .add_io_driver(markdown_capture_driver(sink))
            .await;
    } else {
        wallet.app.add_io_driver(markdown_driver()).await;
    }
}

/// `one_punch` is always added per command; file / effect drivers install once in TUI.
async fn add_kernel_io_drivers(wallet: &mut Wallet, hooks: &DispatchHooks) {
    let is_tui = hooks.wallet_events.is_some();
    if is_tui && wallet.tui_io_drivers_installed {
        return;
    }
    wallet.app.add_io_driver(file_driver()).await;
    add_markdown_io_driver(wallet, hooks).await;
    if is_tui {
        wallet
            .app
            .add_io_driver(complete_run_on_exit_driver())
            .await;
        wallet.tui_io_drivers_installed = true;
    } else {
        wallet.app.add_io_driver(exit_driver()).await;
    }
}

/// Whether this command needs balance sync before running.
pub(crate) fn command_requires_sync(command: &Commands) -> bool {
    match command {
        Commands::Keygen
        | Commands::DeriveChild { .. }
        | Commands::ImportKeys { .. }
        | Commands::ExportKeys
        | Commands::SignMessage { .. }
        | Commands::VerifyMessage { .. }
        | Commands::SignHash { .. }
        | Commands::VerifyHash { .. }
        | Commands::ExportMasterPubkey
        | Commands::ImportMasterPubkey { .. }
        | Commands::ListActiveAddresses
        | Commands::SetActiveMasterAddress { .. }
        | Commands::ListMasterAddresses
        | Commands::ShowSeedphrase
        | Commands::ShowMasterZPub
        | Commands::ShowMasterZPrv
        | Commands::ShowKeyTree { .. }
        | Commands::ShowTx { .. }
        | Commands::SignMultisigTx { .. }
        | Commands::Watch { .. }
        | Commands::TxAccepted { .. } => false,
        _ => true,
    }
}

fn build_initial_poke(command: &Commands) -> CommandNoun<NounSlab> {
    match command {
        Commands::TxAccepted { .. } => Err(NockAppError::from(
            CrownError::Unknown("internal: invalid command for poke builder".into()),
        )),
        Commands::Keygen => {
            let mut entropy = [0u8; 32];
            let mut salt = [0u8; 16];
            getrandom::fill(&mut entropy).map_err(|e| CrownError::Unknown(e.to_string()))?;
            getrandom::fill(&mut salt).map_err(|e| CrownError::Unknown(e.to_string()))?;
            Wallet::keygen(&entropy, &salt)
        }
        Commands::DeriveChild {
            index,
            hardened,
            label,
        } => Wallet::derive_child(*index, *hardened, label),
        Commands::SignMessage {
            message,
            message_file,
            message_pos,
            index,
            hardened,
        } => {
            let bytes = if let Some(m) = message.clone().or(message_pos.clone()) {
                m.as_bytes().to_vec()
            } else if let Some(path) = message_file {
                fs::read(path).map_err(|e| {
                    CrownError::Unknown(format!("Failed to read message file: {}", e))
                })?
            } else {
                return Err(CrownError::Unknown(
                    "either --message or --message-file must be provided".into(),
                )
                .into());
            };
            Wallet::sign_message(&bytes, *index, *hardened)
        }
        Commands::SignHash {
            hash_b58,
            index,
            hardened,
        } => Wallet::sign_hash(hash_b58, *index, *hardened),
        Commands::VerifyMessage {
            message,
            message_file,
            message_pos,
            signature_path,
            signature_pos,
            pubkey,
            pubkey_pos,
        } => {
            let msg_bytes = if let Some(m) = message.clone().or(message_pos.clone()) {
                m.as_bytes().to_vec()
            } else if let Some(path) = message_file {
                fs::read(path).map_err(|e| {
                    CrownError::Unknown(format!("Failed to read message file: {}", e))
                })?
            } else {
                return Err(CrownError::Unknown(
                    "either --message or --message-file must be provided".into(),
                )
                .into());
            };
            let sig_path = signature_path
                .clone()
                .or(signature_pos.clone())
                .ok_or_else(|| {
                    NockAppError::from(CrownError::Unknown(
                        "--signature or SIGNATURE_FILE positional is required".into(),
                    ))
                })?;
            let pk_b58 = pubkey.clone().or(pubkey_pos.clone()).ok_or_else(|| {
                NockAppError::from(CrownError::Unknown(
                    "--pubkey or PUBKEY positional is required".into(),
                ))
            })?;

            let sig_bytes = fs::read(sig_path)
                .map_err(|e| CrownError::Unknown(format!("Failed to read signature: {}", e)))?;
            Wallet::verify_message(&msg_bytes, &sig_bytes, &pk_b58)
        }
        Commands::VerifyHash {
            hash_b58,
            signature_path,
            signature_pos,
            pubkey,
            pubkey_pos,
        } => {
            let sig_path = signature_path
                .clone()
                .or(signature_pos.clone())
                .ok_or_else(|| {
                    NockAppError::from(CrownError::Unknown(
                        "--signature or SIGNATURE_FILE positional is required".into(),
                    ))
                })?;
            let pk_b58 = pubkey.clone().or(pubkey_pos.clone()).ok_or_else(|| {
                NockAppError::from(CrownError::Unknown(
                    "--pubkey or PUBKEY positional is required".into(),
                ))
            })?;
            let sig_bytes = fs::read(sig_path)
                .map_err(|e| CrownError::Unknown(format!("Failed to read signature: {}", e)))?;
            Wallet::verify_hash(hash_b58, &sig_bytes, &pk_b58)
        }
        Commands::ImportKeys {
            file,
            key,
            seedphrase,
            version,
        } => {
            if let Some(file_path) = file {
                Wallet::import_keys(file_path)
            } else if let Some(extended_key) = key {
                Wallet::import_extended(extended_key)
            } else if let Some(seed) = seedphrase {
                let version = version.ok_or_else(|| {
                    NockAppError::from(CrownError::Unknown(
                        "--version is required when using --seedphrase".into(),
                    ))
                })?;
                let normalized_seed = seed.split_whitespace().collect::<Vec<&str>>().join(" ");
                Wallet::import_seed_phrase(&normalized_seed, version)
            } else {
                return Err(CrownError::Unknown(
                    "One of --file, --key, --seedphrase, or --master-privkey must be provided for import-keys".to_string(),
                )
                .into());
            }
        }
        Commands::Watch { subcommand } => match subcommand {
            WatchSubcommand::Address { address } => match normalize_watch_address(address.clone())?
            {
                Some(normalized) => Wallet::watch_address(&normalized),
                None => {
                    return Err(
                        CrownError::Unknown("Invalid watch identifier provided".into()).into(),
                    );
                }
            },
            WatchSubcommand::Pubkey { pubkey } => match normalize_watch_address(pubkey.clone())? {
                Some(normalized) => Wallet::watch_address(&normalized),
                None => {
                    return Err(CrownError::Unknown("Invalid pubkey provided".into()).into());
                }
            },
            WatchSubcommand::Multisig {
                threshold,
                participants,
            } => Wallet::watch_multisig(*threshold, participants),
        },
        Commands::ExportKeys => Wallet::export_keys(),
        Commands::ListNotes => Wallet::list_notes(),
        Commands::ListNotesByAddress { address } => {
            if let Some(pk) = address {
                Wallet::list_notes_by_address(pk)
            } else {
                return Err(CrownError::Unknown("Address is required".into()).into());
            }
        }
        Commands::ListNotesByAddressCsv { address } => Wallet::list_notes_by_address_csv(address),
        Commands::CreateTx { .. } => Wallet::show_balance(),
        Commands::MigrateV0Notes { .. } => Wallet::show_balance(),
        Commands::SignMultisigTx {
            transaction,
            sign_keys,
        } => Wallet::sign_multisig_tx(transaction, sign_keys.as_deref()),
        Commands::SendTx { transaction } => Wallet::send_tx(transaction),
        Commands::ShowTx { transaction } => Wallet::show_tx(transaction),
        Commands::ShowBalance => Wallet::show_balance(),
        Commands::ExportMasterPubkey => Wallet::export_master_pubkey(),
        Commands::ImportMasterPubkey { key_path } => Wallet::import_master_pubkey(key_path),
        Commands::ListActiveAddresses => Wallet::list_active_addresses(),
        Commands::SetActiveMasterAddress { address_b58 } => {
            Wallet::set_active_master_address(address_b58)
        }
        Commands::ListMasterAddresses => Wallet::list_master_addresses(),
        Commands::ShowSeedphrase => Wallet::show_seed_phrase(),
        Commands::ShowMasterZPub => Wallet::show_master_pubkey(),
        Commands::ShowMasterZPrv => Wallet::show_master_privkey(),
        Commands::ShowKeyTree { include_values } => Wallet::show_key_tree(*include_values),
    }
}

/// Run a single wallet command (sync, planner branches, kernel I/O).
pub async fn execute_wallet_command(
    connection: &ConnectionCli,
    wallet: &mut Wallet,
    command: &Commands,
    synced_snapshot_for_planner: &mut Option<NormalizedSnapshot>,
    use_spinner: bool,
    hooks: DispatchHooks,
) -> WalletCommandOutcome {
    let mut poke = build_initial_poke(command)?;

    if command_requires_sync(command) {
        let pb = if use_spinner {
            let pb = ProgressBar::new_spinner();
            pb.enable_steady_tick(Duration::from_millis(100));
            pb.set_message("Syncing balance with Nockchain…");
            Some(pb)
        } else {
            None
        };

        info!(
            "Command requires syncing the current balance, connecting to Nockchain gRPC server..."
        );
        let mut pubkey_peek_slab = NounSlab::new();
        let tracked_tag = make_tas_util(&mut pubkey_peek_slab, "tracked-pubkeys").as_noun();
        let path = T(&mut pubkey_peek_slab, &[tracked_tag, SIG]);
        pubkey_peek_slab.set_root(path);
        let pubkey_slab = wallet.app.peek_handle(pubkey_peek_slab).await?;

        let mut first_name_peek_slab = NounSlab::new();
        let tracked_tag = make_tas_util(&mut first_name_peek_slab, "tracked-names").as_noun();
        let path = T(&mut first_name_peek_slab, &[tracked_tag, SIG]);
        first_name_peek_slab.set_root(path);
        let first_name_slab = wallet.app.peek_handle(first_name_peek_slab).await?;

        let pubkeys = if let Some(pubkey_slab) = pubkey_slab {
            pubkey_slab
                .to_vec()
                .iter()
                .map(|key| {
                    let space = key.noun_space();
                    String::from_noun(unsafe { key.root() }, &space)
                })
                .collect::<Result<Vec<String>, NounDecodeError>>()?
                .into_iter()
                .filter_map(|value| match normalize_watch_address(value) {
                    Ok(Some(normalized)) => Some(Ok(normalized)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                })
                .collect::<Result<Vec<String>, NockAppError>>()?
        } else {
            Vec::new()
        };

        let first_names: Vec<String> = if let Some(name_slab) = first_name_slab {
            let names_noun = unsafe { name_slab.root() };
            let name_space = name_slab.noun_space();
            <Vec<String>>::from_noun(names_noun, &name_space)?
        } else {
            Vec::new()
        };

        let connection_target = connection.target();
        const MAX_SYNC_RETRIES: usize = 5;
        let mut attempt: usize = 0;
        let sync_result = loop {
            attempt += 1;
            if let Some(ref tx) = hooks.sync_attempt {
                let _ = tx.send((attempt, MAX_SYNC_RETRIES));
            }
            if let Some(ref bar) = pb {
                bar.set_message(format!(
                    "Syncing balance with Nockchain… (attempt {}/{})",
                    attempt, MAX_SYNC_RETRIES
                ));
            }
            match crate::connection::sync_wallet_balance(
                wallet,
                &connection_target,
                pubkeys.clone(),
                first_names.clone(),
            )
            .await
            {
                Ok(r) => break r,
                Err(e) if attempt < MAX_SYNC_RETRIES && is_transient_sync_error(&e) => {
                    let pow = (attempt as u32).saturating_sub(1).min(4);
                    let delay = Duration::from_millis(250_u64.saturating_mul(1_u64 << pow));
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        };

        if let Some(pb) = pb {
            pb.finish_and_clear();
        }

        *synced_snapshot_for_planner = sync_result.normalized_snapshot;

        for poke_sync in sync_result.pokes {
            let _ = wallet
                .app
                .poke(SystemWire.to_wire(), poke_sync)
                .await
                .expect("poke should succeed");
        }
    }

    if let Commands::MigrateV0Notes { destination } = command {
        let snap = synced_snapshot_for_planner.as_ref().cloned();
        let mut prepared = wallet
            .prepare_migrate_v0_notes_per_signer(snap, destination.clone())
            .await?;
        if prepared.summary.created_count == 0 {
            let markdown = Wallet::format_migrate_v0_notes_summary(&prepared.summary);
            present_migrate_summary(&hooks, &prepared.summary, &markdown);
            return Err(NockAppError::OtherError(
                "No v0 migration transactions were created".to_string(),
            ));
        }

        let tx_dir = Path::new("txs");
        let before = Wallet::snapshot_written_txs(tx_dir).await?;
        let (noun, operation) = prepared.take_poke().ok_or_else(|| {
            NockAppError::from(CrownError::Unknown(
                "migrate-v0-notes prepared migration transactions but did not produce a batch create poke"
                    .to_string(),
            ))
        })?;
        wallet
            .app
            .add_io_driver(one_punch_driver(noun, operation))
            .await;
        add_kernel_io_drivers(wallet, &hooks).await;

        let pb_run = if use_spinner {
            let pb = ProgressBar::new_spinner();
            pb.enable_steady_tick(Duration::from_millis(100));
            pb.set_message("Running migration…");
            Some(pb)
        } else {
            None
        };

        let run_result = wallet.app.run().await;

        if let Some(pb) = pb_run {
            pb.finish_and_clear();
        }

        match run_result {
            Ok(_) => {
                *synced_snapshot_for_planner = None;
                let after = Wallet::snapshot_written_txs(tx_dir).await?;
                let tx_paths = Wallet::detect_written_tx_paths(&before, &after)?;
                let summary = prepared.finalize(tx_paths)?;
                let markdown = Wallet::format_migrate_v0_notes_summary(&summary);
                present_migrate_summary(&hooks, &summary, &markdown);
                info!("Command executed successfully");
                Ok(wallet_data_from_hooks(&hooks))
            }
            Err(e) => {
                error!("Command failed: {}", e);
                Err(e)
            }
        }
    } else if let Commands::CreateTx {
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
    } = command
    {
        let recipient_specs = recipient_tokens_to_specs(recipients.clone())?;
        let signing_keys = Wallet::collect_signing_keys(*index, *hardened, sign_keys)?;
        let pb = if use_spinner {
            let pb = ProgressBar::new_spinner();
            pb.enable_steady_tick(Duration::from_millis(100));
            pb.set_message("Planning transaction…");
            Some(pb)
        } else {
            None
        };
        let snap_for_planner = synced_snapshot_for_planner.as_ref().cloned();
        poke = wallet
            .create_tx_with_planner(
                snap_for_planner,
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
        if let Some(pb) = pb {
            pb.finish_and_clear();
        }

        wallet
            .app
            .add_io_driver(one_punch_driver(poke.0, poke.1))
            .await;
        add_kernel_io_drivers(wallet, &hooks).await;

        let pb_run = if use_spinner {
            let pb = ProgressBar::new_spinner();
            pb.enable_steady_tick(Duration::from_millis(100));
            pb.set_message("Executing wallet…");
            Some(pb)
        } else {
            None
        };
        let run_result = wallet.app.run().await;
        if let Some(pb) = pb_run {
            pb.finish_and_clear();
        }
        match run_result {
            Ok(_) => {
                *synced_snapshot_for_planner = None;
                info!("Command executed successfully");
                Ok(wallet_data_from_hooks(&hooks))
            }
            Err(e) => {
                error!("Command failed: {}", e);
                Err(e)
            }
        }
    } else {
        wallet
            .app
            .add_io_driver(one_punch_driver(poke.0, poke.1))
            .await;
        add_kernel_io_drivers(wallet, &hooks).await;

        let pb_run = if use_spinner {
            let pb = ProgressBar::new_spinner();
            pb.enable_steady_tick(Duration::from_millis(100));
            pb.set_message("Executing wallet…");
            Some(pb)
        } else {
            None
        };
        let run_result = wallet.app.run().await;
        if let Some(pb) = pb_run {
            pb.finish_and_clear();
        }
        match run_result {
            Ok(_) => {
                info!("Command executed successfully");
                Ok(wallet_data_from_hooks(&hooks))
            }
            Err(e) => {
                error!("Command failed: {}", e);
                Err(e)
            }
        }
    }
}
