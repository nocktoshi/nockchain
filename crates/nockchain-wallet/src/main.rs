#![allow(clippy::doc_overindented_list_items)]
// Allow architectural patterns that would be disruptive to change
#![allow(clippy::io_other_error)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::unnecessary_fallible_conversions)]
#![allow(clippy::result_large_err)]
#![allow(clippy::empty_line_after_doc_comments)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::unused_enumerate_index)]
#![allow(clippy::option_as_ref_cloned)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod command;
mod connection;
mod create_tx;
mod dispatch;
mod error;
mod recipient;
mod tui;
#[cfg(test)]
mod tests;

pub(crate) use tui::wallet_outcome;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use clap::Parser;
#[cfg(test)]
use command::TimelockRangeCli;
#[cfg(test)]
use command::WalletWire;
use command::{ClientType, CommandNoun, Commands, NoteSelectionStrategyCli, WalletCli};
use kernels_open_wallet::KERNEL;
use nockapp::driver::*;
use nockapp::drivers::one_punch::OnePunchWire;
use nockapp::kernel::boot::{self, NockStackSize};
use nockapp::noun::slab::{NockJammer, NounSlab};
use nockapp::utils::bytes::Byts;
use nockapp::utils::make_tas;
use nockapp::wire::Wire;
use nockapp::{system_data_dir, CrownError, NockApp, NockAppError, ToBytesExt};
use nockapp_grpc::pb::common::v1::Base58Hash as PbBase58Hash;
use nockapp_grpc::pb::public::v2::transaction_accepted_response;
use nockapp_grpc::{private_nockapp, public_nockchain};
use nockchain_types::common::{Hash, SchnorrPubkey, TimelockRangeAbsolute, TimelockRangeRelative};
use nockchain_types::tx_engine::common::Name;
use nockchain_types::tx_engine::v1::tx::{LockPrimitive, SpendCondition};
use nockchain_types::{default_fakenet_blockchain_constants, v0, v1};
use nockvm::jets::cold::Nounable;
use nockvm::noun::{Atom, Cell, IndirectAtom, Noun, NounAllocator, D, NO, SIG, T, YES};
use noun_serde::prelude::*;
#[cfg(test)]
use recipient::BRIDGE_LOCK_ROOT_DEFAULT_B58;
use recipient::{planner_recipient_outputs, planner_refund_output_template, RecipientSpec};
use termimad::MadSkin;
use tokio::fs as tokio_fs;
use tracing::{info, warn};
use wallet_tx_builder::adapter::{
    normalize_balance_pages, NormalizeSnapshotError, NormalizedSnapshot, SnapshotConsistencyError,
};
use wallet_tx_builder::lock_resolver::LockMatcher;
use wallet_tx_builder::planner::{plan_create_tx, PlanError};
use wallet_tx_builder::types::{
    CandidateVersionPolicy, ChainContext, PlanRequest, PlanningMode, SelectionMode, SelectionOrder,
};
use zkvm_jetpack::hot::produce_prover_hot_state;

use crate::public_nockchain::v2::client::BalanceRequest;

#[tokio::main]
async fn main() -> Result<(), NockAppError> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("default provider already set elsewhere");

    let mut cli = WalletCli::parse();
    // Use a smaller stack size for the wallet
    cli.boot.stack_size = NockStackSize::Tiny;

    if std::env::var("RUST_LOG").is_err() {
        if matches!(cli.command, Commands::Tui) {
            if cli.verbose {
                std::env::set_var(
                    "RUST_LOG", "info,nockapp=info,nockchain_wallet=info,opentelemetry_sdk=off",
                );
            } else {
                std::env::set_var(
                    "RUST_LOG",
                    "warn,nockapp=warn,nockchain_wallet=warn,tonic=warn,h2=warn,tower=warn,hyper=warn,rustls=warn,opentelemetry_sdk=off",
                );
            }
        }
    }

    boot::init_default_tracing(&cli.boot.clone()); // Init tracing early

    if let Commands::TxAccepted { tx_id } = &cli.command {
        return run_transaction_accepted(&cli.connection, tx_id).await;
    }

    let prover_hot_state = produce_prover_hot_state();
    let data_dir = wallet_data_dir().await?;

    let kernel = boot::setup(
        KERNEL,
        cli.boot.clone(),
        prover_hot_state.as_slice(),
        "wallet",
        Some(data_dir.clone()),
    )
    .await
    .map_err(|e| CrownError::Unknown(format!("Kernel setup failed: {}", e)))?;

    let mut wallet = Wallet::new(kernel);
    let mut synced_snapshot_for_planner: Option<NormalizedSnapshot> = None;

    if cli.fakenet {
        wallet.set_fakenet().await?;
    } else if wallet.is_fakenet().await? {
        return Err(NockAppError::OtherError(
            "Attempted to boot the wallet in mainnet mode, but the loaded state is in fakenet mode. Please use the --fakenet flag to boot the wallet or boot the wallet with the --new flag to create a new mainnet wallet".to_string(),
        ));
    }

    if matches!(cli.command, Commands::Tui) {
        return tui::run(&cli, wallet, synced_snapshot_for_planner, data_dir).await;
    }

    // CLI one-shot: markdown renders via `markdown_driver` during `app.run()` (default hooks).
    // `WalletCommandData` in the return value is unused for stdout; TUI/API consume structured events.
    crate::dispatch::execute_wallet_command(
        &cli,
        &mut wallet,
        &cli.command,
        &mut synced_snapshot_for_planner,
        false,
        crate::dispatch::DispatchHooks::cli(),
    )
    .await
    .map(|_| ())
}

/// Wallet runtime wrapper around the underlying nockapp kernel.
pub struct Wallet {
    app: NockApp,
    /// TUI: `file` / markdown / exit-completion drivers are registered once for the session.
    pub(crate) tui_io_drivers_installed: bool,
}

impl Wallet {
    /// Creates a new `Wallet` instance with the given kernel.
    ///
    /// This wraps the kernel in a NockApp, which exposes a substrate
    /// for kernel interaction with IO driver semantics.
    ///
    /// # Arguments
    ///
    /// * `kernel` - The kernel to initialize the wallet with.
    ///
    /// # Returns
    ///
    /// A new `Wallet` instance with the kernel initialized
    /// as a NockApp.
    fn new(nockapp: NockApp) -> Self {
        Wallet {
            app: nockapp,
            tui_io_drivers_installed: false,
        }
    }

    /// Applies the shared Rust fakenet constants so wallet state matches node fakenet defaults.
    async fn set_fakenet(&mut self) -> Result<(), NockAppError> {
        let mut slab = NounSlab::new();
        let constants = default_fakenet_blockchain_constants();
        let constants_noun = constants.to_noun(&mut slab);
        let (poke, _) = Self::wallet("fakenet", &[constants_noun], Operation::Poke, &mut slab)?;
        let wire = OnePunchWire::Poke.to_wire();
        let _ = self.app.poke(wire, poke).await?;
        Ok(())
    }

    /// Reads whether current wallet state was initialized in fakenet mode.
    async fn is_fakenet(&mut self) -> Result<bool, NockAppError> {
        let mut slab = NounSlab::new();
        let tag = String::from("fakenet").to_noun(&mut slab);
        slab.modify(|_| vec![tag, SIG]);
        let result = self.app.peek(slab).await?;
        let is_fakenet: Option<Option<bool>> =
            unsafe { <Option<Option<bool>>>::from_noun(result.root(), &result.noun_space())? };
        match is_fakenet {
            Some(Some(res)) => Ok(res),
            _ => Err(NockAppError::OtherError(
                "Unexpected result from is_fakenet".to_string(),
            )),
        }
    }

    /// Prepares a wallet command for execution.
    ///
    /// # Arguments
    ///
    /// * `command` - The command to execute.
    /// * `args` - The arguments for the command.
    /// * `operation` - The operation type (Poke or Peek).
    /// * `slab` - The NounSlab to use for the command.
    ///
    /// # Returns
    ///
    /// A `CommandNoun` containing the prepared NounSlab and operation.
    fn wallet(
        command: &str,
        args: &[Noun],
        operation: Operation,
        slab: &mut NounSlab,
    ) -> CommandNoun<NounSlab> {
        let head = make_tas(slab, command).as_noun();

        let tail = match args.len() {
            0 => D(0),
            1 => args[0],
            _ => T(slab, args),
        };

        let full = T(slab, &[head, tail]);

        slab.set_root(full);
        Ok((slab.clone(), operation))
    }

    /// Generates a new key pair. Will be a version 0 key until the wallet supports v1 transactions
    ///
    /// # Arguments
    ///
    /// * `entropy` - The entropy to use for key generation.
    /// * `sal` - The salt to use for key generation.
    fn keygen(entropy: &[u8; 32], sal: &[u8; 16]) -> CommandNoun<NounSlab> {
        let mut slab: NounSlab<NockJammer> = NounSlab::new();
        let ent: Byts = Byts::new(entropy.to_vec());
        let ent_noun = ent.into_noun(&mut slab);
        let sal: Byts = Byts::new(sal.to_vec());
        let sal_noun = sal.into_noun(&mut slab);
        Self::wallet("keygen", &[ent_noun, sal_noun], Operation::Poke, &mut slab)
    }

    ///// Updates the keys in the wallet.
    /////
    ///// # Arguments
    /////
    ///// * `entropy` - The entropy to use for key generation.
    ///// * `salt` - The salt to use for key generation.
    //fn upgrade_keys(entropy: &[u8; 32], salt: &[u8; 16]) -> CommandNoun<NounSlab> {
    //    let mut slab = NounSlab::new();
    //    let ent: Byts = Byts::new(entropy.to_vec());
    //    let ent_noun = ent.into_noun(&mut slab);
    //    let sal: Byts = Byts::new(salt.to_vec());
    //    let sal_noun = sal.into_noun(&mut slab);
    //    Self::wallet(
    //        "upgrade-keys-v2",
    //        &[ent_noun, sal_noun],
    //        Operation::Poke,
    //        &mut slab,
    //    )
    //}

    /// Derives a child key from the current master key path.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the child key to derive.
    /// * `hardened` - Whether the child key should be hardened.
    /// * `label` - Optional label persisted alongside the derived key.
    fn derive_child(index: u64, hardened: bool, label: &Option<String>) -> CommandNoun<NounSlab> {
        let mut slab: NounSlab<NockJammer> = NounSlab::new();
        let index_noun = D(index);
        let hardened_noun = if hardened { YES } else { NO };
        let label_noun = label.as_ref().map_or(SIG, |l| {
            let label_noun = l.into_noun(&mut slab);
            T(&mut slab, &[SIG, label_noun])
        });

        Self::wallet(
            "derive-child",
            &[index_noun, hardened_noun, label_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Signs a transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction_path` - Path to the transaction file
    /// * `index` - Optional index of the key to use for signing
    #[allow(dead_code)]
    fn sign_tx(
        transaction_path: &str,
        index: Option<u64>,
        hardened: bool,
    ) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        // Validate index is within range (though clap should prevent this)
        if let Some(idx) = index {
            if idx >= 2 << 31 {
                return Err(
                    CrownError::Unknown("Key index must not exceed 2^31 - 1".into()).into(),
                );
            }
        }

        // Read and decode the input bundle
        let transaction_data = fs::read(transaction_path)
            .map_err(|e| CrownError::Unknown(format!("Failed to read transaction: {}", e)))?;

        // Convert the bundle data into a noun using cue
        let transaction_noun = slab
            .cue_into(transaction_data.as_bytes()?)
            .map_err(|e| CrownError::Unknown(format!("Failed to decode transaction: {}", e)))?;

        // Format information about signing key
        let sign_key_noun = match index {
            Some(i) => {
                let inner = D(i);
                let hardened_noun = if hardened { YES } else { NO };
                T(&mut slab, &[D(0), inner, hardened_noun])
            }
            None => SIG,
        };

        // Generate random entropy
        let mut entropy_bytes = [0u8; 32];
        getrandom::fill(&mut entropy_bytes).map_err(|e| CrownError::Unknown(e.to_string()))?;
        let entropy = from_bytes(&mut slab, &entropy_bytes).as_noun();

        Self::wallet(
            "sign-tx",
            &[transaction_noun, sign_key_noun, entropy],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Signs an arbitrary message payload with the requested signing key.
    fn sign_message(
        message_bytes: &[u8],
        index: Option<u64>,
        hardened: bool,
    ) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        if let Some(idx) = index {
            if idx >= 2 << 31 {
                return Err(
                    CrownError::Unknown("Key index must not exceed 2^31 - 1".into()).into(),
                );
            }
        }

        let msg_atom = from_bytes(&mut slab, message_bytes).as_noun();

        let sign_key_noun = match index {
            Some(i) => {
                let inner = D(i);
                let hardened_noun = if hardened { YES } else { NO };
                T(&mut slab, &[D(0), inner, hardened_noun])
            }
            None => SIG,
        };

        Self::wallet(
            "sign-message",
            &[msg_atom, sign_key_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Verifies a signature over an arbitrary message payload.
    fn verify_message(
        message_bytes: &[u8],
        signature_jam: &[u8],
        pubkey_b58: &str,
    ) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let msg_atom = from_bytes(&mut slab, message_bytes).as_noun();
        let sig_atom = from_bytes(&mut slab, signature_jam).as_noun();
        let pk_noun = make_tas(&mut slab, pubkey_b58).as_noun();

        Self::wallet(
            "verify-message",
            &[msg_atom, sig_atom, pk_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Signs a base58 tip5 hash directly without message prehashing.
    fn sign_hash(hash_b58: &str, index: Option<u64>, hardened: bool) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        if let Some(idx) = index {
            if idx >= 2 << 31 {
                return Err(
                    CrownError::Unknown("Key index must not exceed 2^31 - 1".into()).into(),
                );
            }
        }

        let hash_noun = make_tas(&mut slab, hash_b58).as_noun();
        let sign_key_noun = match index {
            Some(i) => {
                let inner = D(i);
                let hardened_noun = if hardened { YES } else { NO };
                T(&mut slab, &[D(0), inner, hardened_noun])
            }
            None => SIG,
        };

        Self::wallet(
            "sign-hash",
            &[hash_noun, sign_key_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Verifies a signature over a base58 tip5 hash.
    fn verify_hash(
        hash_b58: &str,
        signature_jam: &[u8],
        pubkey_b58: &str,
    ) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let hash_noun = make_tas(&mut slab, hash_b58).as_noun();
        let sig_atom = from_bytes(&mut slab, signature_jam).as_noun();
        let pk_noun = make_tas(&mut slab, pubkey_b58).as_noun();

        Self::wallet(
            "verify-hash",
            &[hash_noun, sig_atom, pk_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Imports keys from a seed phrase.
    ///
    /// # Arguments
    ///
    /// * `seed_phrase` - The seed phrase to generate the master private key from.
    /// * `version` - The version tag to attach to the generated master key.
    fn import_seed_phrase(seed_phrase: &str, version: u64) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let seed_phrase_noun = make_tas(&mut slab, seed_phrase).as_noun();
        let version_noun = D(version);
        Self::wallet(
            "import-seed-phrase",
            &[seed_phrase_noun, version_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Imports keys.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to jammed keys file
    fn import_keys(input_path: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        let key_data = fs::read(input_path)
            .map_err(|e| CrownError::Unknown(format!("Failed to read master pubkeys: {}", e)))?;

        let pubkey_noun = slab
            .cue_into(key_data.as_bytes()?)
            .map_err(|e| CrownError::Unknown(format!("Failed to decode master pubkeys: {}", e)))?;

        Self::wallet("import-keys", &[pubkey_noun], Operation::Poke, &mut slab)
    }

    /// Imports an extended key.
    ///
    /// # Arguments
    ///
    /// * `extended_key` - Extended key string (e.g., "zprv..." or "zpub...")
    fn import_extended(extended_key: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let key_noun = make_tas(&mut slab, extended_key).as_noun();
        Self::wallet("import-extended", &[key_noun], Operation::Poke, &mut slab)
    }

    /// Imports a watch-only public key.
    ///
    /// # Arguments
    ///
    /// * `watch_address` - Watch-only b58 encoded address. Can be v1 or v0.
    fn watch_address(watch_address: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let address_noun = make_tas(&mut slab, watch_address).as_noun();
        Self::wallet("watch-address", &[address_noun], Operation::Poke, &mut slab)
    }

    /// Imports a watch-only first name.
    ///
    /// # Arguments
    ///
    /// * `first_name` - Base58-encoded first name hash.
    #[allow(dead_code)]
    fn watch_first_name(first_name: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let first_name_noun = make_tas(&mut slab, first_name).as_noun();
        let lock_noun = SIG; // unit: no known lock provided
        Self::wallet(
            "watch-first-name",
            &[first_name_noun, lock_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Imports a watch-only multisig lock by its parameters.
    ///
    /// # Arguments
    ///
    /// * `m` - The M value of the multisig.
    /// * `pubkeys_str` - Comma-separated list of base58 pubkey hashes.
    fn watch_multisig(m: u64, pubkeys_str: &str) -> CommandNoun<NounSlab> {
        if m == 0 {
            return Err(
                CrownError::Unknown("m must be greater than 0 for multisig watch".into()).into(),
            );
        }

        let pubkey_hashes = Self::parse_pubkey_hashes(pubkeys_str)?;

        if m as usize > pubkey_hashes.len() {
            return Err(CrownError::Unknown(format!(
                "m ({}) cannot exceed number of pubkeys ({})",
                m,
                pubkey_hashes.len()
            ))
            .into());
        }

        let mut slab = NounSlab::new();
        let m_noun = D(m);
        let pubkeys_noun = pubkey_hashes.into_iter().rev().fold(D(0), |acc, hash| {
            let hash_b58 = hash.to_base58();
            let hash_noun = make_tas(&mut slab, &hash_b58).as_noun();
            Cell::new(&mut slab, hash_noun, acc).as_noun()
        });

        Self::wallet(
            "watch-address-multisig",
            &[m_noun, pubkeys_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Exports keys to a file.
    fn export_keys() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("export-keys", &[], Operation::Poke, &mut slab)
    }

    #[allow(dead_code)]
    /// Builds a kernel timelock intent from optional absolute/relative ranges.
    fn timelock_intent_from_ranges(
        absolute: Option<TimelockRangeAbsolute>,
        relative: Option<TimelockRangeRelative>,
    ) -> Option<v0::TimelockIntent> {
        if absolute.is_none() && relative.is_none() {
            None
        } else {
            Some(v0::TimelockIntent {
                absolute: absolute.unwrap_or_else(TimelockRangeAbsolute::none),
                relative: relative.unwrap_or_else(TimelockRangeRelative::none),
            })
        }
    }

    /// Parses `"[first last],[first last]"` note-name syntax used by create-tx.
    fn parse_note_names(raw: &str) -> Result<Vec<(String, String)>, NockAppError> {
        let mut names = Vec::new();

        for piece in raw.split(',') {
            let trimmed = piece.trim();
            if trimmed.is_empty() {
                continue;
            }

            if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
                return Err(CrownError::Unknown(format!(
                    "Invalid note name '{}', expected [first last]",
                    trimmed
                ))
                .into());
            }

            let inner = &trimmed[1..trimmed.len() - 1];
            let parts: Vec<&str> = inner.split_whitespace().collect();
            if parts.len() != 2 {
                return Err(CrownError::Unknown(format!(
                    "Invalid note name '{}', expected exactly two components",
                    trimmed
                ))
                .into());
            }

            let first = parts[0].to_string();
            let last = parts[1].to_string();
            names.push((first, last));
        }

        if names.is_empty() {
            return Err(
                CrownError::Unknown("At least one note name must be provided".to_string()).into(),
            );
        }

        Ok(names)
    }

    /// Resolves effective sign-key list from explicit `--sign-key` or index/hardened fallback.
    fn collect_signing_keys(
        index: Option<u64>,
        hardened: bool,
        sign_keys: &[String],
    ) -> Result<Vec<(u64, bool)>, NockAppError> {
        if !sign_keys.is_empty() {
            sign_keys
                .iter()
                .map(|entry| Self::parse_sign_key_entry(entry))
                .collect()
        } else if let Some(idx) = index {
            Ok(vec![(idx, hardened)])
        } else {
            Ok(Vec::new())
        }
    }

    /// Parses one `index[:hardened]` sign-key token from CLI input.
    fn parse_sign_key_entry(entry: &str) -> Result<(u64, bool), NockAppError> {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            return Err(CrownError::Unknown("Sign key entries cannot be empty".to_string()).into());
        }

        let (index_part, hardened_part) = trimmed
            .split_once(':')
            .map(|(index, hardened)| (index, Some(hardened)))
            .unwrap_or((trimmed, None));
        Self::parse_sign_key_components(index_part, hardened_part)
    }

    /// Lists all notes in the wallet.
    fn list_notes() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("list-notes", &[], Operation::Poke, &mut slab)
    }

    /// Exports the master public key.
    ///
    /// # Returns
    ///
    /// Retrieves and displays master public key and chaincode.
    fn export_master_pubkey() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("export-master-pubkey", &[], Operation::Poke, &mut slab)
    }

    /// Imports a master public key.
    ///
    /// # Arguments
    ///
    /// * `key` - Base58-encoded public key
    /// * `chain_code` - Base58-encoded chain code
    fn import_master_pubkey(input_path: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        let key_data = fs::read(input_path)
            .map_err(|e| CrownError::Unknown(format!("Failed to read master pubkeys: {}", e)))?;

        let pubkey_noun = slab
            .cue_into(key_data.as_bytes()?)
            .map_err(|e| CrownError::Unknown(format!("Failed to decode master pubkeys: {}", e)))?;

        Self::wallet(
            "import-master-pubkey",
            &[pubkey_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Creates a transaction from a transaction file.
    ///
    /// # Arguments
    ///
    /// * `transaction_path` - Path to the transaction file to create transaction from
    fn send_tx(transaction_path: &str) -> CommandNoun<NounSlab> {
        // Read and decode the transaction file
        let transaction_data = fs::read(transaction_path)
            .map_err(|e| CrownError::Unknown(format!("Failed to read transaction file: {}", e)))?;

        let mut slab = NounSlab::new();
        let transaction_noun = slab.cue_into(transaction_data.as_bytes()?).map_err(|e| {
            CrownError::Unknown(format!("Failed to decode transaction data: {}", e))
        })?;

        Self::wallet("send-tx", &[transaction_noun], Operation::Poke, &mut slab)
    }

    /// Displays a transaction file contents.
    ///
    /// # Arguments
    ///
    /// * `transaction_path` - Path to the transaction file to display
    fn show_tx(transaction_path: &str) -> CommandNoun<NounSlab> {
        // Read and decode the transaction file
        let transaction_data = fs::read(transaction_path)
            .map_err(|e| CrownError::Unknown(format!("Failed to read transaction file: {}", e)))?;

        let mut slab = NounSlab::new();
        let transaction_noun = slab.cue_into(transaction_data.as_bytes()?).map_err(|e| {
            CrownError::Unknown(format!("Failed to decode transaction data: {}", e))
        })?;

        Self::wallet("show-tx", &[transaction_noun], Operation::Poke, &mut slab)
    }

    /// Lists all addresses nested under the active master address.
    fn list_active_addresses() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("list-active-addresses", &[], Operation::Poke, &mut slab)
    }

    /// Sets the active master address.
    fn set_active_master_address(address_b58: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let address_noun = make_tas(&mut slab, address_b58).as_noun();
        Self::wallet(
            "set-active-master-address",
            &[address_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Lists known master addresses.
    fn list_master_addresses() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("list-master-addresses", &[], Operation::Poke, &mut slab)
    }

    /// Lists notes by public key
    fn list_notes_by_address(pubkey: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let pubkey_noun = make_tas(&mut slab, pubkey).as_noun();
        Self::wallet(
            "list-notes-by-address",
            &[pubkey_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Lists notes by public key in CSV format
    fn list_notes_by_address_csv(pubkey: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let pubkey_noun = make_tas(&mut slab, pubkey).as_noun();
        Self::wallet(
            "list-notes-by-address-csv",
            &[pubkey_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    /// Shows the aggregate wallet balance summary.
    fn show_balance() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        let balance_tag = make_tas(&mut slab, "balance").as_noun();
        let path_noun = Cell::new(&mut slab, balance_tag, D(0)).as_noun();

        Self::wallet("show", &[path_noun], Operation::Poke, &mut slab)
    }

    /// Shows the seed phrase for the current master key.
    fn show_seed_phrase() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("show-seed-phrase", &[], Operation::Poke, &mut slab)
    }

    /// Shows the master public key.
    fn show_master_pubkey() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("show-master-zpub", &[], Operation::Poke, &mut slab)
    }

    /// Shows the master private key.
    fn show_master_privkey() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("show-master-zprv", &[], Operation::Poke, &mut slab)
    }

    /// Shows the key tree structure.
    fn show_key_tree(include_values: bool) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let include_values_noun = if include_values { YES } else { NO };
        Self::wallet(
            "show-key-tree",
            &[include_values_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    fn parse_sign_key_components(
        index_str: &str,
        hardened_str: Option<&str>,
    ) -> Result<(u64, bool), NockAppError> {
        let index = index_str.trim().parse::<u64>().map_err(|err| {
            CrownError::Unknown(format!("Invalid key index '{}': {}", index_str.trim(), err))
        })?;
        if index >= 2 << 31 {
            return Err(CrownError::Unknown("Key index must not exceed 2^31 - 1".into()).into());
        }
        let hardened = if let Some(flag) = hardened_str {
            Self::parse_boolish(flag)?
        } else {
            false
        };
        Ok((index, hardened))
    }

    /// Parses permissive bool-like hardened flags used by CLI sign-key input.
    fn parse_boolish(flag: &str) -> Result<bool, NockAppError> {
        match flag {
            "true" | "t" | "1" | "yes" | "y" => Ok(true),
            "false" | "f" | "0" | "no" | "n" => Ok(false),
            _ => Err(CrownError::Unknown(format!(
                "Invalid hardened value '{}', expected true/false",
                flag
            ))
            .into()),
        }
    }

    /// Parses comma-separated `index:hardened` sign-key tuples from CLI input.
    fn parse_sign_keys(sign_keys_str: &str) -> Result<Vec<(u64, bool)>, NockAppError> {
        let mut sign_keys = Vec::new();
        for piece in sign_keys_str.split(',') {
            let trimmed = piece.trim();
            if trimmed.is_empty() {
                continue;
            }
            let parts: Vec<&str> = trimmed.split(':').collect();
            if parts.len() != 2 {
                return Err(CrownError::Unknown(format!(
                    "Invalid sign key '{}', expected index:hardened",
                    trimmed
                ))
                .into());
            }
            sign_keys.push(Self::parse_sign_key_components(parts[0], Some(parts[1]))?);
        }
        if sign_keys.is_empty() {
            return Err(
                CrownError::Unknown("At least one sign key must be provided".to_string()).into(),
            );
        }
        Ok(sign_keys)
    }

    /// Parses comma-separated base58 pubkey hashes for multisig watch import.
    fn parse_pubkey_hashes(pubkeys_str: &str) -> Result<Vec<Hash>, NockAppError> {
        let pubkeys: Vec<Hash> = pubkeys_str
            .split(',')
            .map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    return Err(NockAppError::from(CrownError::Unknown(
                        "Empty pubkey hash provided in list".into(),
                    )));
                }
                Hash::from_base58(trimmed).map_err(|err| {
                    NockAppError::from(CrownError::Unknown(format!(
                        "Invalid pubkey hash '{}': {}",
                        trimmed, err
                    )))
                })
            })
            .collect::<Result<Vec<Hash>, NockAppError>>()?;

        if pubkeys.is_empty() {
            return Err(
                CrownError::Unknown("At least one pubkey hash must be provided".into()).into(),
            );
        }

        Ok(pubkeys)
    }

    /// Signs a multisig transaction with provided key index/hardened tuples.
    fn sign_multisig_tx(
        transaction_path: &str,
        sign_keys_str: Option<&str>,
    ) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        let transaction_data = fs::read(transaction_path)
            .map_err(|e| CrownError::Unknown(format!("Failed to read transaction file: {}", e)))?;

        let transaction_noun = slab.cue_into(transaction_data.as_bytes()?).map_err(|e| {
            CrownError::Unknown(format!("Failed to decode transaction data: {}", e))
        })?;

        let sign_keys_noun = if let Some(sign_keys_str) = sign_keys_str {
            let sign_keys = Self::parse_sign_keys(sign_keys_str)?;
            sign_keys
                .into_iter()
                .rev()
                .fold(D(0), |acc, (index, hardened)| {
                    let index_noun = D(index);
                    let hardened_noun = if hardened { YES } else { NO };
                    let pair = T(&mut slab, &[index_noun, hardened_noun]);
                    Cell::new(&mut slab, pair, acc).as_noun()
                })
        } else {
            SIG
        };

        Self::wallet(
            "sign-multisig-tx",
            &[transaction_noun, sign_keys_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    #[allow(dead_code)]
    /// Displays a multisig transaction payload without signing.
    fn show_multisig_tx(transaction_path: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        let transaction_data = fs::read(transaction_path)
            .map_err(|e| CrownError::Unknown(format!("Failed to read transaction file: {}", e)))?;

        let transaction_noun = slab.cue_into(transaction_data.as_bytes()?).map_err(|e| {
            CrownError::Unknown(format!("Failed to decode transaction data: {}", e))
        })?;

        Self::wallet(
            "show-multisig-tx",
            &[transaction_noun],
            Operation::Poke,
            &mut slab,
        )
    }
}

/// Returns wallet data directory path, creating it if missing.
pub async fn wallet_data_dir() -> Result<PathBuf, NockAppError> {
    let wallet_data_dir = system_data_dir().join("wallet");
    if !wallet_data_dir.exists() {
        tokio_fs::create_dir_all(&wallet_data_dir)
            .await
            .map_err(|e| {
                CrownError::Unknown(format!("Failed to create wallet data directory: {}", e))
            })?;
    }
    Ok(wallet_data_dir)
}

#[allow(dead_code)]
/// Confirms dangerous upper-bound timelock usage with explicit user acknowledgement.
fn confirm_upper_bound_warning() -> Result<(), NockAppError> {
    println!(
        "Warning: specifying an upper timelock bound will make the output unspendable after that height. Only use this feature if you know what you're doing."
    );
    print!("Type 'YES' to continue: ");
    io::stdout()
        .flush()
        .map_err(|e| CrownError::Unknown(format!("Failed to flush stdout: {}", e)))?;
    let mut response = String::new();
    io::stdin()
        .read_line(&mut response)
        .map_err(|e| CrownError::Unknown(format!("Failed to read confirmation: {}", e)))?;

    if response.trim() == "YES" {
        Ok(())
    } else {
        Err(CrownError::Unknown(
            "Aborted create-tx because upper bound was not confirmed with YES".into(),
        )
        .into())
    }
}

/// Normalizes watch input as either schnorr pubkey or hash base58 value.
pub(crate) fn normalize_watch_address(value: String) -> Result<Option<String>, NockAppError> {
    if value.len() >= SchnorrPubkey::BYTES_BASE58 {
        match SchnorrPubkey::from_base58(&value) {
            Ok(pubkey) => pubkey
                .to_base58()
                .map(Some)
                .map_err(|err| NockAppError::OtherError(err.to_string())),
            Err(err) => {
                warn!(
                    "Skipping invalid watch-only schnorr pubkey '{}': {}",
                    value, err
                );
                Ok(None)
            }
        }
    } else {
        match Hash::from_base58(&value) {
            Ok(hash) => Ok(Some(hash.to_base58())),
            Err(err) => {
                warn!("Skipping invalid watch-only hash '{}': {}", value, err);
                Ok(None)
            }
        }
    }
}

#[allow(dead_code)]
/// Normalizes a first-name hash and filters invalid values.
fn normalize_first_name(value: String) -> Result<Option<String>, NockAppError> {
    match Hash::from_base58(&value) {
        Ok(hash) => Ok(Some(hash.to_base58())),
        Err(err) => {
            warn!("Skipping invalid first name '{}': {}", value, err);
            Ok(None)
        }
    }
}

/// Queries the public node for acceptance status of one transaction id.
async fn run_transaction_accepted(
    connection: &connection::ConnectionCli,
    tx_id: &str,
) -> Result<(), NockAppError> {
    if connection.client != ClientType::Public {
        return Err(NockAppError::OtherError(
            "transaction-accepted command requires the public client (--client public)".to_string(),
        ));
    }

    let endpoint = connection.public_grpc_server_addr.to_string();
    let mut client = public_nockchain::PublicNockchainGrpcClient::connect(endpoint.clone())
        .await
        .map_err(|err| {
            NockAppError::OtherError(format!(
                "Failed to connect to public Nockchain gRPC server at {}: {}",
                endpoint, err
            ))
        })?;

    Hash::from_base58(tx_id).map_err(|_| {
        NockAppError::OtherError(format!(
            "Invalid transaction ID (expected base58-encoded hash): {}",
            tx_id
        ))
    })?;

    let request = PbBase58Hash {
        hash: tx_id.to_string(),
    };

    let response = client.transaction_accepted(request).await.map_err(|err| {
        NockAppError::OtherError(format!(
            "Transaction accepted query failed for {}: {}",
            tx_id, err
        ))
    })?;

    let accepted = match response.result {
        Some(transaction_accepted_response::Result::Accepted(value)) => value,
        Some(transaction_accepted_response::Result::Error(err)) => {
            return Err(NockAppError::OtherError(format!(
                "Transaction accepted query returned error code {}: {}",
                err.code, err.message
            )))
        }
        None => {
            return Err(NockAppError::OtherError(
                "Transaction accepted query returned an empty result".to_string(),
            ))
        }
    };

    let _event = crate::wallet_outcome::tx_accepted_event(tx_id, accepted);
    let markdown = crate::wallet_outcome::tx_accepted_markdown(tx_id, accepted);
    let skin = MadSkin::default_dark();
    println!("{}", skin.term_text(&markdown));

    Ok(())
}

/// Renders a compact markdown summary for transaction acceptance status.
#[allow(dead_code)]
fn format_transaction_accepted_markdown(tx_id: &str, accepted: bool) -> String {
    crate::wallet_outcome::tx_accepted_markdown(tx_id, accepted)
}

/// Builds an atom from raw bytes using indirect atom allocation.
pub fn from_bytes(stack: &mut NounSlab, bytes: &[u8]) -> Atom {
    unsafe {
        let mut tas_atom = IndirectAtom::new_raw_bytes(stack, bytes.len(), bytes.as_ptr());
        tas_atom.normalize_as_atom_stack()
    }
}
