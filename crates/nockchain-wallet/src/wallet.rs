//! Wallet runtime and helpers.

use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use nockapp::driver::{make_driver, PokeResult};
use nockapp::drivers::one_punch::OnePunchWire;
use nockapp::noun::slab::{NockJammer, NounSlab};
use nockapp::utils::bytes::Byts;
use nockapp::utils::make_tas;
use nockapp::wire::Wire;
use nockapp::{system_data_dir, CrownError, NockApp, NockAppError};
use nockchain_types::common::{Hash, SchnorrPubkey, TimelockRangeAbsolute, TimelockRangeRelative};
use nockchain_types::{default_fakenet_blockchain_constants, v0};
use nockvm::jets::cold::Nounable;
use nockvm::noun::{Atom, Cell, IndirectAtom, Noun, NounAllocator, D, NO, SIG, T, YES};
use nockapp::ToBytesExt;
use noun_serde::prelude::*;
use tokio::fs as tokio_fs;
use tracing::warn;

use crate::command::{CommandNoun, Operation};

/// Wallet runtime wrapper around the underlying nockapp kernel.
pub struct Wallet {
    pub(crate) app: NockApp,
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
    pub fn new(nockapp: NockApp) -> Self {
        Wallet {
            app: nockapp,
            tui_io_drivers_installed: false,
        }
    }

    /// Applies the shared Rust fakenet constants so wallet state matches node fakenet defaults.
    #[allow(dead_code)]
    pub(crate) async fn set_fakenet(&mut self) -> Result<(), NockAppError> {
        self.set_fakenet_with_overrides(None, None).await
    }

    /// Applies shared fakenet constants with optional phase overrides for custom local chains.
    pub(crate) async fn set_fakenet_with_overrides(
        &mut self,
        v1_phase: Option<u64>,
        bythos_phase: Option<u64>,
    ) -> Result<(), NockAppError> {
        let mut slab = NounSlab::new();
        let mut constants = default_fakenet_blockchain_constants();
        if let Some(v1_phase) = v1_phase {
            constants = constants.with_v1_phase(v1_phase);
        }
        if let Some(bythos_phase) = bythos_phase {
            constants = constants.with_bythos_phase(bythos_phase);
        }
        let constants_noun = constants.to_noun(&mut slab);
        let (poke, _) = Self::wallet("fakenet", &[constants_noun], Operation::Poke, &mut slab)?;
        let wire = OnePunchWire::Poke.to_wire();
        let _ = self.app.poke(wire, poke).await?;
        Ok(())
    }

    /// Reads whether current wallet state was initialized in fakenet mode.
    pub(crate) async fn is_fakenet(&mut self) -> Result<bool, NockAppError> {
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
    pub(crate) fn wallet(
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
    pub(crate) fn keygen(entropy: &[u8; 32], sal: &[u8; 16]) -> CommandNoun<NounSlab> {
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
    pub(crate) fn derive_child(index: u64, hardened: bool, label: &Option<String>) -> CommandNoun<NounSlab> {
        let mut slab: NounSlab<NockJammer> = NounSlab::new();
        let index_noun = D(index);
        let hardened_noun = if hardened { YES } else { NO };
        let label_noun = label.as_ref().map_or(SIG, |l| {
            let label_noun = l.to_noun(&mut slab);
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
    pub(crate) fn sign_tx(
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
    pub(crate) fn sign_message(
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
    pub(crate) fn verify_message(
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
    pub(crate) fn sign_hash(hash_b58: &str, index: Option<u64>, hardened: bool) -> CommandNoun<NounSlab> {
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
    pub(crate) fn verify_hash(
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
    pub(crate) fn import_seed_phrase(seed_phrase: &str, version: u64) -> CommandNoun<NounSlab> {
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
    pub(crate) fn import_keys(input_path: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn import_extended(extended_key: &str) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let key_noun = make_tas(&mut slab, extended_key).as_noun();
        Self::wallet("import-extended", &[key_noun], Operation::Poke, &mut slab)
    }

    /// Imports a watch-only public key.
    ///
    /// # Arguments
    ///
    /// * `watch_address` - Watch-only b58 encoded address. Can be v1 or v0.
    pub(crate) fn watch_address(watch_address: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn watch_first_name(first_name: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn watch_multisig(m: u64, pubkeys_str: &str) -> CommandNoun<NounSlab> {
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

    pub(crate) fn load_multisig_watch_manifest_pokes(
        threshold: u64,
        manifest_path: &str,
    ) -> Result<Vec<NounSlab>, NockAppError> {
        let manifest = fs::read_to_string(manifest_path).map_err(|err| {
            CrownError::Unknown(format!(
                "Failed to read multisig watch manifest '{}': {}",
                manifest_path, err
            ))
        })?;

        let mut pokes = Vec::new();
        for entry in manifest.lines().map(str::trim) {
            if entry.is_empty() || entry.starts_with('#') {
                continue;
            }

            let (noun, _) = Self::watch_multisig(threshold, entry)?;
            pokes.push(noun);
        }

        if pokes.is_empty() {
            return Err(CrownError::Unknown(format!(
                "Multisig watch manifest '{}' contained no entries",
                manifest_path
            ))
            .into());
        }

        Ok(pokes)
    }

    fn markdown_text_from_effect(effect: &NounSlab) -> Result<Option<String>, NockAppError> {
        let space = effect.noun_space();
        let Ok(effect_cell) = unsafe { effect.root() }.in_space(&space).as_cell() else {
            return Ok(None);
        };
        if effect_cell.head().eq_bytes(b"markdown") {
            let markdown_text = effect_cell.tail();
            let atom = markdown_text
                .as_atom()
                .map_err(|_| CrownError::Unknown("Malformed markdown effect".to_string()))?;
            return Ok(Some(
                String::from_utf8_lossy(&atom.to_bytes_until_nul()?).to_string(),
            ));
        }
        Ok(None)
    }

    fn is_exit_effect(effect: &NounSlab) -> bool {
        let space = effect.noun_space();
        let Ok(effect_cell) = unsafe { effect.root() }.in_space(&space).as_cell() else {
            return false;
        };
        effect_cell.head().eq_bytes(b"exit")
    }

    pub(crate) fn derived_address_from_effects(effects: &[NounSlab]) -> Result<String, NockAppError> {
        let mut derived_address: Option<String> = None;
        let mut markdown_blocks = Vec::new();

        for effect in effects {
            if let Some(markdown) = Self::markdown_text_from_effect(effect)? {
                for line in markdown.lines() {
                    let trimmed = line.trim();
                    if let Some(address) = trimmed.strip_prefix("- Address: ") {
                        let candidate = address.trim();
                        if !candidate.is_empty() && candidate != "N/A (private key)" {
                            derived_address = Some(candidate.to_string());
                        }
                    }
                }
                markdown_blocks.push(markdown);
            }
        }

        derived_address.ok_or_else(|| {
            CrownError::Unknown(format!(
                "derive-child batch could not extract a derived address from wallet output: {:?}",
                markdown_blocks
            ))
            .into()
        })
    }

    pub async fn derive_child_batch(
        &mut self,
        start_index: u64,
        count: u64,
        hardened: bool,
        label_prefix: &Option<String>,
    ) -> Result<Vec<(u64, String)>, NockAppError> {
        let end_exclusive = start_index.checked_add(count).ok_or_else(|| {
            CrownError::Unknown("derive-child-batch index range overflowed".to_string())
        })?;
        if end_exclusive > (1u64 << 31) {
            return Err(CrownError::Unknown(
                "derive-child-batch index must stay below 2^31".to_string(),
            )
            .into());
        }

        let mut derive_requests = Vec::with_capacity(count as usize);
        for offset in 0..count {
            let index = start_index + offset;
            let label = label_prefix
                .as_ref()
                .map(|prefix| format!("{prefix}-{index}"));
            let (noun, _) = Self::derive_child(index, hardened, &label)?;
            derive_requests.push((index, noun));
        }

        let (derived_sender, mut derived_receiver) =
            tokio::sync::mpsc::unbounded_channel::<Result<(u64, String), NockAppError>>();

        self.app
            .add_io_driver(make_driver(move |handle| async move {
                for (index, poke) in derive_requests {
                    match handle.poke(OnePunchWire::Poke.to_wire(), poke).await? {
                        PokeResult::Ack => {}
                        PokeResult::Nack => {
                            let _ = handle.exit.exit(1).await;
                            return Err(NockAppError::PokeFailed);
                        }
                    }

                    let mut effects = Vec::new();
                    loop {
                        let effect = handle.next_effect().await?;
                        let is_exit = Self::is_exit_effect(&effect);
                        effects.push(effect);
                        if is_exit {
                            break;
                        }
                    }

                    let address = Self::derived_address_from_effects(&effects)?;
                    if derived_sender.send(Ok((index, address))).is_err() {
                        return Err(CrownError::Unknown(
                            "derive-child-batch receiver dropped unexpectedly".to_string(),
                        )
                        .into());
                    }
                }

                handle.exit.exit(0).await?;
                Ok(())
            }))
            .await;

        self.app.run().await?;

        let mut derived = Vec::with_capacity(count as usize);
        while let Some(derive_result) = derived_receiver.recv().await {
            derived.push(derive_result?);
        }

        if derived.len() != count as usize {
            return Err(CrownError::Unknown(format!(
                "derive-child-batch expected {} derived addresses, got {}",
                count,
                derived.len()
            ))
            .into());
        }

        Ok(derived)
    }

    /// Exports keys to a file.
    pub(crate) fn export_keys() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("export-keys", &[], Operation::Poke, &mut slab)
    }

    #[allow(dead_code)]
    /// Builds a kernel timelock intent from optional absolute/relative ranges.
    pub(crate) fn timelock_intent_from_ranges(
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
    pub(crate) fn parse_note_names(raw: &str) -> Result<Vec<(String, String)>, NockAppError> {
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
    pub fn collect_signing_keys(
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
    pub(crate) fn parse_sign_key_entry(entry: &str) -> Result<(u64, bool), NockAppError> {
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
    pub(crate) fn list_notes() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("list-notes", &[], Operation::Poke, &mut slab)
    }

    /// Exports the master public key.
    ///
    /// # Returns
    ///
    /// Retrieves and displays master public key and chaincode.
    pub(crate) fn export_master_pubkey() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("export-master-pubkey", &[], Operation::Poke, &mut slab)
    }

    /// Imports a master public key.
    ///
    /// # Arguments
    ///
    /// * `key` - Base58-encoded public key
    /// * `chain_code` - Base58-encoded chain code
    pub(crate) fn import_master_pubkey(input_path: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn send_tx(transaction_path: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn show_tx(transaction_path: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn list_active_addresses() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("list-active-addresses", &[], Operation::Poke, &mut slab)
    }

    /// Sets the active master address.
    pub(crate) fn set_active_master_address(address_b58: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn list_master_addresses() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("list-master-addresses", &[], Operation::Poke, &mut slab)
    }

    /// Lists notes by public key
    pub(crate) fn list_notes_by_address(pubkey: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn list_notes_by_address_csv(pubkey: &str) -> CommandNoun<NounSlab> {
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
    pub(crate) fn show_balance() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();

        let balance_tag = make_tas(&mut slab, "balance").as_noun();
        let path_noun = Cell::new(&mut slab, balance_tag, D(0)).as_noun();

        Self::wallet("show", &[path_noun], Operation::Poke, &mut slab)
    }

    /// Shows the seed phrase for the current master key.
    pub(crate) fn show_seed_phrase() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("show-seed-phrase", &[], Operation::Poke, &mut slab)
    }

    /// Shows the master public key.
    pub(crate) fn show_master_pubkey() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("show-master-zpub", &[], Operation::Poke, &mut slab)
    }

    /// Shows the master private key.
    pub(crate) fn show_master_privkey() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("show-master-zprv", &[], Operation::Poke, &mut slab)
    }

    /// Shows the raw master private key as base58.
    pub(crate) fn show_master_prv() -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        Self::wallet("show-master-prv", &[], Operation::Poke, &mut slab)
    }

    /// Shows the key tree structure.
    pub(crate) fn show_key_tree(include_values: bool) -> CommandNoun<NounSlab> {
        let mut slab = NounSlab::new();
        let include_values_noun = if include_values { YES } else { NO };
        Self::wallet(
            "show-key-tree",
            &[include_values_noun],
            Operation::Poke,
            &mut slab,
        )
    }

    pub(crate) fn parse_sign_key_components(
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
    pub(crate) fn parse_boolish(flag: &str) -> Result<bool, NockAppError> {
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
    pub(crate) fn parse_sign_keys(sign_keys_str: &str) -> Result<Vec<(u64, bool)>, NockAppError> {
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
    pub(crate) fn parse_pubkey_hashes(pubkeys_str: &str) -> Result<Vec<Hash>, NockAppError> {
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
    pub(crate) fn sign_multisig_tx(
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
    pub(crate) fn show_multisig_tx(transaction_path: &str) -> CommandNoun<NounSlab> {
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
pub fn normalize_watch_address(value: String) -> Result<Option<String>, NockAppError> {
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

/// Builds an atom from raw bytes using indirect atom allocation.
pub fn from_bytes(stack: &mut NounSlab, bytes: &[u8]) -> Atom {
    unsafe {
        let mut tas_atom = IndirectAtom::new_raw_bytes(stack, bytes.len(), bytes.as_ptr());
        tas_atom.normalize_as_atom_stack()
    }
}
