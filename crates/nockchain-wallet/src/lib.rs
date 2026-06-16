//! Nockchain wallet library: command dispatch, planner, and kernel runtime.

pub mod command;
pub mod error;
pub mod recipient;
pub mod wallet;
pub mod connection;
pub mod create_tx;
pub mod dispatch;
pub mod wallet_outcome;

#[cfg(test)]
mod tests;

pub use command::{CommandNoun, Commands, WalletCli};
pub use connection::ConnectionCli;
pub use dispatch::{DispatchHooks};
pub use create_tx::{PlannedCreateTx, WrittenTxSnapshot};
pub use recipient::RecipientSpec;
pub use recipient::{validate_blob_field, validate_memo_utf8};
pub use wallet::{from_bytes, normalize_watch_address, wallet_data_dir, Wallet};
pub use wallet_outcome::{
    migrate_summary_event, tx_accepted_event, tx_accepted_markdown, WalletCommandData,
    WalletCommandJsonResponse, WalletCommandOutcome, WalletEvent, WALLET_OUTCOME_SCHEMA,
};
pub use wallet_tx_builder::adapter::NormalizedSnapshot;

use std::path::PathBuf;

use kernels_open_wallet::KERNEL;
use nockapp::kernel::boot;
use nockapp::{CrownError, NockAppError};
use zkvm_jetpack::hot::produce_prover_hot_state;



/// Boot wallet kernel using only the minimal options needed (no WalletCli / Commands required).
/// Intended for the TUI and other non-CLI consumers.
pub async fn boot_wallet(
    boot: nockapp::kernel::boot::Cli,
    fakenet: bool,
    fakenet_v1_phase: Option<u64>,
    fakenet_bythos_phase: Option<u64>,
) -> Result<(Wallet, Option<NormalizedSnapshot>, PathBuf), NockAppError> {
    let prover_hot_state = produce_prover_hot_state();
    let data_dir = wallet_data_dir().await?;

    let kernel = boot::setup(
        KERNEL,
        boot.clone(),
        prover_hot_state.as_slice(),
        "wallet",
        Some(data_dir.clone()),
    )
    .await
    .map_err(|e| CrownError::Unknown(format!("Kernel setup failed: {}", e)))?;

    let mut wallet = Wallet::new(kernel);
    let synced_snapshot_for_planner: Option<NormalizedSnapshot> = None;

    if fakenet {
        wallet
            .set_fakenet_with_overrides(fakenet_v1_phase, fakenet_bythos_phase)
            .await?;
    } else if wallet.is_fakenet().await? {
        return Err(NockAppError::OtherError(
            "Attempted to boot the wallet in mainnet mode, but the loaded state is in fakenet mode. Please use the --fakenet flag to boot the wallet or boot the wallet with the --new flag to create a new mainnet wallet".to_string(),
        ));
    }

    Ok((wallet, synced_snapshot_for_planner, data_dir))
}
