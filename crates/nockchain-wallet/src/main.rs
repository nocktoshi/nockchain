#![allow(clippy::doc_overindented_list_items)]
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

use clap::Parser;
use nockapp::kernel::boot::{self, NockStackSize};
use nockapp::NockAppError;
use nockapp_grpc::pb::common::v1::Base58Hash as PbBase58Hash;
use nockapp_grpc::pb::public::v2::transaction_accepted_response;
use nockapp_grpc::public_nockchain;
use nockchain_types::common::Hash;
use nockchain_wallet::command::{ClientType, Commands, WalletCli};
use nockchain_wallet::dispatch::{execute_wallet_command, DispatchHooks};
use nockchain_wallet::{boot_wallet, tx_accepted_markdown};
use termimad::MadSkin;

#[tokio::main]
async fn main() -> Result<(), NockAppError> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("default provider already set elsewhere");

    let mut cli = WalletCli::parse();
    cli.boot.stack_size = NockStackSize::Tiny;

    boot::init_default_tracing(&cli.boot.clone());

    if let Commands::TxAccepted { tx_id } = &cli.command {
        return run_transaction_accepted(&cli.connection, tx_id).await;
    }

    if let Commands::TxStatus {
        tx_id,
        wait,
        timeout_secs,
    } = &cli.command
    {
        return run_tx_status(&cli.connection, tx_id, *wait, *timeout_secs).await;
    }

    let (mut wallet, mut synced_snapshot_for_planner, _data_dir) = boot_wallet(
        cli.boot.clone(),
        cli.fakenet,
        cli.fakenet_v1_phase,
        cli.fakenet_bythos_phase,
    )
    .await?;

    execute_wallet_command(
        &cli.connection,
        &mut wallet,
        &cli.command,
        &mut synced_snapshot_for_planner,
        false,
        DispatchHooks::cli(),
    )
    .await
    .map(|_| ())
}

async fn run_transaction_accepted(
    connection: &nockchain_wallet::ConnectionCli,
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

    let markdown = tx_accepted_markdown(tx_id, accepted);
    let skin = MadSkin::default_dark();
    println!("{}", skin.term_text(&markdown));

    Ok(())
}

/// Reports a transaction's true lifecycle status by asking the node's block
/// explorer where it lives: confirmed in a block (with height + confirmation
/// depth against the current tip), pending in the mempool, or unknown.
async fn run_tx_status(
    connection: &nockchain_wallet::ConnectionCli,
    tx_id: &str,
    wait: bool,
    timeout_secs: u64,
) -> Result<(), NockAppError> {
    use nockapp_grpc::public_nockchain;
    use nockchain_types::common::Hash;
    use nockchain_wallet::command::ClientType;
    use termimad::MadSkin;
    use tracing::info;

    if connection.client != ClientType::Public {
        return Err(NockAppError::OtherError(
            "tx-status command requires the public client (--client public)".to_string(),
        ));
    }

    Hash::from_base58(tx_id).map_err(|_| {
        NockAppError::OtherError(format!(
            "Invalid transaction ID (expected base58-encoded hash): {}",
            tx_id
        ))
    })?;

    let endpoint = connection.public_grpc_server_addr.to_string();
    let mut client = public_nockchain::PublicNockchainGrpcClient::connect(endpoint.clone())
        .await
        .map_err(|err| {
            NockAppError::OtherError(format!(
                "Failed to connect to public Nockchain gRPC server at {}: {}",
                endpoint, err
            ))
        })?;

    const POLL_INTERVAL_SECS: u64 = 5;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let skin = MadSkin::default_dark();

    loop {
        let (markdown, confirmed) = fetch_tx_status_markdown(&mut client, tx_id).await?;

        if confirmed || !wait {
            println!("{}", skin.term_text(&markdown));
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            println!("{}", skin.term_text(&markdown));
            return Err(NockAppError::OtherError(format!(
                "tx-status: {} did not confirm within {}s",
                tx_id, timeout_secs
            )));
        }
        info!(
            "tx-status: {} not yet confirmed, polling again in {}s...",
            tx_id, POLL_INTERVAL_SECS
        );
        tokio::time::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS)).await;
    }
}

async fn fetch_tx_status_markdown(
    client: &mut nockapp_grpc::public_nockchain::PublicNockchainGrpcClient,
    tx_id: &str,
) -> Result<(String, bool), NockAppError> {
    use nockapp_grpc::pb::common::v1::Base58Hash as PbBase58Hash;
    use nockapp_grpc::pb::public::v2::transaction_accepted_response;

    let tx_hash = PbBase58Hash {
        hash: tx_id.to_string(),
    };

    let block = client
        .get_transaction_block(tx_hash.clone())
        .await
        .map_err(|err| {
            NockAppError::OtherError(format!(
                "tx-status: get_transaction_block failed for {}: {}",
                tx_id, err
            ))
        })?;

    if let Some((height, block_id)) = block {
        let tip = client.explorer_heaviest_height().await.unwrap_or(height);
        let confirmations = tip.saturating_sub(height).saturating_add(1);
        let markdown = [
            "## Transaction Status".to_string(),
            format!("- tx id: `{}`", tx_id),
            "- status: **confirmed** (mined into a block)".to_string(),
            format!("- block height: {}", height),
            format!("- block id: `{}`", block_id.to_base58()),
            format!("- confirmations: {} (tip at height {})", confirmations, tip),
        ]
        .join("
");
        return Ok((markdown, true));
    }

    let in_mempool = match client.transaction_accepted(tx_hash).await {
        Ok(resp) => matches!(
            resp.result,
            Some(transaction_accepted_response::Result::Accepted(true))
        ),
        Err(_) => false,
    };
    let markdown = if in_mempool {
        [
            "## Transaction Status".to_string(),
            format!("- tx id: `{}`", tx_id),
            "- status: **pending** (in the node mempool, not yet mined)".to_string(),
            "- next: a miner must include it. If it has been pending a while, re-run `send-tx <file>` to re-broadcast (txs age out of network mempools).".to_string(),
        ]
        .join("
")
    } else {
        [
            "## Transaction Status".to_string(),
            format!("- tx id: `{}`", tx_id),
            "- status: **unknown to node** (not in a block and not in the mempool)".to_string(),
            "- next: submit it with `send-tx <file>`.".to_string(),
        ]
        .join("
")
    };
    Ok((markdown, false))
}

