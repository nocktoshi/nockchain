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