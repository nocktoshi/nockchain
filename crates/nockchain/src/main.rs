use std::error::Error;

use clap::Parser;
use kernels::dumb::KERNEL;
use nockapp::kernel::boot;
use nockapp::NockApp;
use zkvm_jetpack::hot::produce_prover_hot_state;

// When enabled, use jemalloc for more stable memory allocation
#[cfg(all(feature = "jemalloc", not(feature = "tracing-heap")))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "tracing-heap")]
#[global_allocator]
static ALLOC: tracy_client::ProfiledAllocator<tikv_jemallocator::Jemalloc> =
    tracy_client::ProfiledAllocator::new(tikv_jemallocator::Jemalloc, 100);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    nockvm::check_endian();
    let cli = nockchain::NockchainCli::parse();
    boot::init_default_tracing(&cli.nockapp_cli);

    let prover_hot_state = produce_prover_hot_state();
    let mut nockchain: NockApp =
        nockchain::init_with_kernel(Some(cli), KERNEL, prover_hot_state.as_slice()).await?;
    nockchain.run().await?;

    // Initialize the indexer
    let indexer = ChainIndexer::new(Path::new("chain_index"), "http://[::1]:5555").await?;

    // Start the indexing process
    indexer.start_indexing().await?;

    // Query balances
    let total = indexer.get_total_balance().await?;
    let locked = indexer.get_locked_balances().await?;
    let address_balance = indexer.get_balance(address.as_bytes())?;

    // Get the block these balances are from
    let last_block = indexer.get_last_indexed_block().await?;

    Ok(())
}
