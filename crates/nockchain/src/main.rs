use std::error::Error;

use clap::Parser;
use crown::kernel::boot;
use kernels::dumb::KERNEL;
use zkvm_jetpack::hot::produce_prover_hot_state;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    sword::check_endian();
    let cli = nockchain::NockchainCli::parse();
    boot::init_default_tracing(&cli.crown_cli);

    let prover_hot_state = produce_prover_hot_state();
    let nockchain =
        nockchain::init_with_kernel(Some(cli), KERNEL, prover_hot_state.as_slice()).await?;
    nockchain.run().await?;
    Ok(())
}
