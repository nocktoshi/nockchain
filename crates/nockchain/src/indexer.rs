pub mod wallet;

use std::path::Path;
use std::time::Duration;

#[derive(Debug)]
pub struct BlockInfo {
    // Add necessary fields based on your requirements
    pub id: String,
    pub data: Vec<u8>,
}

impl BlockInfo {
    pub fn from_bytes(data: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        // Implement deserialization logic
        Ok(BlockInfo {
            id: String::from_utf8(data.to_vec())?,
            data: data.to_vec(),
        })
    }
}

use nockapp_grpc::client::NockAppGrpcClient;
use nockapp_grpc::pb::{
    peek_response, wallet_get_balance_response, PeekRequest, PeekResponse, WalletGetBalanceRequest, WalletGetBalanceResponse
};
use prost::Message;
use rocksdb::{Options, DB};
use tokio::time::sleep;
use tracing::{error, info};

pub struct ChainIndexer {
    db: DB,
    grpc_client: NockAppGrpcClient,
}

impl ChainIndexer {
    pub fn new(
        db_path: &Path,
        client: NockAppGrpcClient,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, db_path)?;

        Ok(Self {
            db,
            grpc_client: client,
        })
    }

    async fn peek_heaviest_block(
        &mut self,
    ) -> Result<Option<BlockInfo>, Box<dyn std::error::Error>> {
        let request = PeekRequest {
            pid: 0,
            path: vec!["heaviest-block".to_string()],
        };

        let response = self.grpc_client.peek(request.pid, request.path).await?;

        match response {
            Some(PeekResponse { result: Some(peek_response::Result::JammedData(data)) }) => {
                // Convert the jammed data to bytes
                match BlockInfo::from_bytes(&data) {
                    Ok(block_info) => Ok(Some(block_info)),
                    Err(e) => {
                        error!("Failed to deserialize block info: {}", e);
                        Ok(None)
                    }
                }
            }
            Some(peek_response::Result::Error(status)) => {
                error!("gRPC error: {:?}", status);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn peek_wallet_balances(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let request = WalletGetBalanceRequest {
            pid: 0,
            address: None,  // Get all wallet balances
            block_id: None, // Get latest block
        };

        let response = self
            .grpc_client
            .wallet_get_balance(&request, None, None)
            .await?;

        match response.result {
            Some(wallet_get_balance_response::Result::Balance(balance_data)) => {
                // Convert the balance data to WalletBalance
                let wallet_balance = wallet::WalletBalance::from_jammed_data(&balance_data)?;

                // Store the balance data in the DB
                for (note, balance) in wallet_balance.notes {
                    let key = format!("balance_{}", note);
                    self.db.put(key.as_bytes(), &balance.to_be_bytes())?;
                }

                // Store locked balances
                for (i, balance) in wallet_balance.locked_balances.iter().enumerate() {
                    let key = format!("locked_{}", i);
                    self.db.put(key.as_bytes(), &balance.to_be_bytes())?;
                }

                // Store block ID if available
                if let Some(block_id) = wallet_balance.block_id {
                    self.db.put(b"block_id", block_id.as_bytes())?;
                }

                // Store total balance if available
                if let Some(total) = wallet_balance.total_balance {
                    self.db.put(b"total_balance", &total.to_be_bytes())?;
                }

                Ok(())
            }
            Some(wallet_get_balance_response::Result::Error(status)) => {
                error!("Failed to get wallet balance: {:?}", status);
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting chain indexer...");

        loop {
            if let Err(e) = self.peek_wallet_balances().await {
                error!("Failed to index wallet balances: {}", e);
            }

            if let Err(e) = self.peek_heaviest_block().await {
                error!("Failed to index heaviest block: {}", e);
            }

            sleep(Duration::from_secs(5)).await;
        }
    }
}

pub async fn make_indexer_driver(
    db_path: &Path,
    client: NockAppGrpcClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut indexer = ChainIndexer::new(db_path, client)?;
    indexer.start().await
}
