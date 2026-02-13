use anyhow::Result;
use ethers::{
    prelude::*,
    providers::{Http, Middleware, Provider},
    signers::{LocalWallet, Signer},
};
use std::sync::Arc;

pub static mut WEB3_LOG_ENABLED: bool = false;

pub async fn must_new_web3(
    url: &str,
    key: &str,
) -> Arc<SignerMiddleware<Provider<Http>, LocalWallet>> {
    match new_web3(url, key).await {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to connect to fullnode at {}: {}", url, e);
            panic!("Failed to connect to fullnode");
        }
    }
}

pub async fn new_web3(
    url: &str,
    key: &str,
) -> Result<Arc<SignerMiddleware<Provider<Http>, LocalWallet>>> {
    let client = Provider::<Http>::try_from(url)?;
    let chain_id = client.get_chainid().await.expect("Failed to get chain id");
    let wallet: LocalWallet = key.parse()?;
    let wallet = wallet.with_chain_id(chain_id.as_u64());
    let client = client.with_signer(wallet);
    Ok(Arc::new(client))
}

pub fn default_signer(
    client: &SignerMiddleware<Provider<Http>, LocalWallet>,
) -> Result<LocalWallet> {
    Ok(client.signer().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_new_web3() {
        let url = "https://evmrpc-testnet.0g.ai";
        let key = "0x758e4e906b7639e701c69fdcbf26bbc5001cf7e3a7ca56205d53ff88db3d3085";

        match new_web3(url, key).await {
            Ok(client) => {
                log::info!("Successfully created Web3 client");

                // get current block number
                match client.get_block_number().await {
                    Ok(block_number) => {
                        log::info!("Current block number: {}", block_number);
                    }
                    Err(e) => {
                        panic!("Failed to get block number: {:?}", e);
                    }
                }
            }
            Err(e) => {
                panic!("Failed to create Web3 client: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_must_new_web3() {
        let url = "https://evmrpc-testnet.0g.ai";
        let key = "0x758e4e906b7639e701c69fdcbf26bbc5001cf7e3a7ca56205d53ff88db3d3085";

        let client = must_new_web3(url, key).await;

        let address = client.address();
        log::info!("Account address: {:?}", address);

        let balance = client
            .get_balance(address, None)
            .await
            .expect("Failed to get balance");
        log::info!("Account balance: {}", balance);

        let chain_id = client.get_chainid().await.expect("Failed to get chain id");
        log::info!("Chain id: {}", chain_id);
    }
}
