use anyhow::{anyhow, Result};
use ethers::{
    prelude::*,
    providers::{Http, Middleware, Provider},
    signers::{LocalWallet, Signer},
};
use log::{debug, error, info};
use std::sync::Arc;

pub static mut WEB3_LOG_ENABLED: bool = false;

pub async fn must_new_web3(
    url: &str,
    key: &str,
) -> Arc<SignerMiddleware<Provider<Http>, LocalWallet>> {
    match new_web3(url, key).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to connect to fullnode at {}: {}", url, e);
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

// pub async fn wait_for_receipt<M>(
//     client: Arc<M>,
//     tx_hash: H256,
//     success_required: bool,
//     opt: &RetryOption,
// ) -> Result<TransactionReceipt>
// where
//     M: Middleware + 'static,
//     M::Error: 'static,
// {
//     let mut interval = time::interval(opt.interval);

//     loop {
//         interval.tick().await;
//         match client.get_transaction_receipt(tx_hash).await? {
//             Some(receipt) => {
//                 if success_required && receipt.status.unwrap_or_default().is_zero() {
//                     return Err(anyhow!("Transaction execution failed"));
//                 }
//                 return Ok(receipt);
//             }
//             None => {
//                 error!("Transaction not executed yet: {:?}", tx_hash);
//             }
//         }
//     }
// }

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
        let url = "http://localhost:8545";
        let key = "0x758e4e906b7639e701c69fdcbf26bbc5001cf7e3a7ca56205d53ff88db3d3085";

        match new_web3(url, key).await {
            Ok(client) => {
                println!("Successfully created Web3 client");

                // 尝试获取当前区块号
                match client.get_block_number().await {
                    Ok(block_number) => {
                        println!("Current block number: {}", block_number);
                        assert!(true, "Successfully retrieved block number");
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
        let key = "0x9ac8c66a0712816db4364b7004c89cd077da0e07b3ec2c0314eeb3b03f8df21e";

        let client = must_new_web3(url, key).await;

        let address = client.address();
        println!("Account address: {:?}", address);

        let balance = client
            .get_balance(address, None)
            .await
            .expect("Failed to get balance");
        println!("Account balance: {}", balance);

        let chain_id = client
            .get_chainid()
            .await
            .expect("Failed to get chain id");
        println!("Chain id: {}", chain_id);
    }

    // #[tokio::test]
    // async fn test_wait_for_receipt() {
    //     let url = "http://localhost:8545";
    //     let key = "0xb3aa221c3203fcd1ed4821c3307edac067b3563b299bd0d3fb850d8d7932b7b4";
    //     let client = must_new_web3(url, key).await;

    //     // 获取当前区块号
    //     let block_number = client
    //         .get_block_number()
    //         .await
    //         .expect("Failed to get block number");
    //     println!("Current block number: {}", block_number);

    //     // 获取账户地址
    //     let address = client.address();
    //     println!("Account address: {:?}", address);

    //     // 获取账户余额
    //     let balance = client
    //         .get_balance(address, None)
    //         .await
    //         .expect("Failed to get balance");
    //     println!("Account balance: {} wei", balance);

    //     // 创建一个交易请求
    //     let tx = TransactionRequest::new()
    //         .to("0x28d2d9EDb91D7773B36ff07a5Dd4623F310CB102") // 接收地址，这里用了一个示例地址
    //         .value(U256::from(1000000000000000000u64)) // 1 ETH
    //         .from(address)
    //         .chain_id(1337u64);

    //     // 发送交易
    //     let pending_tx = client
    //         .send_transaction(tx, None)
    //         .await
    //         .expect("Failed to send transaction");

    //     let receipt = wait_for_receipt(client.clone(), pending_tx.tx_hash(), true, &RetryOption::default()).await;
    //     assert!(receipt.is_ok());
    // }
}
