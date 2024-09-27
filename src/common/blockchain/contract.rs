use anyhow::{anyhow, Error, Result};
use ethers::prelude::*;
use ethers::types::U256;
use serde_json::Value;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

const CUSTOM_GAS_PRICE: Option<U256> = None;
const CUSTOM_GAS_LIMIT: Option<U256> = None;

pub async fn deploy<M: Middleware + 'static>(
    client: Arc<M>,
    data_or_file: &str,
) -> Result<Address> {
    let bytecode = parse_bytecode(data_or_file)?;

    let mut tx = TransactionRequest::new().data(bytecode);

    if let Some(gas_price) = unsafe { CUSTOM_GAS_PRICE } {
        tx = tx.gas_price(gas_price);
    }

    if let Some(gas_limit) = unsafe { CUSTOM_GAS_LIMIT } {
        tx = tx.gas(gas_limit);
    }

    let pending_tx = client.send_transaction(tx, None).await?;
    let receipt = pending_tx
        .await?
        .ok_or_else(|| anyhow!("Transaction failed"))?;

    receipt
        .contract_address
        .ok_or_else(|| anyhow!("No contract address in receipt"))
}

fn parse_bytecode(data_or_file: &str) -> Result<Bytes> {
    if data_or_file.starts_with("0x") {
        return Ok(data_or_file.parse()?);
    }

    let content = fs::read_to_string(data_or_file)?;
    let data: Value = serde_json::from_str(&content)?;

    let bytecode = data
        .get("bytecode")
        .and_then(|b| b.as_str())
        .or_else(|| {
            data.get("bytecode")
                .and_then(|b| b.get("object"))
                .and_then(|o| o.as_str())
        })
        .ok_or_else(|| anyhow!("Bytecode not found in JSON"))?;

    Ok(bytecode.parse()?)
}

pub struct Contract {
    pub client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    pub account: Address,
}

// pub struct TransactOpts {
//     pub chain_id: U256,
//     pub gas_price: U256,
//     pub gas_limit: U256
// }

#[derive(Clone)]
pub struct RetryOption {
    pub interval: Duration,
    // Note: In Rust, we typically don't include loggers in structs.
    // Instead, we use the `log` crate's macros directly.
}

impl Default for RetryOption {
    fn default() -> Self {
        RetryOption {
            interval: Duration::from_secs(3),
        }
    }
}

impl Contract {
    pub fn new(client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>) -> Self {
        let account = client.address();
        Self { client, account }
    }

    pub async fn create_transact_opts(&self) -> TransactionRequest {
        let mut tx = TransactionRequest::new()
            .from(self.account)
            .chain_id(self.client.get_chainid().await.unwrap_or_default().as_u64());
        // let chain_id = self.client.get_chainid().await.unwrap_or_default();
        if let Some(gas_price) = CUSTOM_GAS_PRICE {
            tx = tx.gas_price(gas_price);
        }

        if let Some(gas_limit) = CUSTOM_GAS_LIMIT {
            tx = tx.gas(gas_limit);
        }

        tx
    }

    pub async fn wait_for_receipt(
        &self,
        tx_hash: TxHash,
        success_required: bool,
        opts: &RetryOption,
    ) -> Result<TransactionReceipt, Error> {
        let mut interval = time::interval(opts.interval);

        loop {
            interval.tick().await;
            match self.client.get_transaction_receipt(tx_hash).await? {
                Some(receipt) => {
                    if success_required && receipt.status.unwrap_or_default().is_zero() {
                        return Err(anyhow!("Transaction execution failed"));
                    }
                    return Ok(receipt);
                }
                None => {
                    log::warn!("Transaction not executed yet: {:?}", tx_hash);
                }
            }
        }
    }

    pub async fn get_nonce(&self) -> U256 {
        let nonce = self.client.get_transaction_count(self.account, None).await.expect("Fail to get nonce");
        log::info!("current nonce: {:?}", nonce);
        nonce
    }
}
