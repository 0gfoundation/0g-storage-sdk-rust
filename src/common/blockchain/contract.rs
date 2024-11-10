use anyhow::{anyhow, Result};
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

    if let Some(gas_price) = CUSTOM_GAS_PRICE {
        tx = tx.gas_price(gas_price);
    }

    if let Some(gas_limit) = CUSTOM_GAS_LIMIT {
        tx = tx.gas(gas_limit);
    }

    let pending_tx = client.send_transaction(tx, None).await?;
    let receipt = pending_tx.await?.ok_or_else(|| anyhow!("Transaction failed"))?;

    receipt.contract_address.ok_or_else(|| anyhow!("No contract address in receipt"))
}

fn parse_bytecode(data_or_file: &str) -> Result<Bytes> {
    if data_or_file.starts_with("0x") {
        return data_or_file.parse().map_err(Into::into);
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

    bytecode.parse().map_err(Into::into)
}

pub struct Contract {
    pub client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
    pub account: Address,
}

#[derive(Clone, Debug)]
pub struct RetryOption {
    pub interval: Duration,
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
        let mut tx = TransactionRequest::new().from(self.account);

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
    ) -> Result<TransactionReceipt> {
        let mut interval = time::interval(opts.interval);

        loop {
            interval.tick().await;
            if let Some(receipt) = self.client.get_transaction_receipt(tx_hash).await? {
                if success_required && receipt.status.unwrap_or_default().is_zero() {
                    return Err(anyhow!("Transaction execution failed"));
                }
                return Ok(receipt);
            }
            log::info!("Transaction not executed yet: {:?}", tx_hash);
        }
    }

    pub async fn get_nonce(&self) -> Result<U256> {
        let nonce = self.client.get_transaction_count(self.account, None).await?;
        log::info!("Current nonce: {:?}", nonce);
        Ok(nonce)
    }
}
