use anyhow::Error;
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    signers::LocalWallet
};
use std::ops::Deref;
use std::sync::Arc;

use super::flow::{Submission, ZgsFlow};
use super::market::Market;
use crate::common::blockchain::contract::Contract;

pub struct FlowContract {
    contract: Contract,
    flow: ZgsFlow<SignerMiddleware<Provider<Http>, LocalWallet>>,
}

impl Deref for FlowContract {
    type Target = Contract;

    fn deref(&self) -> &Self::Target {
        &self.contract
    }
}

impl FlowContract {
    pub fn new(
        client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        flow_address: Address,
    ) -> Self {
        let contract = Contract::new(client.clone());
        let flow = ZgsFlow::new(flow_address, client);
        Self { contract, flow }
    }

    pub async fn get_market_contract(
        &self,
    ) -> Result<Market<SignerMiddleware<Provider<Http>, LocalWallet>>, Error> {
        let market_addr: Address = self.flow.market().await?;
        log::info!("market address: {:?}", market_addr);
        let market = Market::new(market_addr, self.client.clone());
        Ok(market)
    }

    pub async fn submit(&self, submission: Submission, fee: U256, traction_opts: TransactionRequest) -> Result<TxHash, Error> {
        let mut tx_builder = self.flow.submit(submission).legacy();
        if let Some(from) = traction_opts.from {
            tx_builder = tx_builder.from(from);
        }
        if let Some(gas_price) = traction_opts.gas_price {
            tx_builder = tx_builder.gas_price(gas_price);
        }
        if let Some(gas_limit) = traction_opts.gas {
            tx_builder = tx_builder.gas(gas_limit);
        }
        tx_builder = tx_builder.value(fee);
        log::debug!("tx builder: {:?}", tx_builder);
        let pending_tx = tx_builder.send().await?;
        Ok(pending_tx.tx_hash())
    }

    pub async fn batch_submit(
        &self,
        submissions: Vec<Submission>,
        total_fee: U256,
        traction_opts: TransactionRequest
    ) -> Result<TxHash, Error> {
        let mut tx_builder = self.flow.batch_submit(submissions).legacy();;
        if let Some(from) = traction_opts.from {
            tx_builder = tx_builder.from(from);
        }
        if let Some(gas_price) = traction_opts.gas_price {
            tx_builder = tx_builder.gas_price(gas_price);
        }
        if let Some(gas_limit) = traction_opts.gas {
            tx_builder = tx_builder.gas(gas_limit);
        }
        tx_builder = tx_builder.value(total_fee);
        log::debug!("tx builder: {:?}", tx_builder);
        let pending_tx = tx_builder.send().await?;

        Ok(pending_tx.tx_hash())
    }
}
