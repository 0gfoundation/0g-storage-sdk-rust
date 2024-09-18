use anyhow::Error;
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    signers::LocalWallet,
    types::transaction::eip2718::TypedTransaction,
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
        let mut tx_builder = self.flow.submit(submission);
        tx_builder.tx = TypedTransaction::Legacy(traction_opts);
        tx_builder = tx_builder.value(fee);
        log::info!("tx builder: {:?}", tx_builder);
        let pending_tx = tx_builder.send().await?;
        Ok(pending_tx.tx_hash())
    }

    pub async fn batch_submit(
        &self,
        submissions: Vec<Submission>,
        total_fee: U256,
        traction_opts: TransactionRequest
    ) -> Result<TxHash, Error> {
        let mut tx_builder = self.flow.batch_submit(submissions);
        tx_builder.tx = TypedTransaction::Legacy(traction_opts);
        tx_builder = tx_builder.value(total_fee);
        log::info!("tx builder: {:?}", tx_builder);
        let pending_tx = tx_builder.send().await?;

        Ok(pending_tx.tx_hash())
    }
}
