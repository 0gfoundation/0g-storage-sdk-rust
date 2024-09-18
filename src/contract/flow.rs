use super::market::Market;
use ethers::prelude::*;
use std::sync::Arc;

abigen!(ZgsFlow, "contracts-abis/Flow.json");

// pub struct FlowContract<M> {
//     flow: ZgsFlow<M>,
//     client: Arc<M>,
// }

// impl<M: Middleware + 'static> FlowContract<M> {
//     pub fn new(flow_address: Address, client: Arc<M>) -> Result<Self, ContractError<M>> {
//         let flow = ZgsFlow::new(flow_address, client.clone());

//         Ok(Self { flow, client })
//     }

//     pub async fn get_market_contract(&self) -> Result<Market<M>, ContractError<M>> {
//         let market_addr: Address = self.flow.market().await?;
//         log::info!("market address: {:?}", market_addr);
//         let market = Market::new(market_addr, self.client.clone());
//         Ok(market)
//     }

//     pub async fn submit(
//         &self,
//         submission: Submission,
//         fee: U256,
//     ) -> Result<TxHash, ContractError<M>>
//     where
//         M: Middleware + 'static,
//     {
//         let tx_builder = self
//             .flow
//             .submit(submission)
//             .value(fee)
//             .legacy()
//             .gas_price(U256::from(1_000_000_000));
//         log::info!("tx builder: {:?}", tx_builder);
//         let pending_tx = tx_builder.send().await?;
//         Ok(pending_tx.tx_hash())
//     }

//     pub async fn batch_submit(
//         &self,
//         submissions: Vec<Submission>,
//         total_fee: U256,
//     ) -> Result<TxHash, ContractError<M>>
//     where
//         M: Middleware + 'static,
//     {
//         let tx_builder = self.flow.batch_submit(submissions).value(total_fee);

//         let pending_tx = tx_builder.send().await?;

//         Ok(pending_tx.tx_hash())
//     }

//     pub async fn wait_for_receipt(
//         &self,
//         tx_hash: TxHash,
//         success_required: bool,
//     ) -> Result<TransactionReceipt, anyhow::Error> {
//         let opt = Some(RetryOption::default());
//         wait(Arc::clone(&self.client), tx_hash, success_required, opt).await
//     }
// }

impl Submission {
    pub fn fee(&self, price_per_sector: &U256) -> U256 {
        let mut sectors: u64 = 0;

        // 遍历节点，并累加 sectors
        for node in &self.nodes {
            sectors += 1 << node.height.as_u64();
        }

        // sectors 乘以 price_per_sector
        U256::from(sectors) * price_per_sector
    }
}
