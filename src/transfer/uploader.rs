use crate::cmd::upload::{UploadOption, FinalityRequirement};
use crate::common::blockchain::contract::RetryOption;
use crate::common::options::LogOption;
use crate::contract::flow::Submission;
use crate::contract::contract::FlowContract;
use crate::contract::market::Market;
use crate::core::dataflow::{merkle_tree, IterableData};
use crate::core::flow::Flow;
use crate::node::client_zgs::ZgsClient;
use anyhow::{Context, Result};
use ethers::providers::Middleware;
use ethers::types::{TransactionReceipt, H256, U256};
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    signers::LocalWallet
};
use std::sync::Arc;

const SMALL_FILE_SIZE_THRESHOLD: i64 = 256 * 1024;
const DEFAULT_TASK_SIZE: u32 = 10;

pub struct Uploader{
    pub clients: Vec<ZgsClient>,
    pub flow: FlowContract,
    pub market: Market<SignerMiddleware<Provider<Http>, LocalWallet>>,
}

impl Uploader {
    pub async fn new(
        web3_client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        clients: Vec<ZgsClient>,
        log_opt: &LogOption,
    ) -> Result<Self> {
        if clients.is_empty() {
            anyhow::bail!("Storage node not specified");
        }

        let status = clients[0].get_status().await.with_context(|| {
            format!("Failed to get status from storage node {}", clients[0].url)
        })?;

        let chain_id = web3_client
            .get_chainid()
            .await
            .with_context(|| "Failed to get chain ID from blockchain node")?;

        if chain_id != U256::from(status.networkIdentity.chainId) {
            anyhow::bail!(
                "Chain ID mismatch, blockchain = {}, storage node = {}",
                chain_id,
                status.networkIdentity.chainId
            )
        }

        let flow = FlowContract::new(web3_client.clone(), status.networkIdentity.flowAddress,);
        let market = flow.get_market_contract().await?;
        Ok(Self {
            clients,
            flow,
            market,
        })
    }

    pub async fn check_log_existance(&self, root: [u8; 32]) -> Result<bool> {
        for client in self.clients.iter() {
            match client.get_file_info(root).await {
                Ok(Some(_info)) => {
                    // existed
                    return Ok(true);
                }
                // not existed
                Ok(None) => continue,
                Err(err) => return Err(err.into()),
            }
        }
        Ok(false)
    }

    pub async fn upload(&self, data: Arc<dyn IterableData>, opt: &UploadOption) -> Result<H256> {
        let root = merkle_tree(data.clone()).await?.root();
        let mut root_bytes = [0u8; 32];
        root_bytes.copy_from_slice(root.as_bytes());
        let exist = self.check_log_existance(root_bytes).await?;
        let tx = if !opt.skip_tx & !exist {
            log::info!(
                "Data[{:?}] to be uploaded not exist and not skip, uploading...",
                root
            );
            let (tx_hash, receipt) = self.submit_log_entry(vec![data.clone()], vec![opt.tags.clone()], 
                                                           opt.finality_required <= FinalityRequirement::TransactionPacked,
                                                           opt.nonce, opt.fee).await?;

            if data.size() <= SMALL_FILE_SIZE_THRESHOLD {
                log::info!("Upload small data immediately");
            } else if opt.finality_required <= FinalityRequirement::TransactionPacked {
                self.wait_for_log_entry(root, FinalityRequirement::TransactionPacked, receipt).await?;
            }

            tx_hash
        } else {
            println!("Data have been uploaded");
            H256::zero()
        };

        Ok(tx)
    }

    async fn submit_log_entry(
        &self,
        datas: Vec<Arc<dyn IterableData>>,
        tags: Vec<Vec<u8>>,
        wait_for_receipt: bool,
        nonce: Option<U256>,
        fee: Option<U256>,
    ) -> Result<(H256, Option<TransactionReceipt>)> {
        let submissions: Result<Vec<Submission>> = datas
            .into_iter()
            .zip(tags.into_iter())
            .map(|(data, tag)| {
                let flow = Flow::new(data, tag);
                flow.create_submission()
                    .context("Failed to create submission")
            })
            .collect();

        let submissions = submissions?;
        let price_per_sector = self.market.price_per_sector().call().await?;
        log::info!("Price per sector: {:?}", price_per_sector);
        let mut opts: TransactionRequest = self.flow.create_transact_opts().await;
        if let Some(nonce_value) = nonce {
            opts = opts.nonce(nonce_value);
        }        
        let tx = if submissions.len() == 1 {
            let fee = fee.unwrap_or_else(|| submissions[0].fee(&price_per_sector));
            log::info!("Submit with fee: {}", fee);
            self.flow.submit(submissions[0].clone(), fee, opts).await?
        } else {
            let total_fee = fee.unwrap_or_else(|| {
                submissions
                    .iter()
                    .map(|s| s.fee(&price_per_sector))
                    .fold(U256::zero(), |acc, fee| acc + fee)
            });
            log::info!("Batch submit with fee: {}", total_fee);
            self.flow
                .batch_submit(submissions.clone(), total_fee, opts)
                .await?
        };

        log::info!("Transaction sent: {:?}", tx);

        if wait_for_receipt {
            let receipt = self.flow.wait_for_receipt(tx, true, &RetryOption::default()).await?;
            Ok((receipt.transaction_hash, Some(receipt)))
        } else {
            Ok((tx, None))
        }
    }

    async fn wait_for_log_entry(&self, root: H256, finality_required: FinalityRequirement, receipt: Option<TransactionReceipt>) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::Uploader;
    use crate::common::blockchain::rpc::must_new_web3;
    use crate::core::file::File;
    use crate::node::client_zgs::must_new_zgs_clients;
    use crate::{cmd::upload::UploadOption, common::options::LogOption};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    // Helper function to create a file with specific content
    fn create_temp_file_with_content(content: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap(); // 重置文件指针
        file
    }

    #[tokio::test]
    async fn test_new_uploader() {
        let node_urls = vec![String::from("http://127.0.0.1:5678")];
        let clients = must_new_zgs_clients(&node_urls);

        let chain_url = "https://evmrpc-testnet.0g.ai";
        let key = "0x9ac8c66a0712816db4364b7004c89cd077da0e07b3ec2c0314eeb3b03f8df21e";
        let web3_client = must_new_web3(chain_url, key).await;

        match Uploader::new(web3_client, clients, &LogOption {}).await {
            Ok(uploder) => {
                println!("Successfully created uploder");
            }
            Err(e) => {
                panic!("Failed to create uploader: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_upload() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

        let node_urls = vec![String::from("http://127.0.0.1:5678")];
        let clients = must_new_zgs_clients(&node_urls);

        let chain_url = "https://evmrpc-testnet.0g.ai";
        let key = "0x9ac8c66a0712816db4364b7004c89cd077da0e07b3ec2c0314eeb3b03f8df21e";
        let web3_client = must_new_web3(chain_url, key).await;

        let data = File::open(create_temp_file_with_content(b"Hello! World!!").path()).unwrap();
        // let data = File::open(Path::new("tmp123456")).unwrap();
        let uploader = Uploader::new(web3_client, clients, &LogOption {})
            .await
            .unwrap();
        let root = uploader
            .upload(Arc::new(data), &UploadOption::default())
            .await
            .unwrap();
        println!("uploaded data root: {:?}", root);
    }
}
