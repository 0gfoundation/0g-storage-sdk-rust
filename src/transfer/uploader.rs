use crate::cmd::upload::{FinalityRequirement, UploadOption};
use crate::common::blockchain::contract::RetryOption;
use crate::common::options::LogOption;
use crate::common::shard::{check_replica, ShardConfig};
use crate::contract::contract::FlowContract;
use crate::contract::flow::Submission;
use crate::contract::market::Market;
use crate::core::dataflow::{
    merkle_tree, read_at, segment_root, IterableData, DEFAULT_CHUNK_SIZE,
    DEFAULT_SEGMENT_MAX_CHUNKS, DEFAULT_SEGMENT_SIZE,
};
use crate::core::flow::Flow;
use crate::core::merkle::tree::Tree;
use crate::node::{client_zgs::ZgsClient, types::SegmentWithProof};
use anyhow::{Context, Result};
use ethers::providers::Middleware;
use ethers::types::{TransactionReceipt, H256, U256};
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    signers::LocalWallet,
};
use futures::future::{join_all, try_join_all};
use std::sync::Arc;
use std::time::Instant;
use tokio::{time, time::Duration};

const SMALL_FILE_SIZE_THRESHOLD: i64 = 256 * 1024;
const DEFAULT_TASK_SIZE: u32 = 10;

pub struct Uploader {
    pub clients: Vec<ZgsClient>,
    pub flow: FlowContract,
    pub market: Market<SignerMiddleware<Provider<Http>, LocalWallet>>,
}

#[derive(Clone)]
pub struct UploadTask {
    pub client_index: usize,
    pub seg_index: u64,
    pub num_shard: u64,
}

#[derive(Clone)]
pub struct SegmentUploader {
    pub data: Arc<dyn IterableData>,
    pub tree: Arc<Tree>,
    pub clients: Vec<ZgsClient>,
    pub tasks: Vec<UploadTask>,
    pub task_size: u32,
}

impl Uploader {
    pub async fn new(
        web3_client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        clients: Vec<ZgsClient>,
        log_opt: &LogOption,
    ) -> Result<Self> {
        env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or(log_opt.level.as_str()),
        )
        .init();
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

        if chain_id != U256::from(status.network_identity.chain_id) {
            anyhow::bail!(
                "Chain ID mismatch, blockchain = {}, storage node = {}",
                chain_id,
                status.network_identity.chain_id
            )
        }

        let flow = FlowContract::new(web3_client.clone(), status.network_identity.flow_address);
        let market = flow.get_market_contract().await?;
        Ok(Self {
            clients,
            flow,
            market,
        })
    }

    pub async fn check_log_existence(&self, root: [u8; 32]) -> Result<bool> {
        for client in &self.clients {
            match client.get_file_info(root).await {
                Ok(Some(_)) => return Ok(true),
                Ok(None) => continue,
                Err(err) => return Err(err.into()),
            }
        }
        Ok(false)
    }

    pub async fn upload(&self, data: Arc<dyn IterableData>, opt: &UploadOption) -> Result<H256> {
        let start_time = Instant::now();

        let tree = Arc::new(merkle_tree(data.clone()).await?);
        let root = tree.root();
        let mut root_bytes = [0u8; 32];
        root_bytes.copy_from_slice(root.as_bytes());
        let exist = self.check_log_existence(root_bytes).await?;
        let tx_hash = if !opt.skip_tx & !exist {
            log::info!(
                "Data[{:?}] to be uploaded not exist and not skip, uploading...",
                root
            );
            let (tx_hash, receipt) = self
                .submit_log_entry(
                    vec![data.clone()],
                    vec![opt.tags.clone()],
                    opt.finality_required <= FinalityRequirement::TransactionPacked,
                    opt.nonce,
                    opt.fee,
                )
                .await?;

            if data.size() <= SMALL_FILE_SIZE_THRESHOLD {
                log::info!("Upload small data immediately");
            } else if opt.finality_required <= FinalityRequirement::TransactionPacked {
                let _ = self.wait_for_log_entry(root, FinalityRequirement::TransactionPacked, receipt)
                    .await;
            }

            tx_hash
        } else {
            log::info!("Data have been uploaded or skiped");
            H256::zero()
        };

        self.upload_file(data, tree, opt.expected_replica, opt.task_size)
            .await?;
        let _ = self.wait_for_log_entry(root, opt.finality_required, None)
            .await;

        log::info!("Upload completed in {:?}", start_time.elapsed());
        Ok(tx_hash)
    }

    async fn submit_log_entry(
        &self,
        datas: Vec<Arc<dyn IterableData>>,
        tags: Vec<Vec<u8>>,
        wait_for_receipt: bool,
        nonce: Option<U256>,
        fee: Option<U256>,
    ) -> Result<(H256, Option<TransactionReceipt>)> {
        let submission_futures: Vec<_> = datas
            .into_iter()
            .zip(tags.into_iter())
            .map(|(data, tag)| {
                let flow = Flow::new(data, tag);
                async move {
                    flow.create_submission()
                        .await
                        .context("Failed to create submission")
                }
            })
            .collect();

        let submissions: Vec<Submission> = try_join_all(submission_futures)
            .await
            .context("Failed to create submissions")?;

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
            let receipt = self
                .flow
                .wait_for_receipt(tx, true, &RetryOption::default())
                .await?;
            assert_eq!(tx, receipt.transaction_hash);
            Ok((receipt.transaction_hash, Some(receipt)))
        } else {
            Ok((tx, None))
        }
    }

    pub async fn wait_for_log_entry(
        &self,
        root: H256,
        finality_required: FinalityRequirement,
        receipt: Option<TransactionReceipt>,
    ) -> Result<()> {
        if matches!(finality_required, FinalityRequirement::WaitNothing) {
            return Ok(());
        }

        log::info!(
            "Wait for log entry on storage node: root={:?}, finality={:?}",
            root,
            finality_required
        );

        let root_bytes = root.to_fixed_bytes();

        loop {
            time::sleep(Duration::from_secs(1)).await;

            let check_results = join_all(self.clients.iter().map(|client| async {
                match client.get_file_info(root_bytes).await {
                    Ok(Some(info)) => {
                        if matches!(finality_required, FinalityRequirement::FileFinalized) 
                           && !info.finalized {
                            log::warn!(
                                "Log entry is available, but not finalized yet: cached={}, uploadedSegments={}",
                                info.is_cached,
                                info.uploaded_seg_num
                            );
                            false
                        } else {
                            true
                        }
                    },
                    Ok(None) => {
                        if let Some(receipt) = &receipt {
                            if let Ok(status) = client.get_status().await {
                                log::warn!(
                                    "Log entry is unavailable yet: txBlockNumber={:?}, zgsNodeSyncHeight={}",
                                    receipt.block_number,
                                    status.log_sync_height
                                );
                            }
                        }
                        false
                    },
                    Err(e) => {
                        log::warn!("Failed to get file info from client: {}", e);
                        false
                    }
                }
            })).await;

            if check_results.into_iter().all(|result| result) {
                return Ok(());
            }
        }
    }

    pub async fn new_segment_uploader(
        &self,
        data: Arc<dyn IterableData>,
        tree: Arc<Tree>,
        expected_replica: u32,
        task_size: u32,
    ) -> Result<SegmentUploader> {
        let num_segments = data.num_segments();
        let shard_configs = get_shard_configs(&self.clients).await?;
    
        if !check_replica(&shard_configs, expected_replica) {
            anyhow::bail!("Selected nodes cannot cover all shards");
        }
    
        let root_bytes = tree.root().to_fixed_bytes();
    
        let client_tasks = try_join_all(self.clients.iter().enumerate().map(|(client_index, client)| {
            // 克隆需要在闭包中使用的值
            let shard_config = shard_configs[client_index].clone();
            // let client = client.clone();
    
            async move {
                let info = client.get_file_info(root_bytes).await
                    .context("Failed to get file info")?;
    
                if let Some(info) = info {
                    if info.finalized {
                        return Ok::<Vec<UploadTask>, anyhow::Error>(Vec::new());
                    }
                }
    
                let tasks = (shard_config.shard_id..num_segments)
                    .step_by(shard_config.num_shard as usize * task_size as usize)
                    .map(|seg_index| UploadTask {
                        client_index,
                        seg_index,
                        num_shard: shard_config.num_shard,
                    })
                    .collect::<Vec<UploadTask>>();
    
                Ok::<Vec<UploadTask>, anyhow::Error>(tasks)
            }
        })).await?;
    
        let mut client_tasks: Vec<Vec<UploadTask>> = client_tasks.into_iter()
            .filter(|tasks| !tasks.is_empty())
            .collect();
    
        client_tasks.sort_by_key(|tasks| std::cmp::Reverse(tasks.len()));
    
        let tasks: Vec<UploadTask> = client_tasks.into_iter().flatten().collect();
    
        Ok(SegmentUploader {
            data,
            tree,
            clients: self.clients.clone(),
            tasks,
            task_size,
        })
    }

    async fn upload_file(
        &self,
        data: Arc<dyn IterableData>,
        tree: Arc<Tree>,
        expected_replica: u32,
        task_size: u32,
    ) -> Result<()> {
        let task_size = task_size.max(DEFAULT_TASK_SIZE);

        log::info!(
            "Begin to upload file: segNum={}, nodeNum={}",
            data.num_segments(),
            self.clients.len()
        );

        let segment_uploader = self
            .new_segment_uploader(data, tree, expected_replica, task_size)
            .await?;

        let tasks = (0..segment_uploader.tasks.len()).map(|task_index| {
            let segment_uploader = segment_uploader.clone();
            tokio::spawn(async move {
                segment_uploader.do_task(task_index)
                    .await
            })
        });

        let results = join_all(tasks).await;

        let mut success_count = 0;
        let mut failure_count = 0;

        for (index, task_result) in results.into_iter().enumerate() {
            match task_result {
                Ok(Ok(_)) => {
                    success_count += 1;
                    log::debug!("Task {} completed successfully", index);
                }
                Ok(Err(e)) => {
                    failure_count += 1;
                    log::error!("Task {} failed: {}", index, e);
                }
                Err(e) => {
                    failure_count += 1;
                    log::error!("Task {} panicked: {}", index, e);
                }
            }
        }

        log::info!(
            "File upload completed: segNum={}, total tasks={}, successful={}, failed={}",
            segment_uploader.data.num_segments(),
            segment_uploader.tasks.len(),
            success_count,
            failure_count
        );

        if failure_count > 0 {
            anyhow::bail!("{} out of {} tasks failed during file upload", failure_count, segment_uploader.tasks.len());
        }

        Ok(())
    }
}

pub async fn get_shard_configs(clients: &[ZgsClient]) -> Result<Vec<ShardConfig>> {
    let shard_config_futures = clients.iter().map(|client| async {
        let shard_config = client
            .get_shard_config()
            .await
            .context("Failed to get shard config")?;

        if !shard_config.is_valid() {
            anyhow::bail!("Invalid shard config: NumShard is zero");
        }

        Ok(shard_config)
    });

    // 并行执行所有 future
    let results = join_all(shard_config_futures).await;

    // 处理结果
    results.into_iter().collect()
}

impl SegmentUploader {
    async fn do_task(&self, task_index: usize) -> Result<()> {
        let upload_task = &self.tasks[task_index];
        let num_chunks = self.data.num_chunks();
        let num_segments = self.data.num_segments();
        let mut seg_index = upload_task.seg_index;
        let start_seg_index = seg_index;
        let mut segments = Vec::new();

        for _ in 0..self.task_size {
            let start_index = seg_index as usize * DEFAULT_SEGMENT_MAX_CHUNKS;
            if start_index >= num_chunks as usize {
                break;
            }

            let segment = read_at(
                self.data.as_ref(),
                DEFAULT_SEGMENT_SIZE as usize,
                (seg_index as usize * DEFAULT_SEGMENT_SIZE) as i64,
                self.data.padded_size(),
            )?;

            if start_index + (segment.len() / DEFAULT_CHUNK_SIZE) >= num_chunks as usize {
                let expected_len = DEFAULT_CHUNK_SIZE * (num_chunks as usize - start_index);
                segments.push(SegmentWithProof {
                    root: self.tree.root(),
                    data: segment[..expected_len].to_vec(),
                    index: seg_index as usize,
                    proof: self.tree.proof_at(seg_index as usize),
                    file_size: self.data.size() as usize,
                });
                break;
            }

            segments.push(SegmentWithProof {
                root: self.tree.root(),
                data: segment,
                index: seg_index as usize,
                proof: self.tree.proof_at(seg_index as usize),
                file_size: self.data.size() as usize,
            });

            seg_index += upload_task.num_shard;
        }

        match self.clients[upload_task.client_index]
            .upload_segments(&segments)
            .await
        {
            Ok(_) => {}
            Err(e) if !is_duplicate_error(&e.to_string()) => return Err(e.into()),
            _ => {}
        }

        log::debug!(
            "Segments uploaded: total={}, from_seg_index={}, to_seg_index={}, step={}, root={:?}",
            num_segments,
            start_seg_index,
            seg_index,
            upload_task.num_shard,
            segment_root(&segments[0].data),
        );

        Ok(())
    }
}

fn is_duplicate_error(msg: &str) -> bool {
    msg.contains("Invalid params: root; data: already uploaded and finalized")
        || msg.contains("segment has already been uploaded or is being uploaded")
}

#[cfg(test)]
mod tests {
    use super::Uploader;
    use crate::common::blockchain::rpc::must_new_web3;
    use crate::core::file::File;
    use crate::node::client_zgs::must_new_zgs_clients;
    use crate::{cmd::upload::UploadOption, common::options::LogOption};
    use std::io::{Seek, SeekFrom, Write};
    use std::path::Path;
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

        match Uploader::new(web3_client, clients, &LogOption::default()).await {
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
        let node_urls = vec![String::from("http://127.0.0.1:5678")];
        let clients = must_new_zgs_clients(&node_urls);

        let chain_url = "https://evmrpc-testnet.0g.ai";
        let key = "0x9ac8c66a0712816db4364b7004c89cd077da0e07b3ec2c0314eeb3b03f8df21e";
        let web3_client = must_new_web3(chain_url, key).await;

        // let data = File::open(create_temp_file_with_content(b"Hello!!!! World!!!!!").path()).unwrap();
        let data = File::open(Path::new("tmp123456")).unwrap();
        let uploader = Uploader::new(web3_client, clients, &LogOption::default())
            .await
            .unwrap();
        let root = uploader
            .upload(Arc::new(data), &UploadOption::default())
            .await
            .unwrap();
        println!("uploaded data root: {:?}", root);
    }
}
