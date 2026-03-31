use crate::cmd::upload::{BatchUploadOption, FinalityRequirement, UploadOption};
use crate::common::blockchain::contract::RetryOption;
use crate::common::shard::{check_replica, ShardConfig};
use crate::contracts::contract::FlowContract;
use crate::contracts::flow::Submission;
use crate::contracts::market::Market;
use crate::core::dataflow::{
    async_read_at, merkle_tree, segment_range, segment_root, IterableData, DEFAULT_CHUNK_SIZE,
    DEFAULT_SEGMENT_MAX_CHUNKS, DEFAULT_SEGMENT_SIZE,
};
use crate::core::flow::Flow;
use crate::core::merkle::tree::Tree;
use crate::node::{
    client_zgs::ZgsClient,
    types::{FileInfo, SegmentWithProof},
};
use anyhow::{Context, Result};
use ethers::providers::Middleware;
use ethers::types::{TransactionReceipt, H256, U256};
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    signers::LocalWallet,
};
use futures::future::try_join_all;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;

const DEFAULT_TASK_SIZE: u32 = 10;

pub struct Uploader {
    pub clients: Vec<ZgsClient>,
    pub flow: FlowContract,
    pub market: Market<SignerMiddleware<Provider<Http>, LocalWallet>>,
}

#[derive(Clone, Debug)]
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

pub type Web3Client = Arc<SignerMiddleware<Provider<Http>, LocalWallet>>;
impl Uploader {
    pub async fn new(
        web3_client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        clients: Vec<ZgsClient>,
    ) -> Result<Self> {
        Self::new_with_addresses(web3_client, clients, None, None).await
    }

    pub async fn new_with_addresses(
        web3_client: Arc<SignerMiddleware<Provider<Http>, LocalWallet>>,
        clients: Vec<ZgsClient>,
        flow_address: Option<Address>,
        market_address: Option<Address>,
    ) -> Result<Self> {
        if clients.is_empty() {
            anyhow::bail!("Storage node not specified");
        }

        // Determine flow address (Go logic)
        let flow_addr = if let Some(addr) = flow_address {
            // Use provided flow address (saves 1 RPC call to storage node)
            addr
        } else {
            // Fetch from storage node (current behavior)
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

            status.network_identity.flow_address
        };

        let flow = FlowContract::new(web3_client.clone(), flow_addr);

        // Determine market address (Go logic)
        let market = if let Some(market_addr) = market_address {
            // Use provided market address (saves 1 RPC call to chain)
            Market::new(market_addr, web3_client.clone())
        } else {
            // Get market from flow contract (current behavior)
            flow.get_market_contract().await?
        };

        Ok(Self {
            clients,
            flow,
            market,
        })
    }

    pub async fn check_log_existence(&self, root: H256) -> Result<Option<FileInfo>> {
        for client in &self.clients {
            match client.get_file_info(root).await {
                Ok(Some(info)) => {
                    return Ok(Some(info));
                }
                Ok(None) => continue,
                Err(err) => return Err(err.into()),
            }
        }
        Ok(None)
    }

    pub async fn batch_upload(
        &self,
        datas: Vec<Arc<dyn IterableData>>,
        wait_for_log_entry: bool,
        opt: &BatchUploadOption,
    ) -> Result<(H256, Vec<H256>)> {
        let start_time = std::time::Instant::now();

        let n = datas.len();
        if n == 0 {
            anyhow::bail!("empty datas");
        }

        let task_size = std::cmp::max(opt.task_size, 1);
        if opt.data_options.len() != n {
            anyhow::bail!("datas and tags length mismatch");
        }

        log::info!("Prepare to upload batchly, dataNum={}", n);

        // calculate merkle tree and check existence
        let mut trees = vec![None; n];
        let mut data_roots = vec![H256::zero(); n];
        let mut file_infos = vec![None; n];

        // control concurrency
        let tasks = futures::stream::iter(0..n).map(|i| {
            let data = datas[i].clone();
            async move {
                log::info!(
                    "Data prepared to upload, size={}, chunks={}, segments={}",
                    data.size(),
                    data.num_chunks(),
                    data.num_segments(),
                );

                // calculate merkle tree
                let tree = merkle_tree(data)
                    .await
                    .context("Failed to create data merkle tree")?;

                log::info!("Data merkle root calculated: {:?}", tree.root());

                // check existence
                let info = self
                    .check_log_existence(tree.root())
                    .await
                    .context("Failed to check if log entry available")?;

                Ok::<_, anyhow::Error>((i, tree, info))
            }
        });

        // control concurrency
        let mut buffered = tasks.buffer_unordered(task_size as usize);
        while let Some(result) = buffered.next().await {
            let (i, tree, info) = result?;
            data_roots[i] = tree.root();
            trees[i] = Some(Arc::new(tree));
            file_infos[i] = info;
        }

        // collect data to submit
        let mut to_submit_datas = Vec::new();
        let mut to_submit_tags = Vec::new();
        let mut last_tree_to_submit = None;

        for i in 0..n {
            let opt = &opt.data_options[i];
            if !opt.skip_tx || file_infos[i].is_none() {
                to_submit_datas.push(datas[i].clone());
                to_submit_tags.push(opt.tags.clone());
                last_tree_to_submit = trees[i].clone();
            }
        }

        // submit log to blockchain
        let (tx_hash, receipt) = if !to_submit_datas.is_empty() {
            let (hash, rcpt) = self
                .submit_log_entry(
                    to_submit_datas,
                    to_submit_tags,
                    wait_for_log_entry,
                    opt.nonce,
                    opt.fee,
                )
                .await
                .context("Failed to submit log entry")?;

            // wait for storage node to get log entry
            if let Some(last_tree) = last_tree_to_submit {
                self.wait_for_log_entry(
                    last_tree.root(),
                    FinalityRequirement::TransactionPacked,
                    rcpt.clone(),
                )
                .await
                .context("Failed to check if log entry available on storage node")?;
            }

            (hash, rcpt)
        } else {
            (H256::zero(), None)
        };

        // second phase: upload file
        let tasks = futures::stream::iter(0..n).map(|i| {
            let data = datas[i].clone();
            let tree = trees[i].clone().unwrap();
            let info = file_infos[i].clone();
            let opt = &opt.data_options[i];
            let receipt = receipt.clone();

            async move {
                // if no file info, wait for log entry
                if info.is_none() {
                    self.wait_for_log_entry(
                        tree.root(),
                        FinalityRequirement::TransactionPacked,
                        receipt.clone(),
                    )
                    .await?;
                }

                // upload file to storage node
                self.upload_file(
                    data,
                    tree.clone(),
                    opt.expected_replica as u32,
                    opt.task_size as u32,
                )
                .await
                .context("Failed to upload file")?;

                // wait for transaction finality
                self.wait_for_log_entry(tree.root(), opt.finality_required, None)
                    .await
                    .context("Failed to wait for transaction finality")?;

                Ok::<_, anyhow::Error>(())
            }
        });

        // control concurrency
        let mut buffered = tasks.buffer_unordered(task_size as usize);
        while let Some(result) = buffered.next().await {
            result?;
        }

        log::info!("batch upload took: {:?}", start_time.elapsed());

        Ok((tx_hash, data_roots))
    }

    /// Upload a (potentially large) file, splitting it into power-of-2-aligned
    /// fragments when it exceeds `fragment_size` bytes.
    ///
    /// Each fragment is submitted to the blockchain and uploaded to the storage
    /// nodes as an independent file.  Returns one Merkle root per fragment in
    /// upload order — pass this list to `Downloader::download_fragments` to
    /// reassemble the original file.
    pub async fn splitable_upload(
        &self,
        data: Arc<dyn IterableData>,
        fragment_size: i64,
        opt: &UploadOption,
    ) -> Result<Vec<H256>> {
        use crate::core::fragment::{next_pow2, split_data};

        // Align to next power of 2, minimum one chunk (256 bytes)
        let frag_size = (next_pow2(fragment_size as u64) as i64)
            .max(crate::core::dataflow::DEFAULT_CHUNK_SIZE as i64);

        let fragments = if data.size() <= frag_size {
            vec![data]
        } else {
            let frags = split_data(data, frag_size);
            log::info!(
                "Split file into {} fragments of up to {} bytes each",
                frags.len(),
                frag_size
            );
            frags
        };

        const DEFAULT_BATCH_SIZE: usize = 10;
        let mut all_roots: Vec<H256> = Vec::new();

        for (batch_idx, batch) in fragments.chunks(DEFAULT_BATCH_SIZE).enumerate() {
            log::info!(
                "Batch submitting fragments {} to {}",
                batch_idx * DEFAULT_BATCH_SIZE,
                batch_idx * DEFAULT_BATCH_SIZE + batch.len()
            );
            let batch_opt = BatchUploadOption {
                data_options: vec![opt.clone(); batch.len()],
                task_size: opt.task_size,
                fee: opt.fee,
                nonce: U256::zero(),
            };
            let (_, roots) = self.batch_upload(batch.to_vec(), false, &batch_opt).await?;
            all_roots.extend(roots);
        }

        Ok(all_roots)
    }

    pub async fn upload(&self, data: Arc<dyn IterableData>, opt: &UploadOption) -> Result<H256> {
        let start_time = Instant::now();

        let tree = Arc::new(merkle_tree(data.clone()).await?);
        let root = tree.root();
        let file_info = self.check_log_existence(root).await?;
        let tx_hash = if !opt.skip_tx || file_info.is_none() {
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
            log::info!("Tx[{:?}] receipt: {:?}", tx_hash, receipt);

            // Always wait for log entry to be available on storage nodes (uploadSlow path)
            // This is required before uploading segments to ensure the tx is synced
            let _ = self
                .wait_for_log_entry(root, FinalityRequirement::TransactionPacked, receipt)
                .await;

            tx_hash
        } else {
            log::info!("Data[{:?}] have been uploaded or skiped", root);
            H256::zero()
        };

        self.upload_file(
            data,
            tree,
            opt.expected_replica as u32,
            opt.task_size as u32,
        )
        .await?;
        let _ = self
            .wait_for_log_entry(root, opt.finality_required, None)
            .await;

        log::info!("Upload completed in {:?}", start_time.elapsed());
        Ok(tx_hash)
    }

    async fn submit_log_entry(
        &self,
        datas: Vec<Arc<dyn IterableData>>,
        tags: Vec<Vec<u8>>,
        wait_for_receipt: bool,
        nonce: U256,
        fee: U256,
    ) -> Result<(H256, Option<TransactionReceipt>)> {
        let submitter = self.flow.account;
        let submission_futures: Vec<_> = datas
            .into_iter()
            .zip(tags.into_iter())
            .map(|(data, tag)| {
                let flow = Flow::new(data, tag);
                async move {
                    flow.create_submission(submitter)
                        .await
                        .context("Failed to create submission")
                }
            })
            .collect();

        let submissions: Vec<Submission> = try_join_all(submission_futures)
            .await
            .context("Failed to create submissions")?;

        let price_per_sector = self.market.price_per_sector().call().await?;
        log::debug!("Price per sector: {:?}", price_per_sector);

        let mut opts: TransactionRequest = self.flow.create_transact_opts().await;
        opts = opts.nonce(nonce);

        let tx = if submissions.len() == 1 {
            let fee = if fee.is_zero() {
                submissions[0].fee(&price_per_sector)
            } else {
                fee
            };
            log::info!("Submit with fee: {}", fee);
            self.flow.submit(submissions[0].clone(), fee, opts).await?
        } else {
            let total_fee = if fee.is_zero() {
                submissions
                    .iter()
                    .map(|s| s.fee(&price_per_sector))
                    .fold(U256::zero(), |acc, fee| acc + fee)
            } else {
                submissions
                    .iter()
                    .map(|_| fee)
                    .fold(U256::zero(), |acc, fee| acc + fee)
            };
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
        if finality_required == FinalityRequirement::WaitNothing {
            return Ok(());
        }

        log::info!(
            "Wait for log entry on storage node: root={:?}, finality={:?}",
            root,
            finality_required
        );

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let mut ok = true;
            for client in &self.clients {
                match client.get_file_info(root).await {
                    Ok(Some(info)) => {
                        if finality_required <= FinalityRequirement::FileFinalized
                            && !info.finalized
                        {
                            log::info!(
                                "Log entry is available, but not finalized yet: tx_seq={}, cached={}, uploaded_segments={}",
                                info.tx.seq, info.is_cached, info.uploaded_seg_num
                            );
                            ok = false;
                            break;
                        } else {
                            log::info!(
                                "Log entry available: tx_seq={}, finalized={}, uploaded_segments={}",
                                info.tx.seq, info.finalized, info.uploaded_seg_num
                            );
                        }
                    }
                    Ok(None) => {
                        if let Some(ref receipt) = receipt {
                            if let Ok(status) = client.get_status().await {
                                log::info!(
                                    "Log entry is unavailable yet: tx_block_number={:?}, zgs_node_sync_height={}",
                                    receipt.block_number, status.log_sync_height
                                );
                            }
                        } else {
                            log::info!("Log entry is unavailable yet");
                        }
                        ok = false;
                        break;
                    }
                    Err(e) => anyhow::bail!("Failed to get file info: {}", e),
                }
            }

            if ok {
                break;
            }
        }

        Ok(())
    }

    pub async fn new_segment_uploader(
        &self,
        data: Arc<dyn IterableData>,
        tree: Arc<Tree>,
        expected_replica: u32,
        task_size: u32,
    ) -> Result<SegmentUploader> {
        let shard_configs = get_shard_configs(&self.clients);
        log::debug!("Storage node shard configs: {:?}", shard_configs);

        if !check_replica(&shard_configs, expected_replica) {
            anyhow::bail!("Selected nodes cannot cover all shards");
        }

        let root = tree.root();
        log::debug!("tree root used query file status: {:?}", root);

        let client_tasks = try_join_all(self.clients.iter().enumerate().map(
            |(client_index, client)| {
                let shard_config = shard_configs[client_index].clone();
                async move {
                    let info = client
                        .get_file_info(root)
                        .await
                        .context("Failed to get file info")?;
                    log::debug!("file info queryed before upload: {:?}", info);

                    if let Some(info) = info {
                        if info.finalized {
                            return Ok::<Vec<UploadTask>, anyhow::Error>(Vec::new());
                        }

                        let (start_segment_index, end_segment_index) = segment_range(
                            info.tx.start_entry_index as usize,
                            info.tx.size as usize,
                        );

                        let mut seg_index =
                            shard_config.next_segment_index(start_segment_index as u64);

                        let mut tasks = Vec::new();

                        while seg_index <= end_segment_index as u64 {
                            tasks.push(UploadTask {
                                client_index,
                                seg_index: seg_index - start_segment_index as u64,
                                num_shard: shard_config.num_shard,
                            });

                            seg_index += shard_config.num_shard * task_size as u64;
                        }

                        Ok::<Vec<UploadTask>, anyhow::Error>(tasks)
                    } else {
                        anyhow::bail!("")
                    }
                }
            },
        ))
        .await?;

        let mut client_tasks: Vec<Vec<UploadTask>> = client_tasks
            .into_iter()
            .filter(|tasks| !tasks.is_empty())
            .collect();

        client_tasks.sort_by_key(|tasks| std::cmp::Reverse(tasks.len()));

        let tasks: Vec<UploadTask> = client_tasks.into_iter().flatten().collect();
        log::debug!("Tasks: {:?}", tasks);

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
            async move {
                segment_uploader
                    .do_task(task_index)
                    .await
                    .with_context(|| format!("Task {} failed", task_index))
            }
        });

        let results = try_join_all(tasks).await?;

        log::info!(
            "File upload completed: segNum={}, total tasks={}",
            segment_uploader.data.num_segments(),
            results.len()
        );

        Ok(())
    }
}

pub fn get_shard_configs(clients: &[ZgsClient]) -> Vec<ShardConfig> {
    clients.iter().map(|c| c.shard_config().clone()).collect()
}

impl SegmentUploader {
    async fn do_task(&self, task_index: usize) -> Result<()> {
        let upload_task = &self.tasks[task_index];
        let num_chunks = self.data.num_chunks();
        let num_segments = self.data.num_segments();
        let mut seg_index = upload_task.seg_index;
        // log::info!("seg_index: {:?}", seg_index);
        let start_seg_index = seg_index;
        let mut segments = Vec::new();

        for _ in 0..self.task_size {
            let start_index = seg_index as usize * DEFAULT_SEGMENT_MAX_CHUNKS;
            if start_index >= num_chunks as usize {
                break;
            }

            let segment = async_read_at(
                self.data.clone(),
                DEFAULT_SEGMENT_SIZE,
                (seg_index as usize * DEFAULT_SEGMENT_SIZE) as i64,
                self.data.padded_size(),
            )
            .await?;

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

        #[cfg(not(test))]
        {
            match self.clients[upload_task.client_index]
                .upload_segments(&segments)
                .await
            {
                Ok(_) => {}
                Err(e) if !is_duplicate_error(&e.to_string()) => return Err(e.into()),
                _ => {}
            }
        }

        #[cfg(test)]
        {
            for segment in &segments {
                log::info!(
                    "root: {:?}, index: {:?}, proof: {:?}",
                    segment.root,
                    segment.index,
                    segment.proof
                );
            }
        }

        log::info!(
            "Segments uploaded: total={}, from_seg_index={}, to_seg_index={}, step={}, root={:?}",
            num_segments,
            start_seg_index,
            seg_index,
            upload_task.num_shard,
            segment_root(&segments[0].data, 0),
        );

        Ok(())
    }
}

#[cfg(not(test))]
fn is_duplicate_error(msg: &str) -> bool {
    msg.contains("Invalid params: root; data: already uploaded and finalized")
        || msg.contains("segment has already been uploaded or is being uploaded")
}
