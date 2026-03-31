use crate::cmd::upload::{BatchUploadOption, UploadOption};
use crate::common::options::GLOBAL_OPTION;
use crate::common::{
    rpc::{
        client::{validate_url, RpcClient},
        error::{RpcError, ZgRpcResult},
    },
    shard::{select, ShardedNode, ShardedNodes},
};
use crate::core::dataflow::IterableData;
use crate::node::client_zgs::{must_new_zgs_client, ZgsClient};
use crate::transfer::downloader::{DownloadContext, Downloader};
use crate::transfer::uploader::{Uploader, Web3Client};

use anyhow::{Context, Result};
use ethers::types::H256;
use futures::future::try_join_all;
use serde_json::json;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct IndexerClientOption {
    /// Number of routines for concurrent downloads/uploads
    pub routines: usize,
}

impl Default for IndexerClientOption {
    fn default() -> Self {
        Self {
            routines: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        }
    }
}

pub struct IndexerClient {
    client: RpcClient,
    option: IndexerClientOption,
}

impl Deref for IndexerClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl IndexerClient {
    pub async fn new(url: &str) -> Result<Self> {
        Self::new_with_option(url, IndexerClientOption::default()).await
    }

    pub async fn new_with_option(url: &str, option: IndexerClientOption) -> Result<Self> {
        let url = validate_url(url)?;
        let rpc_config = GLOBAL_OPTION.lock().await.rpc_config.clone();
        let client = RpcClient::new(&url, &rpc_config)?;
        Ok(Self { client, option })
    }

    /// Upload a (potentially large) file via the indexer, splitting it into
    /// power-of-2-aligned fragments when it exceeds `fragment_size` bytes.
    ///
    /// Returns one Merkle root per fragment in upload order.
    pub async fn splitable_upload(
        &self,
        w3_client: Web3Client,
        data: Arc<dyn IterableData>,
        fragment_size: i64,
        opt: &UploadOption,
        flow_address: Option<ethers::types::Address>,
        market_address: Option<ethers::types::Address>,
    ) -> Result<Vec<H256>> {
        use crate::core::fragment::{next_pow2, split_data};
        use ethers::types::U256;

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
                fee: U256::zero(),
                nonce: U256::zero(),
            };
            let (_, roots) = self
                .batch_upload(
                    w3_client.clone(),
                    batch.to_vec(),
                    false,
                    &batch_opt,
                    flow_address,
                    market_address,
                )
                .await?;
            all_roots.extend(roots);
        }

        Ok(all_roots)
    }

    pub async fn batch_upload(
        &self,
        w3_client: Web3Client,
        datas: Vec<Arc<dyn IterableData>>,
        wait_for_log_entry: bool,
        opt: &BatchUploadOption,
        flow_address: Option<ethers::types::Address>,
        market_address: Option<ethers::types::Address>,
    ) -> Result<(H256, Vec<H256>)> {
        let expected_replica = opt
            .data_options
            .iter()
            .map(|opt| opt.expected_replica)
            .max()
            .unwrap_or(1)
            .max(1);

        // record problematic nodes
        let mut dropped = Vec::<String>::new();

        loop {
            // create uploader from indexer nodes
            let uploader = self
                .new_uploader_from_indexer_nodes(
                    w3_client.clone(),
                    expected_replica as usize,
                    &dropped,
                    flow_address,
                    market_address,
                )
                .await?;
            // try batch upload
            match uploader
                .batch_upload(datas.clone(), wait_for_log_entry, opt)
                .await
            {
                Ok((hash, roots)) => {
                    return Ok((hash, roots));
                }
                Err(err) => {
                    dropped.push(err.to_string());
                    log::error!("Dropped problematic node and retry");
                }
            }
        }
    }

    pub async fn upload(
        &self,
        w3_client: Web3Client,
        data: Arc<dyn IterableData>,
        opt: &UploadOption,
        flow_address: Option<ethers::types::Address>,
        market_address: Option<ethers::types::Address>,
    ) -> Result<H256> {
        let mut dropped = Vec::new();
        let mut expected_replica = 1;
        if opt.expected_replica > 0 {
            expected_replica = opt.expected_replica;
        }
        loop {
            match self
                .new_uploader_from_indexer_nodes(
                    w3_client.clone(),
                    expected_replica as usize,
                    &dropped,
                    flow_address,
                    market_address,
                )
                .await
            {
                Ok(uploader) => match uploader.upload(data.clone(), opt).await {
                    Ok(tx_hash) => return Ok(tx_hash),
                    Err(err) => match err.downcast_ref::<RpcError>() {
                        Some(rpc_err) => {
                            dropped.push(rpc_err.url.clone());
                            log::error!("Dropped problematic node and retry");
                        }
                        None => {
                            return Err(err);
                        }
                    },
                },
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn get_shard_nodes(&self) -> ZgRpcResult<ShardedNodes> {
        self.client
            .request_no_params("indexer_getShardedNodes")
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "indexer_getShardedNodes".to_string(),
                url: self.url.clone(),
            })
    }

    async fn new_uploader_from_indexer_nodes(
        &self,
        w3_client: Web3Client,
        expected_replica: usize,
        dropped: &[String],
        flow_address: Option<ethers::types::Address>,
        market_address: Option<ethers::types::Address>,
    ) -> Result<Uploader> {
        let clients = self.select_nodes(expected_replica, dropped).await?;
        let urls: Vec<String> = clients
            .iter()
            .map(|client| client.url.to_string())
            .collect();
        log::info!("Get {} storage nodes from indexer: {:?}", urls.len(), urls);

        Uploader::new_with_addresses(w3_client, clients, flow_address, market_address).await
    }

    pub async fn select_nodes(
        &self,
        expected_replica: usize,
        dropped: &[String],
    ) -> Result<Vec<ZgsClient>> {
        let ShardedNodes { trusted, .. } = self.get_shard_nodes().await?;
        let valid_nodes = if let Some(trusted) = trusted {
            self.validate_nodes(trusted, dropped).await?
        } else {
            Vec::new()
        };

        let mut non_empty_nodes: Vec<_> = valid_nodes.into_iter().flatten().collect();

        if non_empty_nodes.len() < expected_replica {
            anyhow::bail!("Cannot select enough nodes to meet the replication requirement. Available: {}, Required: {}", non_empty_nodes.len(), expected_replica);
        }

        let (selected, ok) = select(
            non_empty_nodes.as_mut_slice(),
            expected_replica as u32,
            true,
        );

        if !ok {
            anyhow::bail!("Failed to select shard nodes");
        }

        let futures: Vec<_> = selected
            .into_iter()
            .map(|node| async move { Ok(must_new_zgs_client(&node.url).await) })
            .collect();
        try_join_all(futures).await
    }

    async fn validate_nodes(
        &self,
        nodes: Vec<ShardedNode>,
        dropped: &[String],
    ) -> Result<Vec<Option<ShardedNode>>> {
        let futures = nodes
            .into_iter()
            .filter(|node| !dropped.contains(&node.url))
            .map(|mut node| async move {
                let client = must_new_zgs_client(&node.url).await;
                let start = Instant::now();
                match client.get_shard_config().await {
                    Ok(config) if config.is_valid() => {
                        node.config = config;
                        node.latency = start.elapsed().as_millis() as i64;
                        Ok(Some(node))
                    }
                    Ok(config) => {
                        log::error!(
                            "Invalid shard config received from node {}: {:?}",
                            node.url,
                            config
                        );
                        Ok(None)
                    }
                    Err(e) => {
                        log::error!("Failed to get shard config from node {}: {}", node.url, e);
                        Ok(None)
                    }
                }
            });

        try_join_all(futures).await
    }

    /// Download a list of fragment files (each identified by its root hash) via
    /// the indexer and concatenate them into a single output file.
    ///
    /// Each root hash is resolved independently through the indexer so that
    /// fragments stored on different nodes are handled transparently.
    /// Pass `encryption_key` to decrypt a file that was encrypted before splitting.
    pub async fn download_fragments(
        &self,
        roots: Vec<H256>,
        file: &PathBuf,
        with_proof: bool,
        encryption_key: Option<&[u8; 32]>,
    ) -> Result<()> {
        if roots.is_empty() {
            anyhow::bail!("No root hashes provided");
        }

        use std::io::Write as IoWrite;
        let mut out = std::fs::File::create(file)
            .with_context(|| format!("Failed to create output file {:?}", file))?;

        use crate::transfer::encryption::{decrypt_fragment_data, EncryptionHeader};
        let mut header: Option<EncryptionHeader> = None;
        let mut data_offset: u64 = 0;

        for (i, root) in roots.iter().enumerate() {
            let temp_path = file.with_extension(format!("part.{}", i));

            self.download(*root, &temp_path, with_proof)
                .await
                .with_context(|| format!("Failed to download fragment {}", i))?;

            let fragment_data = std::fs::read(&temp_path)
                .with_context(|| format!("Failed to read fragment {}", i))?;
            let _ = std::fs::remove_file(&temp_path);

            if let Some(key) = encryption_key {
                if i == 0 {
                    header = Some(
                        EncryptionHeader::parse(&fragment_data)
                            .context("Failed to parse encryption header from fragment 0")?,
                    );
                }
                let hdr = header.as_ref().unwrap();
                let (plaintext, new_offset) =
                    decrypt_fragment_data(key, hdr, &fragment_data, i == 0, data_offset)
                        .with_context(|| format!("Failed to decrypt fragment {}", i))?;
                data_offset = new_offset;
                out.write_all(&plaintext)
                    .with_context(|| format!("Failed to write fragment {}", i))?;
            } else {
                out.write_all(&fragment_data)
                    .with_context(|| format!("Failed to write fragment {}", i))?;
            }

            log::info!("Downloaded and appended fragment {}/{}", i + 1, roots.len());
        }

        log::info!(
            "Completed assembling {} fragments into {:?}",
            roots.len(),
            file
        );
        Ok(())
    }

    pub async fn download(&self, root: H256, file: &PathBuf, with_proof: bool) -> Result<()> {
        let locations = self.get_file_locations(root).await?;

        if locations.is_none() {
            anyhow::bail!(
                "File not found or shards incomplete, FindFile triggered, try again later"
            );
        }

        let mut locations = locations.unwrap();

        // Use select() to check shard coverage
        let (selected, covered) = select(&mut locations, 1, true);
        if !covered {
            anyhow::bail!(
                "File not found or shards incomplete, FindFile triggered, try again later"
            );
        }

        let mut clients = Vec::new();
        for location in selected {
            let client =
                match ZgsClient::new_with_shard_config(&location.url, location.config).await {
                    Ok(client) => client,
                    Err(_) => {
                        log::debug!(
                            "Failed to initialize client of node {}, dropped.",
                            location.url
                        );
                        continue;
                    }
                };

            clients.push(client);
        }

        if clients.is_empty() {
            anyhow::bail!("No node holding the file found, FindFile triggered, try again later");
        }

        log::info!(
            "Get storage nodes from indexer: {:?}",
            clients.iter().map(|c| &c.url).collect::<Vec<_>>()
        );

        let downloader = Downloader::new(clients, self.option.routines)?;

        downloader.download(root, file, with_proof).await
    }

    /// Resolve storage nodes and file metadata, returning a reusable
    /// `DownloadContext` for downloading multiple segments without repeated
    /// indexer queries.
    ///
    /// Exactly one of `root` or `tx_seq` must be provided.
    pub async fn get_download_context(
        &self,
        root: Option<H256>,
        tx_seq: Option<u64>,
    ) -> Result<DownloadContext> {
        match (root, tx_seq) {
            (Some(_), Some(_)) => anyhow::bail!("Cannot specify both root and tx_seq"),
            (None, None) => anyhow::bail!("Must specify either root or tx_seq"),
            _ => {}
        }

        let (clients, file_info, file_root) = if let Some(root) = root {
            let locations = self.get_file_locations(root).await?;
            if locations.is_none() {
                anyhow::bail!(
                    "File not found or shards incomplete, FindFile triggered, try again later"
                );
            }

            let mut locations = locations.unwrap();
            let (selected, covered) = select(&mut locations, 1, true);
            if !covered {
                anyhow::bail!(
                    "File not found or shards incomplete, FindFile triggered, try again later"
                );
            }

            let mut clients = Vec::new();
            for location in selected {
                if let Ok(client) =
                    ZgsClient::new_with_shard_config(&location.url, location.config).await
                {
                    clients.push(client);
                }
            }
            if clients.is_empty() {
                anyhow::bail!("No node holding the file found");
            }

            let info = clients[0].get_file_info(root).await?;
            if info.is_none() {
                anyhow::bail!("File info not found for root {:?}", root);
            }
            (clients, info.unwrap(), root)
        } else {
            let ShardedNodes { trusted, .. } = self.get_shard_nodes().await?;
            let valid_nodes = if let Some(trusted) = trusted {
                self.validate_nodes(trusted, &[]).await?
            } else {
                Vec::new()
            };

            let futures: Vec<_> = valid_nodes
                .into_iter()
                .flatten()
                .map(|node| async move {
                    Result::<ZgsClient, anyhow::Error>::Ok(must_new_zgs_client(&node.url).await)
                })
                .collect();
            let clients: Vec<ZgsClient> = try_join_all(futures).await?;
            if clients.is_empty() {
                anyhow::bail!("No storage nodes available");
            }

            let info = clients[0].get_file_info_by_tx_seq(tx_seq.unwrap()).await?;
            if info.is_none() {
                anyhow::bail!("File info not found for tx_seq {}", tx_seq.unwrap());
            }
            let info = info.unwrap();
            let file_root = info.tx.data_merkle_root;
            (clients, info, file_root)
        };

        log::info!(
            "Resolved {} storage nodes for download context: {:?}",
            clients.len(),
            clients.iter().map(|c| &c.url).collect::<Vec<_>>()
        );

        DownloadContext::new(clients, self.option.routines, file_info, file_root)
    }

    /// Download a specific segment by either root hash or transaction sequence number.
    ///
    /// For downloading many segments of the same file, prefer `get_download_context()`
    /// to avoid repeated node resolution.
    pub async fn download_segment(
        &self,
        root: Option<H256>,
        tx_seq: Option<u64>,
        segment_index: u64,
        with_proof: bool,
        clients: Option<Vec<ZgsClient>>,
    ) -> Result<(Vec<u8>, H256)> {
        if let Some(clients) = clients {
            // Pre-selected clients path
            if clients.is_empty() {
                anyhow::bail!("Provided clients list is empty");
            }
            let info = if let Some(root) = root {
                clients[0].get_file_info(root).await?
            } else if let Some(tx_seq) = tx_seq {
                clients[0].get_file_info_by_tx_seq(tx_seq).await?
            } else {
                anyhow::bail!("Must specify either root or tx_seq");
            };
            if info.is_none() {
                anyhow::bail!("File info not found");
            }
            let info = info.unwrap();
            let file_root = info.tx.data_merkle_root;

            let downloader = Downloader::new(clients, self.option.routines)?;
            let segment = downloader
                .download_segment(file_root, info.tx.seq, segment_index, with_proof, &info)
                .await?;
            Ok((segment, file_root))
        } else {
            // Delegate to get_download_context for indexer-resolved path
            let ctx = self.get_download_context(root, tx_seq).await?;
            let segment = ctx.download_segment(segment_index, with_proof).await?;
            Ok((segment, ctx.file_root()))
        }
    }

    pub async fn get_file_locations(&self, root: H256) -> ZgRpcResult<Option<Vec<ShardedNode>>> {
        self.client
            .request("indexer_getFileLocations", vec![json!(root)])
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "indexer_getFileLocations".to_string(),
                url: self.url.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_rpc_get_shard_nodes() {
        let client = IndexerClient::new("https://indexer-storage-testnet-turbo.0g.ai")
            .await
            .unwrap();
        let result = client.get_shard_nodes().await;
        assert!(result.is_ok());
    }
}
