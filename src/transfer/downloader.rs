use super::download::download_file::DownloadingFile;
use crate::common::shard::ShardConfig;
use crate::core::dataflow::{
    merkle_tree, num_splits, padded_segment_root, DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_MAX_CHUNKS,
    DEFAULT_SEGMENT_SIZE,
};
use crate::core::file::File;
use crate::node::client_zgs::ZgsClient;
use crate::node::types::FileInfo;

use anyhow::{Context, Result};
use ethers::types::H256;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Downloader {
    pub clients: Vec<ZgsClient>,
    routines: usize,
}

struct DownloadSegmentConfig<'a> {
    clients: &'a [ZgsClient],
    shard_configs: &'a [ShardConfig],
    task: u64,
    routine: usize,
    offset: u64,
    start_segment_index: u64,
    end_segment_index: u64,
    num_chunks: u64,
    tx_seq: u64,
    with_proof: bool,
    root: H256,
    file_size: usize,
}

impl Downloader {
    pub fn new(clients: Vec<ZgsClient>) -> Result<Self> {
        if clients.is_empty() {
            anyhow::bail!("Storage node not specified");
        }

        let default_routines = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        Ok(Self {
            clients,
            routines: default_routines,
        })
    }

    pub fn with_routines(mut self, routines: usize) -> Self {
        self.routines = routines;
        self
    }

    pub async fn download(&self, root: H256, file: &PathBuf, with_proof: bool) -> Result<()> {
        // Query file info from storage node
        let info = self
            .query_file(root)
            .await
            .context("Failed to query file info")?;

        // Check file existence before downloading
        self.check_existence(root, file)
            .await
            .context("Failed to check file existence")?;

        // Download segments
        self.download_file(file, root, info.tx.size as usize, with_proof, &info)
            .await
            .context("Failed to download file")?;

        // Validate the downloaded file
        self.validate_download_file(root, file, info.tx.size)
            .await
            .context("Failed to validate downloaded file")?;

        Ok(())
    }

    async fn query_file(&self, root: H256) -> Result<FileInfo> {
        for client in &self.clients {
            match client.get_file_info(root).await {
                Ok(Some(info)) => {
                    return Ok(info);
                }
                Ok(None) => {
                    log::error!("File[{:?}] not found on node[{}]", root, client.url);
                    continue;
                }
                Err(e) => {
                    log::error!("Failed to get file info from node[{}]: {}", client.url, e);
                    continue;
                }
            }
        }
        anyhow::bail!("File[{:?}] not found on any node", root)
    }

    async fn check_existence(&self, root: H256, file: &PathBuf) -> Result<()> {
        if !file.exists() {
            return Ok(());
        }
        let data = Arc::new(File::open(file)?);

        let tree = Arc::new(merkle_tree(data.clone()).await?);

        if tree.root() == root {
            anyhow::bail!("File already exists");
        } else {
            anyhow::bail!("File already exists with different hash");
        }
    }

    async fn download_file(
        &self,
        file: &Path,
        root: H256,
        size: usize,
        with_proof: bool,
        info: &FileInfo,
    ) -> Result<()> {
        let mut file = DownloadingFile::create(file, root, size)?;
        log::info!(
            "Begin to download file from {} storage nodes",
            self.clients.len()
        );

        let shard_configs = get_shard_configs(&self.clients).await?;

        let mut sd = SegmentDownloader::new(
            &self.clients,
            shard_configs,
            &mut file,
            with_proof,
            info,
            self.routines,
        )?;

        sd.download().await?;

        file.seal()?;

        log::info!("Completed to download file");
        Ok(())
    }

    async fn validate_download_file(
        &self,
        root: H256,
        file: &PathBuf,
        file_size: u64,
    ) -> Result<()> {
        if file.metadata()?.len() != file_size {
            anyhow::bail!(
                "File size mismatch: expected = {}, downloaded = {}",
                file_size,
                file.metadata()?.len()
            );
        }

        let data = Arc::new(File::open(file)?);

        let tree = Arc::new(merkle_tree(data.clone()).await?);

        if tree.root() != root {
            anyhow::bail!("Merkle root mismatch, downloaded = {:?}", tree.root());
        }

        log::info!("Succeeded to validate the downloaded file");

        Ok(())
    }
}

struct SegmentDownloader<'a> {
    clients: &'a Vec<ZgsClient>,
    shard_configs: Vec<ShardConfig>,
    file: &'a mut DownloadingFile,
    tx_seq: u64,
    start_segment_index: u64,
    end_segment_index: u64,
    with_proof: bool,
    offset: u64,
    num_chunks: u64,
    routines: usize,
}

impl<'a> SegmentDownloader<'a> {
    fn new(
        clients: &'a Vec<ZgsClient>,
        shard_configs: Vec<ShardConfig>,
        file: &'a mut DownloadingFile,
        with_proof: bool,
        info: &FileInfo,
        routines: usize,
    ) -> Result<Self> {
        let start_segment_index = info.tx.start_entry_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let end_segment_index = (info.tx.start_entry_index
            + num_splits(info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64
            - 1)
            / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let offset = (file.metadata().offset / DEFAULT_SEGMENT_SIZE) as u64;

        log::info!(
            "Start downloading file: size={}, startEntryIndex={}, numChunks={}, startSegmentIndex={}, endSegmentIndex={}",
            info.tx.size,
            info.tx.start_entry_index,
            num_splits(info.tx.size as usize, DEFAULT_CHUNK_SIZE),
            start_segment_index,
            end_segment_index
        );

        Ok(Self {
            clients,
            shard_configs,
            file,
            tx_seq: info.tx.seq,
            start_segment_index,
            end_segment_index,
            with_proof,
            offset,
            num_chunks: num_splits(info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64,
            routines,
        })
    }

    async fn download(&mut self) -> Result<()> {
        let num_tasks = self.end_segment_index - self.start_segment_index + 1 - self.offset;

        // Clone data needed for parallel download
        let clients = self.clients.clone();
        let shard_configs = self.shard_configs.clone();
        let tx_seq = self.tx_seq;
        let start_segment_index = self.start_segment_index;
        let end_segment_index = self.end_segment_index;
        let offset = self.offset;
        let num_chunks = self.num_chunks;
        let with_proof = self.with_proof;
        let root = self.file.metadata().root;
        let file_size = self.file.metadata().size;
        let routines = self.routines;

        // Download segments with controlled concurrency
        let mut results: Vec<(u64, Vec<u8>)> = stream::iter(0..num_tasks)
            .map(|task| {
                let clients = clients.clone();
                let shard_configs = shard_configs.clone();
                let routine = (task % routines as u64) as usize;

                async move {
                    let segment = Self::download_segment_static(DownloadSegmentConfig {
                        clients: &clients,
                        shard_configs: &shard_configs,
                        task,
                        routine,
                        offset,
                        start_segment_index,
                        end_segment_index,
                        num_chunks,
                        tx_seq,
                        with_proof,
                        root,
                        file_size,
                    })
                    .await?;
                    Ok::<(u64, Vec<u8>), anyhow::Error>((task, segment))
                }
            })
            .buffer_unordered(routines)
            .try_collect()
            .await?;

        // Sort results by task index to maintain order
        results.sort_by_key(|(task, _)| *task);

        // Write segments in order
        for (_, segment) in results {
            self.file.write(&segment)?;
        }

        Ok(())
    }

    async fn download_segment_static(config: DownloadSegmentConfig<'_>) -> Result<Vec<u8>> {
        let segment_index = config.offset + config.task;
        // there is no not-aligned & segment-crossed file
        let start_index = segment_index * DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let mut end_index = start_index + DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        if end_index > config.num_chunks {
            end_index = config.num_chunks;
        }

        // Round-robin node selection starting from routine index
        for i in 0..config.clients.len() {
            let node_index = (config.routine + i) % config.clients.len();
            let shard_config = &config.shard_configs[node_index];

            if (config.start_segment_index + segment_index) % shard_config.num_shard
                != shard_config.shard_id
            {
                continue;
            }

            let client = &config.clients[node_index];

            let segment = if config.with_proof {
                Self::download_with_proof_static(
                    client,
                    config.root,
                    start_index,
                    end_index,
                    config.tx_seq,
                    config.file_size,
                )
                .await?
            } else if let Some(data) = client
                .download_segment_by_tx_seq(config.tx_seq, start_index, end_index)
                .await?
            {
                Some(data.0)
            } else {
                None
            };

            if let Some(mut segment) = segment {
                if segment.len() % DEFAULT_CHUNK_SIZE != 0 {
                    log::warn!(
                        "Invalid segment length from node {}, segment {}/({}-{}), chunks [{}, {})",
                        node_index,
                        config.start_segment_index + segment_index,
                        config.start_segment_index,
                        config.end_segment_index,
                        start_index,
                        end_index
                    );
                    continue;
                }

                // Remove paddings for the last chunk
                if config.start_segment_index + segment_index == config.end_segment_index {
                    let last_chunk_size = config.file_size % DEFAULT_CHUNK_SIZE;
                    if last_chunk_size > 0 {
                        let paddings = DEFAULT_CHUNK_SIZE - last_chunk_size;
                        segment.truncate(segment.len() - paddings);
                    }
                }

                log::debug!(
                    "Succeeded to download segment from node {}, segment {}/({}-{}), chunks [{}, {})",
                    node_index,
                    config.start_segment_index + segment_index,
                    config.start_segment_index,
                    config.end_segment_index,
                    start_index,
                    end_index
                );
                return Ok(segment);
            } else {
                log::warn!(
                    "Segment not found on node {}, segment {}/({}-{}), chunks [{}, {})",
                    node_index,
                    config.start_segment_index + segment_index,
                    config.start_segment_index,
                    config.end_segment_index,
                    start_index,
                    end_index
                );
            }
        }

        anyhow::bail!("Failed to download segment {}", segment_index)
    }

    async fn download_with_proof_static(
        client: &ZgsClient,
        root: H256,
        start_index: u64,
        end_index: u64,
        tx_seq: u64,
        file_size: usize,
    ) -> Result<Option<Vec<u8>>> {
        let segment_index = start_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let segment = client
            .download_segment_with_proof_by_tx_seq(tx_seq, segment_index)
            .await?;

        if let Some(segment) = segment {
            let expected_data_len = (end_index - start_index) * DEFAULT_CHUNK_SIZE as u64;
            if expected_data_len as usize != segment.data.len() {
                anyhow::bail!(
                    "Downloaded data length mismatch, expected = {}, actual = {}",
                    expected_data_len,
                    segment.data.len()
                );
            }

            let (segment_root, num_segments_flow_padded) =
                padded_segment_root(segment_index, &segment.data, file_size as u64);

            segment
                .proof
                .validate_hash(root, segment_root, segment_index, num_segments_flow_padded)
                .context("Failed to validate proof")?;

            Ok(Some(segment.data))
        } else {
            Ok(None)
        }
    }
}

pub async fn get_shard_configs(clients: &Vec<ZgsClient>) -> Result<Vec<ShardConfig>> {
    let mut shard_configs = Vec::new();

    for client in clients {
        let shard_config = client.get_shard_config().await?;
        shard_configs.push(shard_config);
    }

    Ok(shard_configs)
}
