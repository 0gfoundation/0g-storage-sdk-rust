use super::download::download_file::DownloadingFile;
use crate::common::options::LogOption;
use crate::common::shard::ShardConfig;
use crate::core::dataflow::{
    merkle_tree, num_splits, padded_segment_root, segment_root, DEFAULT_CHUNK_SIZE,
    DEFAULT_SEGMENT_MAX_CHUNKS, DEFAULT_SEGMENT_SIZE,
};
use crate::core::file::File;
use crate::core::flow::compute_padded_size;
use crate::node::client_zgs::ZgsClient;
use crate::node::types::FileInfo;

use anyhow::{Context, Result};
use ethers::types::H256;
use futures::future::try_join_all;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Downloader {
    pub clients: Vec<ZgsClient>,
}

impl Downloader {
    pub fn new(clients: Vec<ZgsClient>, log_opt: &LogOption) -> Result<Self> {
        env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or(log_opt.level.as_str()),
        )
        .init();

        if clients.is_empty() {
            anyhow::bail!("Storage node not specified");
        }

        Ok(Self { clients })
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
        file: &PathBuf,
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

        let mut sd =
            SegmentDownloader::new(&self.clients, shard_configs, &mut file, with_proof, info)?;

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
}

impl<'a> SegmentDownloader<'a> {
    fn new(
        clients: &'a Vec<ZgsClient>,
        shard_configs: Vec<ShardConfig>,
        file: &'a mut DownloadingFile,
        with_proof: bool,
        info: &FileInfo,
    ) -> Result<Self> {
        let start_segment_index = info.tx.start_entry_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let end_segment_index = (info.tx.start_entry_index
            + num_splits(info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64
            - 1)
            / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let offset = (file.metadata().offset / DEFAULT_SEGMENT_SIZE) as u64;

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
        })
    }

    async fn download(&mut self) -> Result<()> {
        let num_tasks = self.end_segment_index - self.start_segment_index + 1 - self.offset;
        // println!("num_tasks: {}", num_tasks);
        let futures = (0..num_tasks).map(|task| self.download_segment(task));

        let results = try_join_all(futures).await?;
        // println!("result: {:?}", results);
        for result in results {
            self.file.write(&result)?;
        }

        Ok(())
    }

    async fn download_segment(&self, task: u64) -> Result<Vec<u8>> {
        let segment_index = self.offset + task;
        // there is no not-aligned & segment-crossed file
        let start_index = segment_index * DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let mut end_index = start_index + DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        if end_index > self.num_chunks {
            end_index = self.num_chunks;
        }

        let root = self.file.metadata().root;

        for (i, shard_config) in self.shard_configs.iter().enumerate() {
            if (self.start_segment_index + segment_index) % shard_config.num_shard
                != shard_config.shard_id
            {
                continue;
            }

            let client = &self.clients[i];

            let segment = if self.with_proof {
                self.download_with_proof(client, root, start_index, end_index, self.tx_seq)
                    .await?
            } else {
                if let Some(data) = client
                    .download_segment_by_tx_seq(self.tx_seq, start_index as u64, end_index as u64)
                    .await?
                {
                    // println!("segment: {:?}", data);
                    Some(data.0)
                } else {
                    None
                }
            };

            if let Some(mut segment) = segment {
                if segment.len() % DEFAULT_CHUNK_SIZE != 0 {
                    log::error!("Invalid segment length");
                    continue;
                }

                // Remove paddings for the last chunk
                if self.start_segment_index + segment_index == self.end_segment_index {
                    let file_size = self.file.metadata().size;
                    let last_chunk_size = file_size % DEFAULT_CHUNK_SIZE;
                    let mut paddings = DEFAULT_CHUNK_SIZE - last_chunk_size;
                    if paddings == DEFAULT_CHUNK_SIZE {
                        paddings = 0;
                    }
                    segment.truncate(segment.len() - paddings);
                }

                log::info!("Succeeded to download segment");
                // println!("final segment: {:?}", segment);
                return Ok(segment);
            }
        }

        anyhow::bail!("Failed to download segment {}", segment_index)
    }

    async fn download_with_proof(
        &self,
        client: &ZgsClient,
        root: H256,
        start_index: u64,
        end_index: u64,
        tx_seq: u64,
    ) -> Result<Option<Vec<u8>>> {
        let segment_index = start_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let segment = client
            .download_segment_with_proof_by_tx_seq(tx_seq, segment_index)
            .await?;

        // println!("segment_with_proof: {:?}", segment);

        if let Some(segment) = segment {
            let expected_data_len = (end_index - start_index) * DEFAULT_CHUNK_SIZE as u64;
            if expected_data_len as usize != segment.data.len() {
                anyhow::bail!(
                    "Downloaded data length mismatch, expected = {}, actual = {}",
                    expected_data_len,
                    segment.data.len()
                );
            }

            let (segment_root, num_segments_flow_padded) = padded_segment_root(
                segment_index,
                &segment.data,
                self.file.metadata().size as u64,
            );

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
