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
use crate::transfer::downloader::Downloader;
use crate::transfer::uploader::{Uploader, Web3Client};

use anyhow::Result;
use ethers::types::H256;
use futures::future::try_join_all;
use serde_json::json;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

pub struct IndexerClient {
    client: RpcClient,
}

impl Deref for IndexerClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl IndexerClient {
    pub async fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let rpc_config = GLOBAL_OPTION.lock().await.rpc_config.clone();
        let client = RpcClient::new(&url, &rpc_config)?;
        Ok(Self { client })
    }

    pub async fn batch_upload(
        &self,
        w3_client: Web3Client,
        datas: Vec<Arc<dyn IterableData>>,
        wait_for_log_entry: bool,
        opt: &BatchUploadOption,
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
                )
                .await?;
            // try batch upload
            match uploader
                .batch_upload(datas.clone(), wait_for_log_entry, opt)
                .await {
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
    ) -> Result<Uploader> {
        let clients = self.select_nodes(expected_replica, dropped).await?;
        let urls: Vec<String> = clients
            .iter()
            .map(|client| client.url.to_string())
            .collect();
        log::info!("Get {} storage nodes from indexer: {:?}", urls.len(), urls);

        Uploader::new(w3_client, clients).await
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

    pub async fn download(&self, root: H256, file: &PathBuf, with_proof: bool) -> Result<()> {
        let locations = self.get_file_locations(root).await?;

        let mut clients = Vec::new();
        if let Some(locations) = locations {
            for location in locations {
                let client = match ZgsClient::new(&location.url).await {
                    Ok(client) => client,
                    Err(e) => {
                        log::error!(
                            "Failed to initialize client of node {}: {}",
                            location.url,
                            e
                        );
                        continue;
                    }
                };

                match client.get_shard_config().await {
                    Ok(config) if config.is_valid() => {
                        clients.push(client);
                    }
                    _ => {
                        log::error!("Failed to get valid shard config of node {}", client.url);
                    }
                }
            }
        }

        if clients.is_empty() {
            anyhow::bail!("No node holding the file found. FindFile triggered, try again later");
        }

        let downloader = Downloader::new(clients)?;

        downloader.download(root, file, with_proof).await
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
        let client = IndexerClient::new("https://indexer-storage-testnet-standard.0g.ai")
            .await
            .unwrap();
        let result = client.get_shard_nodes().await;
        assert!(result.is_ok());
    }
}
