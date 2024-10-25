use crate::cmd::upload::UploadOption;
use crate::common::options::LogOption;
use crate::common::{
    rpc::{
        client::{validate_url, RpcClient},
        error::{RpcError, ZgRpcResult},
    },
    shard::{select, ShardedNode, ShardedNodes},
};
use crate::transfer::downloader::Downloader;
use crate::core::dataflow::IterableData;
use crate::node::client_zgs::{must_new_zgs_client, ZgsClient};
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
    option: LogOption,
}

impl Deref for IndexerClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl IndexerClient {
    pub fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let client = RpcClient::new(&url)?;
        let option = LogOption::default();
        Ok(Self { client, option })
    }

    pub async fn upload(
        &self,
        w3_client: Web3Client,
        data: Arc<dyn IterableData>,
        opt: &UploadOption,
    ) -> Result<H256> {
        // 设置 Uploader 模块的日志级别
        log::set_max_level(self.option.level);
        let mut dropped = Vec::new();

        loop {
            match self
                .new_uploader_from_indexer_nodes(
                    w3_client.clone(),
                    opt.expected_replica as usize,
                    &dropped,
                )
                .await
            {
                Ok(uploader) => match uploader.upload(data.clone(), opt).await {
                    Ok(tx_hash) => return Ok(tx_hash),
                    Err(err) => match err.downcast_ref::<RpcError>() {
                        Some(rpc_err) => {
                            dropped.push(rpc_err.url.clone());
                            log::error!("dropped problematic node and retry");
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

        log::info!("get {} storage nodes from indexer: {:?}", urls.len(), urls);

        Uploader::new(w3_client, clients, &self.option).await
    }

    async fn select_nodes(
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

        Ok(selected
            .into_iter()
            .map(|node| must_new_zgs_client(&node.url))
            .collect())
    }

    async fn validate_nodes(
        &self,
        nodes: Vec<ShardedNode>,
        dropped: &[String],
    ) -> Result<Vec<Option<ShardedNode>>> {
        let futures = nodes
            .into_iter()
            .filter(|node| !dropped.contains(&node.url))
            .map(|mut node| async {
                let client = must_new_zgs_client(&node.url);
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
                let client = match ZgsClient::new(&location.url) {
                    Ok(client) => client,
                    Err(e) => {
                        log::error!(
                            "Failed to initialize client of node {}: {}", location.url, e
                        );
                        continue;
                    }
                };
    
                match client.get_shard_config().await {
                    Ok(config) if config.is_valid() => {
                        clients.push(client);
                    }
                    _ => {
                        log::error!(
                            "Failed to get valid shard config of node {}",
                            client.url
                        );
                    }
                }
            }
        }

        if clients.is_empty() {
            anyhow::bail!("No node holding the file found. FindFile triggered, try again later");
        }

        let downloader = Downloader::new(clients, &LogOption::default())?;

        downloader
            .download(root, file, with_proof)
            .await
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
    use std::str::FromStr;
    use tokio;

    #[tokio::test]
    async fn test_rpc_get_shard_nodes() {
        // env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
        let client = IndexerClient::new("http://127.0.0.1:12345").unwrap();
        let result = client.get_shard_nodes().await;
        // log::info!("result: {:?}", result);
        match result {
            Ok(shard_nodes) => {
                println!("shard_nodes: {:?}", shard_nodes);
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                panic!("Failed to get shard nodes: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_rpc_get_file_locations() {
        let root =
            H256::from_str("0x85a7ce7d6c7cb09f4e56b89b75eb5205ffacaedda838441ec222f650a8793caf")
                .unwrap();

        let client = IndexerClient::new("http://127.0.0.1:12345").unwrap();
        let result = client.get_file_locations(root).await;
        // log::info!("result: {:?}", result);
        match result {
            Ok(shard_nodes) => {
                println!("Shard nodes: {:?}", shard_nodes);
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                panic!("Failed to get shard nodes: {:?}", e);
            }
        }
    }
}
