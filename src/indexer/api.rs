use ethers::types::H256;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use std::collections::HashMap;
use std::str::FromStr;

use super::file_location_cache::DefaultFileLocationCache;
use super::ip_location::DefaultIPLocationManager;
use super::ip_location::IPLocation;
use super::node_manager::DefaultNodeManger;
use crate::common::rpc::error;
use crate::common::shard::{ShardedNode, ShardedNodes};

#[rpc(server, client, namespace = "indexer")]
pub trait Indexer {
    #[method(name = "getShardedNodes")]
    async fn get_sharded_nodes(&self) -> RpcResult<ShardedNodes>;

    #[method(name = "getNodeLocations")]
    async fn get_node_locations(&self) -> RpcResult<HashMap<String, IPLocation>>;

    #[method(name = "getFileLocations")]
    async fn get_file_locations(&self, root: String) -> RpcResult<Option<Vec<ShardedNode>>>;
}

#[derive(Clone)]
pub struct IndexerServerImpl;

#[async_trait]
impl IndexerServer for IndexerServerImpl {
    async fn get_sharded_nodes(&self) -> RpcResult<ShardedNodes> {
        let trusted = DefaultNodeManger::trusted().await?;
        let discovered = DefaultNodeManger::discovered().await?;
        Ok(ShardedNodes {
            trusted: Some(trusted),
            discovered: Some(discovered),
        })
    }

    async fn get_node_locations(&self) -> RpcResult<HashMap<String, IPLocation>> {
        Ok(DefaultIPLocationManager::all().await?)
    }

    async fn get_file_locations(&self, root: String) -> RpcResult<Option<Vec<ShardedNode>>> {
        let root = H256::from_str(&root)
            .map_err(|e| error::invalid_params(root.as_str(), e.to_string()))?;
        let trusted_clients = DefaultNodeManger::trusted_clients().await?;

        let mut found = false;
        let mut tx_seq = 0;
        for client in trusted_clients {
            match client.get_file_info(root).await {
                Ok(Some(info)) => {
                    found = true;
                    tx_seq = info.tx.seq;
                    break;
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
        if !found {
            return Err(error::internal_error(""));
        }
        return DefaultFileLocationCache::get_file_locations(tx_seq)
            .await
            .map_err(|e| error::internal_error(e.to_string()));
    }
}