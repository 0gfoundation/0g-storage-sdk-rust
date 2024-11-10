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

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::core::client::ClientT;
    use jsonrpsee::http_client::HttpClientBuilder;
    use jsonrpsee::http_server::HttpServerBuilder;
    use jsonrpsee::rpc_params;
    use std::time::Duration;

    use crate::common::shard::ShardedNodes;
    use crate::indexer::file_location_cache::FileLocationCacheConfig;
    use crate::indexer::ip_location::IPLocationConfig;
    use crate::indexer::node_manager::NodeManagerConfig;

    #[tokio::test]
    async fn test_get_sharded_nodes() {
        // Setup mock NODE_MANAGER
        let node_config = NodeManagerConfig {
            trusted_nodes: vec!["http://127.0.0.1:5678".to_string()],
            discovery_node: "".to_string(),
            discovery_interval: Duration::from_secs(10),
            discovery_ports: vec![],
            update_interval: Duration::from_secs(5),
        };
        let _ = DefaultNodeManger::init(node_config).await;

        let ip_config = IPLocationConfig {
            cache_file: "test_ip_chache.json".to_string(),
            cache_write_interval: Duration::from_secs(10),
            access_token: "test_token".to_string(),
        };

        let _ = DefaultIPLocationManager::init(ip_config);

        // Start server
        let server = HttpServerBuilder::default()
            .build("127.0.0.1:1234")
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();
        let server_handle = server.start(IndexerServerImpl.into_rpc()).unwrap();

        // Create client
        let client = HttpClientBuilder::default()
            .build(format!("http://{}", server_addr))
            .unwrap();

        // Call indexer_getShardedNodes method
        let result: ShardedNodes = client
            .request("indexer_getShardedNodes", rpc_params![])
            .await
            .unwrap();

        // Verify result
        assert!(result.trusted.is_some());
        assert!(result.discovered.is_some());
        assert_eq!(result.trusted.unwrap().len(), 1);

        // Call indexer_getFileLocations method
        let file_location_config = FileLocationCacheConfig {
            cache_size: 1024 * 1024,
            expiry: Duration::from_secs(100),
            discovery_node: "".to_string(),
            discovery_ports: vec![],
        };
        let _ = DefaultFileLocationCache::init(file_location_config).await;

        let root =
            H256::from_str("0x85a7ce7d6c7cb09f4e56b89b75eb5205ffacaedda838441ec222f650a8793caf")
                .unwrap();
        let result: Option<Vec<ShardedNode>> = client
            .request("indexer_getFileLocations", rpc_params![root])
            .await
            .unwrap();

        println!("result: {:?}", result);
        // Shutdown server
        server_handle.stop().unwrap();
    }
}
