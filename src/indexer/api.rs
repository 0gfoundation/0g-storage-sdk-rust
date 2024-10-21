use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;

use super::node_manager::NODE_MANAGER;
use crate::common::rpc::error;
use crate::common::shard::ShardedNodes;

#[rpc(server, client, namespace = "indexer")]
pub trait Indexer {
    #[method(name = "getShardedNodes")]
    async fn get_sharded_nodes(&self) -> RpcResult<ShardedNodes>;

    #[method(name = "getFileLocations")]
    async fn get_file_locations(&self) -> RpcResult<()>;
}

pub struct IndexerServerImpl;

#[async_trait]
impl IndexerServer for IndexerServerImpl {
    async fn get_sharded_nodes(&self) -> RpcResult<ShardedNodes> {
        let mannager = NODE_MANAGER.lock().await;

        if let Some(manager) = mannager.as_ref() {
            let trusted = manager.trusted().await?;
            let discovered = manager.discovered();
            Ok(ShardedNodes {
                trusted: Some(trusted),
                discovered: Some(discovered),
            })
        } else {
            Err(error::internal_error(format!(
                "Failed to retrieve trusted nodes"
            )))
        }
    }

    async fn get_file_locations(&self) -> RpcResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonrpsee::core::client::ClientT;
    use jsonrpsee::http_client::HttpClientBuilder;
    use jsonrpsee::http_server::{HttpServerBuilder};
    use jsonrpsee::rpc_params;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::common::shard::{ShardedNode, ShardedNodes};
    use crate::indexer::node_manager::{NODE_MANAGER, NodeManager, NodeManagerConfig};

    // Mock NodeManager for testing
    struct MockNodeManager;

    impl MockNodeManager {
        async fn trusted(&self) -> Result<Vec<ShardedNode>, Box<dyn std::error::Error>> {
            Ok(vec![ShardedNode::default()])
        }

        fn discovered(&self) -> Vec<ShardedNode> {
            vec![ShardedNode::default()]
        }
    }

    #[tokio::test]
    async fn test_get_sharded_nodes() {
        // Setup mock NODE_MANAGER

        let config = NodeManagerConfig {
            trusted_nodes: vec!["http://127.0.0.1:5678".to_string()],
            discovery_node: "".to_string(),
            discovery_interval: Duration::from_secs(100),
            discovery_ports: vec![10],
            update_interval: Duration::from_secs(20)
        };
        NodeManager::init(config);

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

        // Call RPC method
        let result: ShardedNodes = client
            .request("indexer_getShardedNodes", rpc_params![])
            .await
            .unwrap();

        // Verify result
        assert!(result.trusted.is_some());
        assert!(result.discovered.is_some());
        assert_eq!(result.trusted.unwrap().len(), 1);
        assert_eq!(result.discovered.unwrap().len(), 1);

        // Shutdown server
        server_handle.stop().unwrap();
    }
}
