use anyhow::Result;
use serde_json::json;
use std::ops::Deref;
use std::collections::HashMap;

use super::types::{PeerInfo, LocationInfo};
use crate::common::options::LogOption;
use crate::common::rpc::{
    client::{validate_url, RpcClient},
    error::{RpcError, ZgRpcResult},
};


#[derive(Debug, Clone)]
pub struct AdminClient {
    pub client: RpcClient,
    pub option: LogOption,
}

impl Deref for AdminClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl AdminClient {
    pub fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let client = RpcClient::new(&url)?;
        let option = LogOption::default();
        Ok(Self { client, option })
    }

    pub async fn get_peers(&self) -> ZgRpcResult<Option<HashMap<String, PeerInfo>>> {
        self.client
            .request_no_params("admin_getPeers")
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "admin_getPeers".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn get_file_location(&self, tx_seq: u64, all_shards: bool) -> ZgRpcResult<Vec<LocationInfo>> {
        self.client
            .request("admin_getFileLocation", vec![json!(tx_seq), json!(all_shards)])
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "admin_getFileLocation".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn find_file(&self, tx_seq: u64) -> ZgRpcResult<i32> {
        self.client
            .request("admin_findFile", vec![json!(tx_seq)])
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "admin_findFile".to_string(),
                url: self.url.clone(),
            })
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rpc_get_peers() {
        let client = AdminClient::new("https://indexer-storage-testnet-standard.0g.ai:5679").unwrap();
        let result = client.get_peers().await;

        match result {
            Ok(peer_info) => {
                println!("peer info: {:?}", peer_info);
            }
            Err(e) => {
                eprintln!("Failed to get peer info]: {:?}", e);
            }
        }
    }
}