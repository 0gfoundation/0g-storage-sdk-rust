use anyhow::Result;
use serde_json::json;
use std::ops::Deref;
use std::collections::HashMap;

use super::types::{PeerInfo, LocationInfo};
use crate::common::rpc::{
    client::{validate_url, RpcClient},
    error::{RpcError, ZgRpcResult},
};
use crate::common::options::GLOBAL_OPTION;

#[derive(Debug, Clone)]
pub struct AdminClient {
    pub client: RpcClient
}

impl Deref for AdminClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl AdminClient {
    pub async fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let rpc_config = GLOBAL_OPTION.lock().await.rpc_config.clone();
        let client = RpcClient::new(&url, &rpc_config)?;
        Ok(Self { client })
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