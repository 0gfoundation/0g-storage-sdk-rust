use anyhow::Result;
use ethers::types::H256;
use log::error;
use serde_json::json;
use std::ops::Deref;
use thiserror::Error;

use super::types::Segment;
use crate::common::options::GLOBAL_OPTION;
use crate::common::rpc::{
    client::{validate_url, RpcClient},
    error::{RpcError, ZgRpcResult},
};
use crate::node::types::ValueSegment;

#[derive(Error, Debug)]
pub enum KvClientError {
    #[error("Failed to create KV client: {0}")]
    Creation(String),
    #[error("RPC call failed: {0}")]
    RpcCall(String),
}

#[derive(Debug, Clone)]
pub struct KvClient {
    pub client: RpcClient,
}

impl Deref for KvClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl KvClient {
    /// Initialize a new KV client
    pub async fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let rpc_config = GLOBAL_OPTION.lock().await.rpc_config.clone();
        let client = RpcClient::new(&url, &rpc_config)?;
        Ok(Self { client })
    }

    /// Call kv_getValue RPC to query the value of a key
    pub async fn get_value(
        &self,
        stream_id: H256,
        key: Segment,
        start_index: u64,
        length: u64,
        version: Option<u64>,
    ) -> ZgRpcResult<ValueSegment> {
        self.client
            .request(
                "kv_getValue",
                vec![
                    json!(stream_id),
                    json!(key),
                    json!(start_index),
                    json!(length),
                    json!(version),
                ],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "kv_getValue".to_string(),
                url: self.url.clone(),
            })
    }

    // GetTransactionResult Call kv_getTransactionResult RPC to query the kv replay status of a given file.
    pub async fn get_transaction_result(&self, tx_seq: u64) -> ZgRpcResult<Option<String>> {
        self.client
            .request("kv_getTransactionResult", vec![json!(tx_seq)])
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "kv_getTransactionResult".to_string(),
                url: self.url.clone(),
            })
    }
}
