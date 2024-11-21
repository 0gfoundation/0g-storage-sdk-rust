use anyhow::Result;
use ethers::types::H256;
use log::error;
use std::ops::Deref;
use thiserror::Error;
use serde_json::json;

use crate::common::options::LogOption;
use crate::common::rpc::{
    client::{validate_url, RpcClient},
    error::{RpcError, ZgRpcResult},
};
use crate::node::types::ValueSegment;

use super::types::Segment;

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
    pub option: LogOption,
}

impl Deref for KvClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl KvClient {
    /// Initialize a new KV client
    pub fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let client = RpcClient::new(&url)?;
        let option = LogOption::default();
        Ok(Self { client, option })
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
                vec![json!(stream_id), json!(key), json!(start_index), json!(length), json!(version)],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "kv_getValue".to_string(),
                url: self.url.clone(),
            })
    }

    // GetTransactionResult Call kv_getTransactionResult RPC to query the kv replay status of a given file.
    pub async fn get_transaction_result(
        &self,
        tx_seq: u64
    ) -> ZgRpcResult<Option<String>> {
        self.client
            .request(
                "kv_getTransactionResult",
                vec![json!(tx_seq)],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "kv_getTransactionResult".to_string(),
                url: self.url.clone(),
            })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::utils::pad_to_32_bytes;

    #[tokio::test]
    async fn test_get_value() {
        let client = KvClient::new("http://47.251.78.46:6789").unwrap();
        let stream_id =
        H256::from_slice(pad_to_32_bytes("0xc77b304529058d2a7b8679d3113b7e93".trim_start_matches("0x")).unwrap().as_slice());
        let key = Segment("key1".as_bytes().to_vec());
        let result = client.get_value(stream_id, key, 0, 100, None).await;

        match result {
            Ok(peer_info) => {
                println!("value: {:?}", peer_info);
            }
            Err(e) => {
                eprintln!("Failed to get value: {:?}", e);
            }
        }
    }
}  