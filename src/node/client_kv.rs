use anyhow::{Context, Result};
use ethers::types::{Address, H256};
use log::error;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use serde_json::json;

use crate::common::options::LogOption;
use crate::common::rpc::{
    client::{validate_url, RpcClient},
    error::{RpcError, ZgRpcResult},
};
use crate::node::types::{KeyValueSegment, ValueSegment};

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

    // /// Call kv_getNext RPC to query the next key of a given key
    // pub async fn get_next(
    //     &self,
    //     stream_id: H256,
    //     key: &[u8],
    //     start_index: u64,
    //     length: u64,
    //     inclusive: bool,
    //     version: Option<u64>,
    // ) -> Result<KeyValue> {
    //     let mut args = vec![
    //         stream_id.into(),
    //         key.into(),
    //         start_index.into(),
    //         length.into(),
    //         inclusive.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_getNext", args)
    //         .await
    //         .context("kv_getNext call failed")
    // }

    // /// Call kv_getPrev RPC to query the prev key of a given key
    // pub async fn get_prev(
    //     &self,
    //     stream_id: H256,
    //     key: &[u8],
    //     start_index: u64,
    //     length: u64,
    //     inclusive: bool,
    //     version: Option<u64>,
    // ) -> Result<KeyValue> {
    //     let mut args = vec![
    //         stream_id.into(),
    //         key.into(),
    //         start_index.into(),
    //         length.into(),
    //         inclusive.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_getPrev", args)
    //         .await
    //         .context("kv_getPrev call failed")
    // }

    // /// Call kv_getFirst RPC to query the first key
    // pub async fn get_first(
    //     &self,
    //     stream_id: H256,
    //     start_index: u64,
    //     length: u64,
    //     version: Option<u64>,
    // ) -> Result<KeyValue> {
    //     let mut args = vec![
    //         stream_id.into(),
    //         start_index.into(),
    //         length.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_getFirst", args)
    //         .await
    //         .context("kv_getFirst call failed")
    // }

    // /// Call kv_getLast RPC to query the last key
    // pub async fn get_last(
    //     &self,
    //     stream_id: H256,
    //     start_index: u64,
    //     length: u64,
    //     version: Option<u64>,
    // ) -> Result<KeyValue> {
    //     let mut args = vec![
    //         stream_id.into(),
    //         start_index.into(),
    //         length.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_getLast", args)
    //         .await
    //         .context("kv_getLast call failed")
    // }

    // /// Call kv_getTransactionResult RPC to query the kv replay status of a given file
    // pub async fn get_transaction_result(&self, tx_seq: u64) -> Result<String> {
    //     self.rpc_client
    //         .call("kv_getTransactionResult", vec![tx_seq.into()])
    //         .await
    //         .context("kv_getTransactionResult call failed")
    // }

    // /// Call kv_getHoldingStreamIds RPC to query the stream ids monitored by the kv node
    // pub async fn get_holding_stream_ids(&self) -> Result<Vec<H256>> {
    //     self.rpc_client
    //         .call("kv_getHoldingStreamIds", vec![])
    //         .await
    //         .context("kv_getHoldingStreamIds call failed")
    // }

    // /// Call kv_hasWritePermission RPC to check if the account is able to write the stream
    // pub async fn has_write_permission(
    //     &self,
    //     account: Address,
    //     stream_id: H256,
    //     key: &[u8],
    //     version: Option<u64>,
    // ) -> Result<bool> {
    //     let mut args = vec![
    //         account.into(),
    //         stream_id.into(),
    //         key.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_hasWritePermission", args)
    //         .await
    //         .context("kv_hasWritePermission call failed")
    // }

    // /// Call kv_isAdmin RPC to check if the account is the admin of the stream
    // pub async fn is_admin(
    //     &self,
    //     account: Address,
    //     stream_id: H256,
    //     version: Option<u64>,
    // ) -> Result<bool> {
    //     let mut args = vec![
    //         account.into(),
    //         stream_id.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_isAdmin", args)
    //         .await
    //         .context("kv_isAdmin call failed")
    // }

    // /// Call kv_isSpecialKey RPC to check if the key has unique access control
    // pub async fn is_special_key(
    //     &self,
    //     stream_id: H256,
    //     key: &[u8],
    //     version: Option<u64>,
    // ) -> Result<bool> {
    //     let mut args = vec![
    //         stream_id.into(),
    //         key.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_isSpecialKey", args)
    //         .await
    //         .context("kv_isSpecialKey call failed")
    // }

    // /// Call kv_isWriterOfKey RPC to check if the account can write the special key
    // pub async fn is_writer_of_key(
    //     &self,
    //     account: Address,
    //     stream_id: H256,
    //     key: &[u8],
    //     version: Option<u64>,
    // ) -> Result<bool> {
    //     let mut args = vec![
    //         account.into(),
    //         stream_id.into(),
    //         key.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_isWriterOfKey", args)
    //         .await
    //         .context("kv_isWriterOfKey call failed")
    // }

    // /// Call kv_isWriterOfStream RPC to check if the account is the writer of the stream
    // pub async fn is_writer_of_stream(
    //     &self,
    //     account: Address,
    //     stream_id: H256,
    //     version: Option<u64>,
    // ) -> Result<bool> {
    //     let mut args = vec![
    //         account.into(),
    //         stream_id.into(),
    //     ];

    //     if let Some(ver) = version {
    //         args.push(ver.into());
    //     }

    //     self.rpc_client
    //         .call("kv_isWriterOfStream", args)
    //         .await
    //         .context("kv_isWriterOfStream call failed")
    // }
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