use anyhow::Result;
use ethers::types::H256;
use serde_json::json;
use std::ops::Deref;

use super::types::{FileInfo, Segment, SegmentWithProof, Status};
use crate::common::rpc::{
    client::{validate_url, RpcClient},
    error::{RpcError, ZgRpcResult},
};
use crate::common::shard::ShardConfig;
use crate::common::options::GLOBAL_OPTION;

#[derive(Debug, Clone)]
pub struct ZgsClient {
    pub client: RpcClient
}

impl Deref for ZgsClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl ZgsClient {
    pub async fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let rpc_config = GLOBAL_OPTION.lock().await.rpc_config.clone();
        let client = RpcClient::new(&url, &rpc_config)?;
        Ok(Self { client })
    }

    pub async fn get_status(&self) -> ZgRpcResult<Status> {
        self.client
            .request_no_params("zgs_getStatus")
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_getStatus".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn get_file_info(&self, root: H256) -> ZgRpcResult<Option<FileInfo>> {
        self.client
            .request("zgs_getFileInfo", vec![json!(root)])
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_getFileInfo".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn get_shard_config(&self) -> ZgRpcResult<ShardConfig> {
        self.client
            .request_no_params("zgs_getShardConfig")
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_getShardConfig".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn upload_segments(&self, segments: &Vec<SegmentWithProof>) -> ZgRpcResult<()> {
        self.client
            .request("zgs_uploadSegments", vec![json!(segments)])
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_uploadSegments".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn download_segment_with_proof(
        &self,
        root: H256,
        segment_index: u64,
    ) -> ZgRpcResult<Option<SegmentWithProof>> {
        self.client
            .request(
                "zgs_downloadSegmentWithProof",
                vec![json!(root), json!(segment_index)],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_downloadSegmentWithProof".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn download_segment_with_proof_by_tx_seq(
        &self,
        tx_seq: u64,
        segment_index: u64,
    ) -> ZgRpcResult<Option<SegmentWithProof>> {
        self.client
            .request(
                "zgs_downloadSegmentWithProofByTxSeq",
                vec![json!(tx_seq), json!(segment_index)],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_downloadSegmentWithProofByTxSeq".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn download_segment(
        &self,
        root: H256,
        start_index: u64,
        end_index: u64,
    ) -> ZgRpcResult<Option<Segment>> {
        self.client
            .request(
                "zgs_downloadSegment",
                vec![json!(root), json!(start_index), json!(end_index)],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_downloadSegment".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn download_segment_by_tx_seq(
        &self,
        tx_seq: u64,
        start_index: u64,
        end_index: u64,
    ) -> ZgRpcResult<Option<Segment>> {
        self.client
            .request(
                "zgs_downloadSegmentByTxSeq",
                vec![json!(tx_seq), json!(start_index), json!(end_index)],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_downloadSegmentByTxSeq".to_string(),
                url: self.url.clone(),
            })
    }

    pub async fn get_file_info_by_tx_seq(&self, tx_seq: u64) -> ZgRpcResult<Option<FileInfo>> {
        self.client
            .request(
                "zgs_getFileInfoByTxSeq",
                vec![json!(tx_seq)],
            )
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_getFileInfoByTxSeq".to_string(),
                url: self.url.clone(),
            })
    }
}

pub async fn must_new_zgs_client(url: &String) -> ZgsClient {
    ZgsClient::new(&url).await.expect("Failed to create ZGS client")
}

pub async fn must_new_zgs_clients(urls: &[String]) -> Vec<ZgsClient> {
    futures::future::join_all(
        urls.iter().map(|url| ZgsClient::new(url))
    ).await
        .into_iter()
        .map(|result| result.expect("Failed to create ZGS client"))
        .collect()
}