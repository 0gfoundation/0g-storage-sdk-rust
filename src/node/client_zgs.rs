use anyhow::Result;
use std::ops::Deref;

use super::types::{FileInfo, SegmentWithProof, Status};
use crate::common::options::LogOption;
use crate::common::rpc::{client::{validate_url, RpcClient}, error::{RpcError, ZgRpcResult}};
use crate::common::shard::ShardConfig;

#[derive(Debug, Clone)]
pub struct ZgsClient {
    pub client: RpcClient,
    pub option: LogOption,
}

impl Deref for ZgsClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl ZgsClient {
    pub fn new(url: &str) -> Result<Self> {
        let url = validate_url(url)?;
        let client = RpcClient::new(&url)?;
        let option = LogOption::default();
        Ok(Self { client, option })
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

    pub async fn get_file_info(&self, root: [u8; 32]) -> ZgRpcResult<Option<FileInfo>> {
        let root = format!("0x{}", hex::encode(root));
        self.client
            .request("zgs_getFileInfo", root)
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
            .request("zgs_uploadSegments", segments)
            .await
            .map_err(|e| RpcError {
                message: e.to_string(),
                method: "zgs_uploadSegments".to_string(),
                url: self.url.clone(),
            })
    }
}

pub fn must_new_zgs_client(url: &String) -> ZgsClient {
    ZgsClient::new(&url).expect("Failed to create ZGS client")
}

pub fn must_new_zgs_clients(urls: &[String]) -> Vec<ZgsClient> {
    urls.iter()
        .map(|url| ZgsClient::new(url).expect("Failed to create ZGS client"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use ethers::types::H256;

    #[tokio::test]
    async fn test_rpc_get_status() {
        let urls = vec![String::from("http://127.0.0.1:5678")];
        let clients = must_new_zgs_clients(&urls);
        let result = clients[0].get_status().await;

        match result {
            Ok(status) => {
                println!("Status: {:?}", status);
            }
            Err(e) => {
                eprintln!("Failed to get status: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_rpc_get_shard_config() {
        let urls = vec![String::from("http://127.0.0.1:5678")];
        let clients = must_new_zgs_clients(&urls);
        let result = clients[0].get_shard_config().await;
        match result {
            Ok(shard_config) => {
                println!("Shard config: {:?}", shard_config);
            }
            Err(e) => {
                eprintln!("Failed to get shard config: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_rpc_get_file_info() {
        let urls = vec![String::from("http://127.0.0.1:5678")];
        let clients = must_new_zgs_clients(&urls);
        let root = H256::from_str("0x089b1799d7152cb83e0e3dc5d58217f7b550f045c5588ca96aa943b632a4a402").unwrap();
        let result = clients[0].get_file_info(root.to_fixed_bytes()).await;
        // log::debug!("result: {:?}", result);
        match result {
            Ok(file_info) => {
                println!("file info: {:?}", file_info);
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                panic!("Failed to get file info: {:?}", e);
            }
        }
    }
}
