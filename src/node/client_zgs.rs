use anyhow::Result;
use jsonrpsee::core::{client::ClientT, Error as JsonRpseeError, rpc_params, RpcResult};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use std::sync::Arc;
use super::types::{FileInfo, SegmentWithProof, Status};
use crate::common::shard::ShardConfig;


#[derive(Debug, Clone)]
pub struct ZgsClient {
    pub client: Arc<HttpClient>,
    pub url: String,
}


impl ZgsClient {
    pub fn new(url: &str) -> Result<Self, JsonRpseeError> {
        let client = HttpClientBuilder::default().build(url)?;
        Ok(Self {
            client: Arc::new(client),
            url: url.to_string(),
        })
    }

    pub async fn get_status(&self) -> RpcResult<Status> {
        self.client.request("zgs_getStatus", rpc_params![]).await
    }

    pub async fn get_file_info(&self, root: [u8; 32]) -> RpcResult<Option<FileInfo>> {
        let root = format!("0x{}", hex::encode(root)); 
        self.client.request("zgs_getFileInfo", rpc_params![root]).await
    }

    pub async fn get_shard_config(&self) -> RpcResult<ShardConfig> {
        self.client.request("zgs_getShardConfig", rpc_params![]).await
    }

    pub async fn upload_segments(&self, segments: &Vec<SegmentWithProof>) -> RpcResult<()> {
        self.client.request("zgs_uploadSegments", rpc_params![segments]).await
    }
}

pub fn must_new_zgs_clients(urls: &[String]) -> Vec<ZgsClient> {
    urls.iter()
        .map(|url| ZgsClient::new(url).expect("Failed to create ZGS client"))
        .collect()
}


#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ethers::types::H256;

    use super::*;
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
                eprintln!("Error: {:?}", e);
                panic!("Failed to get status: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_rpc_get_shard_config() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
        let urls = vec![String::from("http://127.0.0.1:5678")];
        let clients = must_new_zgs_clients(&urls);
        let result = clients[0].get_shard_config().await;
        // log::debug!("result: {:?}", result);
        match result {
            Ok(shard_config) => {
                println!("Shard config: {:?}", shard_config);
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                panic!("Failed to get shard config: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_rpc_get_file_info() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
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