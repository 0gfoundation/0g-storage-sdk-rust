use anyhow::Result;
use jsonrpsee::core::{client::ClientT, Error as JsonRpseeError, rpc_params};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use std::sync::Arc;
use log::{debug, error, info};
use super::types::{Status, FileInfo};


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

    pub async fn get_status(&self) -> Result<Status, JsonRpseeError> {
        self.client.request("zgs_getStatus", rpc_params![]).await
    }

    pub async fn get_file_info(&self, root: [u8; 32]) -> Result<Option<FileInfo>, JsonRpseeError> {
        let root = format!("0x{}", hex::encode(root));  // 转换为十六进制字符串
        self.client.request("zgs_getFileInfo", rpc_params![root]).await
    }
}

pub fn must_new_zgs_clients(urls: &[String]) -> Vec<ZgsClient> {
    urls.iter()
        .map(|url| ZgsClient::new(url).expect("Failed to create ZGS client"))
        .collect()
}


#[cfg(test)]
mod tests {
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
}