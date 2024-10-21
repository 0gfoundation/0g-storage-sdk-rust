use anyhow::Result;
use std::ops::Deref;
use std::collections::HashMap;

use super::types::{PeerInfo};
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rpc_get_peers() {
        let client = AdminClient::new("http://95.217.78.195:5679").unwrap();
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