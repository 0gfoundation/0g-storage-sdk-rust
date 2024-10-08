use std::fmt;
use std::fmt::Debug;

pub type ZgRpcResult<T> = std::result::Result<T, RpcError>;

#[derive(Debug, Clone)]
pub struct RpcError {
    pub message: String,
    pub method: String,
    pub url: String,
}

impl std::error::Error for RpcError {}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RPC error: {} (method: {}, url: {})",
            self.message, self.method, self.url
        )
    }
}

impl From<std::io::Error> for RpcError {
    fn from(err: std::io::Error) -> Self {
        RpcError {
            message: err.to_string(),
            method: String::new(),
            url: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rpc::client::RpcClient;
    use anyhow::{Context, Result};

    async fn inner(client: &RpcClient, result_type: usize) -> Result<String> {
        match result_type {
            0 => Ok("Success".to_string()),
            1 => {
                let rpc_error = RpcError {
                    message: "Connection failed".to_string(),
                    method: "GET".to_string(),
                    url: client.url.clone(),
                };
                Err(rpc_error).with_context(|| format!("Failed to get status from storage node {}", client.url))
            },
            _ => Err(anyhow::anyhow!("Some other error occurred")),
        }
    } 

    async fn outer(client: &RpcClient, result_type: usize) -> String {
        match inner(client, result_type).await {
            Ok(_) => {
                println!("Ok!");
                "Ok!".to_string()
            }
            Err(err) => {
                if let Some(_) = err.downcast_ref::<RpcError>() {
                    println!("Rpc error!");
                    "Rpc error!".to_string()
                } else {
                    println!("Other error!");
                    "Other error!".to_string()
                }
            }
        }
    }

    #[tokio::test]
    async fn test_rpc_error_propagation() {
        let client = RpcClient::new("http://127.0.0.1:5678").unwrap();
        let ok = outer(&client, 0).await;
        assert_eq!(ok, "Ok!".to_string());
        let rpc_err = outer(&client, 1).await;
        assert_eq!(rpc_err, "Rpc error!".to_string());
        let other_err = outer(&client, 2).await;
        assert_eq!(other_err, "Other error!".to_string());
    }
}
