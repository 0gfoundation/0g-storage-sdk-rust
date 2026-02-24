use super::error::RpcError;
use crate::common::options::RpcOption;
use anyhow::{Context, Result};
use jsonrpsee::core::{client::ClientT, rpc_params, RpcResult};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::types::ParamsSer;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use url::Url;
#[derive(Debug, Clone)]
pub struct RpcClient {
    pub client: Arc<HttpClient>,
    pub url: String,
    pub rpc_config: RpcOption,
}

impl RpcClient {
    pub fn new(url: &str, rpc_config: &RpcOption) -> RpcResult<Self> {
        let url = validate_url(url)?;
        let client = HttpClientBuilder::default()
            .request_timeout(rpc_config.timeout)
            .build(&url)?;
        Ok(Self {
            client: Arc::new(client),
            url: url.to_string(),
            rpc_config: rpc_config.clone(),
        })
    }

    pub async fn request<R>(&self, method: &str, params: Vec<Value>) -> RpcResult<R>
    where
        R: DeserializeOwned,
    {
        let params = params
            .into_iter()
            .map(|param| match param {
                Value::String(s) if s.len() == 64 => Value::String(format!("0x{}", s)),
                _ => param,
            })
            .collect::<Vec<_>>();

        let mut retry_count = 0;
        loop {
            // Deserialize as raw Value first so we can log the actual response
            // on type-mismatch errors (jsonrpsee swallows the body otherwise).
            let raw_result: Result<Value, _> = self
                .client
                .request(method, Some(ParamsSer::Array(params.clone())))
                .await;
            match raw_result {
                Ok(raw) => match serde_json::from_value::<R>(raw.clone()) {
                    Ok(result) => {
                        log::debug!("RPC {}: Success", method);
                        break Ok(result);
                    }
                    Err(e) => {
                        log::error!(
                            "RPC {} result deserialization failed: {} (raw: {}, url: {})",
                            method,
                            e,
                            raw,
                            self.url
                        );
                        break Err(jsonrpsee::core::Error::ParseError(e));
                    }
                },
                Err(e) => {
                    retry_count += 1;
                    log::error!(
                        "RPC {} failed (attempt {}/{}): {} (url: {})",
                        method,
                        retry_count,
                        self.rpc_config.retry_count,
                        e,
                        self.url
                    );
                    if retry_count >= self.rpc_config.retry_count {
                        break Err(e);
                    }
                    tokio::time::sleep(self.rpc_config.retry_interval).await;
                }
            }
        }
    }

    pub async fn request_no_params<R>(&self, method: &str) -> RpcResult<R>
    where
        R: DeserializeOwned,
    {
        let mut retry_count = 0;
        loop {
            let raw_result: Result<Value, _> = self.client.request(method, rpc_params![]).await;
            match raw_result {
                Ok(raw) => match serde_json::from_value::<R>(raw.clone()) {
                    Ok(result) => {
                        log::debug!("RPC {}: Success", method);
                        break Ok(result);
                    }
                    Err(e) => {
                        log::error!(
                            "RPC {} result deserialization failed: {} (raw: {}, url: {})",
                            method,
                            e,
                            raw,
                            self.url
                        );
                        break Err(jsonrpsee::core::Error::ParseError(e));
                    }
                },
                Err(e) => {
                    retry_count += 1;
                    log::error!(
                        "RPC {} failed (attempt {}/{}): {} (url: {})",
                        method,
                        retry_count,
                        self.rpc_config.retry_count,
                        e,
                        self.url
                    );
                    if retry_count >= self.rpc_config.retry_count {
                        break Err(e);
                    }
                    tokio::time::sleep(self.rpc_config.retry_interval).await;
                }
            }
        }
    }

    pub fn wrap_error<T, E>(&self, result: Result<T, E>, method: &str) -> Result<T, RpcError>
    where
        E: Error,
    {
        result.map_err(|e| RpcError {
            message: e.to_string(),
            method: method.to_string(),
            url: self.url.clone(),
        })
    }
}

pub fn validate_url(url: &str) -> Result<String> {
    let url = Url::parse(url).context(format!("Failed to parse URL: {}", url))?;

    if url.scheme() != "http" && url.scheme() != "https" {
        anyhow::bail!("URL scheme must be http or https");
    }

    if let Some(url_str) = url.to_string().strip_suffix("/") {
        if url.port().is_none() {
            let default_port = if url.scheme() == "https" { 443 } else { 80 };
            Ok(format!("{}:{}", url_str, default_port))
        } else {
            Ok(url_str.to_string())
        }
    } else {
        anyhow::bail!("strip suffix error")
    }
}
