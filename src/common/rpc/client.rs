use anyhow::{Context, Result};
use jsonrpsee::core::{client::ClientT, rpc_params, RpcResult};
use jsonrpsee::types::ParamsSer;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use url::Url;
use super::error::RpcError;

#[derive(Debug, Clone)]
pub struct RpcClient {
    pub client: Arc<HttpClient>,
    pub url: String,
}

impl RpcClient {
    pub fn new(url: &str) -> RpcResult<Self> {
        let url = validate_url(url)?;
        let client = HttpClientBuilder::default().build(&url)?;
        Ok(Self {
            client: Arc::new(client),
            url: url.to_string(),
        })
    }

    pub async fn request<R>(&self, method: &str, params: Vec<Value>) -> RpcResult<R>
    where
        R: DeserializeOwned,
    {
        let params = params.into_iter()
            .map(|param| match param {
                Value::String(s) if s.len() == 64 => Value::String(format!("0x{}", s)),
                _ => param,
            })
            .collect::<Vec<_>>();

        self.client.request(method, Some(ParamsSer::Array(params))).await
    }

    pub async fn request_no_params<R>(&self, method: &str) -> RpcResult<R>
    where
        R: DeserializeOwned,
    {
        self.client.request(method, rpc_params![]).await
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
            return Ok(format!("{}:{}", url_str, default_port))
        } else {
            return Ok(url_str.to_string());
        }
    } else {
        anyhow::bail!("strip suffix error")
    }
}
