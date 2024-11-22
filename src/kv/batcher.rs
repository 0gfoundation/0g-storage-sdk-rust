use std::sync::Arc;
use anyhow::{Result, Context};
use ethers::types::H256;

use crate::transfer::uploader::{Uploader, Web3Client};
use crate::cmd::upload::UploadOption;
use crate::node::client_zgs::ZgsClient;
use super::builder::StreamDataBuilder;
use crate::core::in_mem::DataInMemory;

/// Batcher struct to cache and execute KV write and access control operations.
#[derive(Debug)]
pub struct Batcher {
    stream_data_builder: StreamDataBuilder,
    clients: Vec<ZgsClient>,
    web3_client: Web3Client,
}

impl Batcher {
    /// Initialize a new batcher.
    /// Version denotes the expected version of keys to read or write when the cached KV operations
    /// is settled on chain.
    pub fn new(
        version: u64,
        clients: Vec<ZgsClient>,
        web3_client: Web3Client,
    ) -> Self {
        Self {
            stream_data_builder: StreamDataBuilder::new(version),
            clients,
            web3_client
        }
    }

    /// Serialize the cached KV operations in Batcher, then submit the serialized data to 0g storage network.
    /// The submission process is the same as uploading a normal file. The batcher should be dropped after execution.
    /// 
    /// Note: this may be a time-consuming operation, e.g. several seconds or even longer.
    /// When it comes to a time-sensitive context, it should be executed in a separate task44eewe.
    pub async fn exec(
        &self,
        option: &mut UploadOption,
    ) -> Result<H256> {
        // Build stream data
        let stream_data = self.stream_data_builder
            .build(None)
            .context("Failed to build stream data")?;

        let encoded = stream_data
            .encode()
            .context("Failed to encode data")?;

        let data = DataInMemory::new(encoded)
            .context("Failed to create data in memory")?;

        // Create uploader
        let uploader = Uploader::new(
            self.web3_client.clone(),
            self.clients.clone()
        ).await.context("Failed to create uploader")?;

        // Prepare upload options
        option.tags = self.stream_data_builder.build_tags(None);

        // Upload file
        let tx_hash = uploader
            .upload(Arc::new(data), option)
            .await
            .context("Failed to upload data")?;

        Ok(tx_hash)
    }

    // Forward methods to stream_data_builder
    pub fn set(&mut self, stream_id: H256, key: &[u8], value: Vec<u8>) -> &mut Self {
        self.stream_data_builder.set(stream_id, key, value);
        self
    }

    pub fn watch(&mut self, stream_id: H256, key: &[u8]) -> &mut Self {
        self.stream_data_builder.watch(stream_id, key);
        self
    }

    pub fn set_version(&mut self, version: u64) -> &mut Self {
        self.stream_data_builder.set_version(version);
        self
    }
}