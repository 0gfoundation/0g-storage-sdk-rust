use std::sync::Arc;
use anyhow::Result;
use ethers::types::H256;
use crate::cmd::upload::UploadOption;
use crate::core::dataflow::IterableData;

pub struct IndexerClient {
    // Fields for indexer client
}

pub struct IndexerClientOption {

}

impl IndexerClient {
    pub fn new(url: &str, opt: &IndexerClientOption) -> Result<Self> {
        // Initialize indexer client
        todo!()
    }

    pub async fn upload(&self, file: Arc<dyn IterableData>, opt: &UploadOption) -> Result<H256> {
        // 实现上传逻辑
        todo!()
    }
}
