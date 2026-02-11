use anyhow::{bail, Context, Result};
use ethers::types::U256;
use std::env;
use std::sync::Arc;

use zg_storage_client::cmd::upload::{FinalityRequirement, UploadOption};
use zg_storage_client::common::blockchain::rpc::must_new_web3;
use zg_storage_client::core::{dataflow::merkle_tree, in_mem::DataInMemory};
use zg_storage_client::indexer::client::IndexerClient;

async fn run_test() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    println!("args: {:?}", args);

    let key = &args[0];
    let chain_url = &args[1];
    let zgs_node_urls: Vec<&str> = args[2].split(',').collect();
    let indexer_url = &args[3];

    let w3client = must_new_web3(chain_url, key).await;

    let data = DataInMemory::new("indexer_test_data".as_bytes().to_vec())
        .context("Failed to initialize data")?;

    let indexer_client = IndexerClient::new(indexer_url)
        .await
        .context("Failed to initialize indexer client")?;

    let opt = UploadOption {
        tags: vec![],
        finality_required: FinalityRequirement::FileFinalized,
        task_size: 0,
        expected_replica: 0,
        skip_tx: false,
        fee: U256::default(),
        nonce: U256::default(),
    };

    let data = Arc::new(data);
    indexer_client
        .upload(w3client, data.clone(), &opt, None, None)
        .await
        .context("Failed to upload file")?;

    let tree = merkle_tree(data.clone()).await?;
    let root = tree.root();
    println!("root: {:?}", root);

    let locations = indexer_client
        .get_file_locations(root)
        .await
        .context("Failed to get file locations")?;
    println!("locations: {:?}", locations);

    if let Some(locations) = locations {
        if locations.len() != 1 {
            bail!("Unexpected file location length: {}", locations.len());
        }

        if locations[0].url != zgs_node_urls[0] {
            bail!("Unexpected file location: {}", locations[0].url);
        }
    }

    Ok(())
}
#[tokio::main]
async fn main() {
    run_test().await.unwrap();
}
