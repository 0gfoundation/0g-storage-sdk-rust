use anyhow::{bail, Context, Result};
use ethers::types::U256;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use zg_storage_client::{
    common::options::{init_global_config, init_logging},
    cmd::upload::{BatchUploadOption, FinalityRequirement, UploadOption},
    common::blockchain::rpc::must_new_web3,
    core::{dataflow::merkle_tree, in_mem::DataInMemory},
    indexer::client::IndexerClient,
};
use zg_storage_client::core::dataflow::IterableData;

async fn run_test() -> Result<()> {
    // load arguments
    let args: Vec<String> = env::args().skip(1).collect();
    println!("args: {:?}", args);

    let key = &args[0];
    let chain_url = &args[1];
    let zgs_node_urls: Vec<&str> = args[2].split(',').collect();
    let indexer_url = &args[3];

    // init web3 client
    let w3client = must_new_web3(chain_url, key).await;

    // init global config
    init_global_config(
        None,
        None,
        false,
        100,
        Duration::from_secs(1),
        Duration::from_secs(600),
    )
    .await?;

    // prepare batch upload
    let mut datas = Vec::with_capacity(10);
    let mut opts = Vec::with_capacity(10);

    for i in 0..10 {
        let data = DataInMemory::new(format!("indexer_test_data_{}", i).into_bytes())
            .context("Failed to initialize data")?;
        datas.push(Arc::new(data) as Arc<dyn IterableData>);
        
        opts.push(UploadOption {
            tags: vec![],
            finality_required: FinalityRequirement::FileFinalized,
            task_size: 0,
            expected_replica: 0,
            skip_tx: false,
            fee: U256::default(),
            nonce: U256::default(),
        });
    }

    // create indexer client
    let indexer_client = IndexerClient::new(indexer_url)
        .await
        .context("Failed to initialize indexer client")?;

    // batch upload
    let batch_opt = BatchUploadOption {
        task_size: 5,
        data_options: opts,
        fee: U256::default(),
        nonce: U256::default(),
    };

    let (_, roots) = indexer_client
        .batch_upload(w3client, datas, true, &batch_opt)
        .await
        .context("Failed to upload files")?;

    // check file locations
    for root in roots {
        let locations = indexer_client
            .get_file_locations(root)
            .await
            .context("Failed to get file locations")?;

        if let Some(locations) = locations {
            if locations.len() != 1 {
                bail!("Unexpected file location length: {}", locations.len());
            }

            if locations[0].url != zgs_node_urls[0] {
                bail!("Unexpected file location: {}", locations[0].url);
            }
        }
    }

    Ok(())
}

async fn wait_until<F, Fut>(f: F, timeout: Duration) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let start = std::time::Instant::now();
    loop {
        match f().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if start.elapsed() > timeout {
                    return Err(e).context("Operation timed out");
                }
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = wait_until(run_test, Duration::from_secs(180)).await {
        println!("Batch upload test failed: {}", err);
        std::process::exit(1);
    }
}