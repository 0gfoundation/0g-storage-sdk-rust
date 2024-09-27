use crate::common::blockchain::rpc::must_new_web3;
use crate::common::options::LogOption;
use crate::common::utils::duration_from_str;
use crate::core::file::File;
use crate::indexer::client::{IndexerClient, IndexerClientOption};
use crate::node::client_zgs::{must_new_zgs_clients, ZgsClient};
use crate::transfer::uploader::Uploader;
use anyhow::Result;
use clap::Args;
use ethers::types::{H256, U256};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq)]
pub enum FinalityRequirement {
    FileFinalized = 0,     // 等待文件完成
    TransactionPacked = 1, // 等待交易回执, 不等待文件完成
    WaitNothing = 2,       // 不等待任何操作
}

#[derive(Clone, Default)]
pub struct UploadOption {
    pub tags: Vec<u8>,
    pub finality_required: FinalityRequirement,
    pub task_size: u32,
    pub expected_replica: u32,
    pub skip_tx: bool,
    pub fee: Option<U256>,
    pub nonce: Option<U256>,
}

impl Default for FinalityRequirement {
    fn default() -> Self {
        FinalityRequirement::TransactionPacked
    }
}

#[derive(Args)]
pub struct UploadArgs {
    #[arg(long, help = "File name to upload")]
    pub file: PathBuf,

    #[arg(long, default_value = "0x", help = "Tags of the file")]
    pub tags: String,

    #[arg(
        long,
        help = "Fullnode URL to interact with ZeroGStorage smart contract"
    )]
    pub url: String,

    #[arg(long, help = "Private key to interact with smart contract")]
    pub key: String,

    #[arg(long, help = "ZeroGStorage storage node URL")]
    pub node: Vec<String>,

    #[arg(long, help = "ZeroGStorage indexer URL")]
    pub indexer: Option<String>,

    #[arg(
        long,
        default_value = "1",
        help = "expected number of replications to upload"
    )]
    pub expected_replica: u32,

    #[arg(
        long,
        default_value = "true",
        help = "Skip sending the transaction on chain if already exists"
    )]
    pub skip_tx: bool,

    #[arg(long, help = "Wait for file finality on nodes to upload")]
    pub finality_required: bool,

    #[arg(
        long,
        default_value = "10",
        help = "Number of segments to upload in single rpc request"
    )]
    pub task_size: u32,

    #[arg(long, help = "fee paid in a0gi")]
    pub fee: Option<f64>,

    #[arg(long, help = "nonce of upload transaction")]
    pub nonce: Option<u64>,

    #[arg(long, value_parser = duration_from_str, help = "cli task timeout, 0 for no timeout")]
    pub timeout: Option<Duration>,
}

pub async fn run_upload(args: &UploadArgs) -> Result<()> {
    let file = Arc::new(File::open(&args.file)?);

    let fee = args.fee.map(|f| U256::from((f * 1e18) as u64));
    let nonce = args.nonce.map(U256::from);

    let finality_required = if args.finality_required {
        FinalityRequirement::FileFinalized
    } else {
        FinalityRequirement::TransactionPacked
    };

    let opt = UploadOption {
        tags: hex::decode(&args.tags[2..])?,
        finality_required,
        task_size: args.task_size,
        expected_replica: args.expected_replica,
        skip_tx: args.skip_tx,
        fee,
        nonce,
    };

    let web3_client = must_new_web3(&args.url, &args.key).await;

    if let Some(indexer_url) = &args.indexer {
        let indexer_client = IndexerClient::new(indexer_url, &IndexerClientOption {})?;
        indexer_client.upload(file, &opt).await?;
    } else {
        let client = must_new_zgs_clients(&args.node);
        let uploader = Uploader::new(web3_client, client, &LogOption::default()).await?;
        uploader.upload(file, &opt).await?;
    }
    Ok(())
}
