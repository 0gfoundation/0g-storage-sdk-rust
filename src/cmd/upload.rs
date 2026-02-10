use crate::common::blockchain::rpc::must_new_web3;
use crate::common::utils::duration_from_str;
use crate::core::dataflow::merkle_tree;
use crate::core::file::File;
use crate::indexer::client::IndexerClient;
use crate::node::client_zgs::must_new_zgs_clients;
use crate::transfer::uploader::Uploader;
use anyhow::{Context, Result};
use clap::Args;
use ethers::types::{Address, U256};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Default)]
pub enum FinalityRequirement {
    FileFinalized = 0, // wait for file finalized
    #[default]
    TransactionPacked = 1, // wait for transaction packed
    WaitNothing = 2,   // wait nothing
}

#[derive(Clone, Default, Debug)]
pub struct UploadOption {
    pub tags: Vec<u8>,
    pub finality_required: FinalityRequirement,
    pub task_size: u64,
    pub expected_replica: u64,
    pub skip_tx: bool,
    pub fee: U256,
    pub nonce: U256,
}

#[derive(Debug, Clone, Default)]
pub struct BatchUploadOption {
    pub fee: U256,
    pub nonce: U256,
    pub data_options: Vec<UploadOption>,
    pub task_size: u64,
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

    #[arg(long, value_delimiter = ',', help = "ZeroGStorage storage node URL")]
    pub node: Vec<String>,

    #[arg(long, help = "ZeroGStorage indexer URL")]
    pub indexer: Option<String>,

    #[arg(
        long,
        default_value = "1",
        help = "expected number of replications to upload"
    )]
    pub expected_replica: u64,

    #[arg(
        long,
        num_args = 1,
        default_value = "true",
        help = "Skip sending the transaction on chain if already exists"
    )]
    pub skip_tx: bool,

    #[arg(
        long,
        num_args = 1,
        default_value = "false",
        help = "Wait for file finality on nodes to upload"
    )]
    pub finality_required: bool,

    #[arg(
        long,
        help = "Fragment size for splitting file (not implemented yet, placeholder for compatibility)"
    )]
    pub fragment_size: Option<u64>,

    #[arg(
        long,
        default_value = "10",
        help = "Number of segments to upload in single rpc request"
    )]
    pub task_size: u64,

    #[arg(long, default_value = "0", help = "fee paid in a0gi")]
    pub fee: u64,

    #[arg(long, default_value = "0", help = "nonce of upload transaction")]
    pub nonce: u64,

    #[arg(long, default_value = "0", value_parser = duration_from_str, help = "cli task timeout, 0 for no timeout")]
    pub timeout: Duration,

    #[arg(long, help = "Flow contract address (skips RPC call if provided)")]
    pub flow_address: Option<String>,

    #[arg(long, help = "Market contract address (requires flow-address)")]
    pub market_address: Option<String>,
}

pub async fn run_upload(args: &UploadArgs) -> Result<()> {
    // Validate contract address arguments (Go logic)
    if args.market_address.is_some() && args.flow_address.is_none() {
        anyhow::bail!("--market-address requires --flow-address to be provided");
    }

    let file = Arc::new(File::open(&args.file)?);
    let tree = merkle_tree(file.clone()).await?;
    let root = tree.root();

    let fee = U256::from(args.fee * 1e18 as u64);
    let nonce = U256::from(args.nonce);

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

    // Parse contract addresses (Go logic)
    let flow_address = args
        .flow_address
        .as_ref()
        .map(|s| Address::from_str(s))
        .transpose()
        .with_context(|| "Invalid flow address format")?;

    let market_address = args
        .market_address
        .as_ref()
        .map(|s| Address::from_str(s))
        .transpose()
        .with_context(|| "Invalid market address format")?;

    if let Some(indexer_url) = &args.indexer {
        let indexer_client = IndexerClient::new(indexer_url).await?;
        indexer_client
            .upload(web3_client, file, &opt, flow_address, market_address)
            .await?;
    } else {
        let clients = must_new_zgs_clients(&args.node).await;
        let uploader =
            Uploader::new_with_addresses(web3_client, clients, flow_address, market_address)
                .await?;
        uploader.upload(file, &opt).await?;
    }

    println!("root = 0x{}", hex::encode(root));
    Ok(())
}
