use anyhow::{anyhow, Result};
use clap::Args;
use ethers::types::{H256, U256};
use std::time::Duration;

use crate::cmd::upload::{FinalityRequirement, UploadOption};
use crate::common::blockchain::rpc::must_new_web3;
use crate::common::utils::duration_from_str;
use crate::common::utils::pad_to_32_bytes;
use crate::indexer::client::IndexerClient;
use crate::kv::batcher::Batcher;
use crate::node::client_zgs::must_new_zgs_clients;
use crate::node::client_zgs::ZgsClient;

#[derive(Args)]
pub struct KvWriteArgs {
    #[arg(
        long,
        default_value = "0x00",
        help = "Stream to read/write",
        required = true
    )]
    pub stream_id: String,

    #[arg(long, help = "Private key to interact with smart contract")]
    pub key: String,

    #[arg(long, help = "KV keys", value_delimiter = ',', required = true)]
    pub stream_keys: Vec<String>,

    #[arg(long, help = "KV values", value_delimiter = ',', required = true)]
    pub stream_values: Vec<String>,

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

    #[arg(long, help = "Wait for file finality on nodes to upload")]
    pub finality_required: bool,

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

    #[arg(long, default_value = "0", value_parser = duration_from_str, help = "Cli task timeout, 0 for no timeout")]
    pub timeout: Duration,

    #[arg(long, default_value = "18446744073709551615", help = "Key version")]
    pub version: u64,

    #[arg(
        long,
        help = "Fullnode URL to interact with ZeroGStorage smart contract",
        required = true
    )]
    pub url: String,

    #[arg(long, help = "ZeroGStorage storage node URL")]
    pub node: Vec<String>,

    #[arg(long, help = "ZeroGStorage indexer URL")]
    pub indexer: Option<String>,
}

pub async fn run_kv_write(args: &KvWriteArgs) -> Result<()> {
    let fee = U256::from(args.fee * 1e18 as u64);
    let nonce = U256::from(args.nonce);

    let finality_required = if args.finality_required {
        FinalityRequirement::FileFinalized
    } else {
        FinalityRequirement::TransactionPacked
    };

    let mut opt = UploadOption {
        tags: vec![],
        finality_required,
        task_size: args.task_size,
        expected_replica: args.expected_replica,
        skip_tx: args.skip_tx,
        fee,
        nonce,
    };

    let web3_client = must_new_web3(&args.url, &args.key).await;

    let mut clients: Vec<ZgsClient> = vec![];
    if let Some(indexer_url) = &args.indexer {
        let indexer_client = IndexerClient::new(indexer_url).await?;
        clients = indexer_client
            .select_nodes(args.expected_replica as usize, &[])
            .await?;
    }

    if clients.is_empty() {
        if args.node.is_empty() {
            anyhow::bail!("At least one of --node and --indexer should not be empty");
        }
        clients = must_new_zgs_clients(&args.node).await;
    }

    let mut batcher = Batcher::new(args.version, clients, web3_client);

    if args.stream_keys.len() != args.stream_values.len() {
        return Err(anyhow!("keys and values length mismatch"));
    }

    if args.stream_keys.is_empty() {
        return Err(anyhow!("no keys to write"));
    }

    log::debug!("stream_id in args: {:?}", args.stream_id);
    let stream_id =
        H256::from_slice(pad_to_32_bytes(args.stream_id.trim_start_matches("0x"))?.as_slice());
    log::info!("stream_id: {:?}", stream_id);

    for (key, value) in args.stream_keys.iter().zip(args.stream_values.iter()) {
        batcher.set(stream_id, key.as_bytes(), value.as_bytes().to_vec());
    }

    batcher.exec(&mut opt).await?;

    Ok(())
}
