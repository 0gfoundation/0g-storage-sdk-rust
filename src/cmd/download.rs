use crate::common::utils::duration_from_str;
use crate::indexer::client::IndexerClient;
use crate::node::client_zgs::must_new_zgs_clients;
use crate::transfer::downloader::Downloader;
use anyhow::Result;
use clap::Args;
use ethers::types::H256;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Args)]
pub struct DownloadArgs {
    #[arg(long, help = "File name to download")]
    pub file: PathBuf,

    #[arg(long, value_delimiter = ',', help = "ZeroGStorage storage node URL")]
    pub node: Vec<String>,

    #[arg(long, help = "ZeroGStorage indexer URL")]
    pub indexer: Option<String>,

    #[arg(long, help = "Merkle root to download file")]
    pub root: String,

    #[arg(long, help = "Whether to download with merkle proof for validation")]
    pub proof: bool,

    #[arg(long, help = "Number of routines for downloading simultaneously")]
    pub routines: Option<usize>,

    #[arg(long, value_parser = duration_from_str, help = "cli task timeout, 0 for no timeout")]
    pub timeout: Option<Duration>,
}

pub async fn run_download(args: &DownloadArgs) -> Result<()> {
    let root = H256::from_str(&args.root)?;

    if let Some(indexer_url) = &args.indexer {
        let indexer_client = IndexerClient::new(indexer_url).await?;
        indexer_client
            .download(root, &args.file, args.proof)
            .await?;
    } else {
        let clients = must_new_zgs_clients(&args.node).await;
        let mut downloader = Downloader::new(clients)?;
        if let Some(routines) = args.routines {
            downloader = downloader.with_routines(routines);
        }
        downloader.download(root, &args.file, args.proof).await?;
    }

    Ok(())
}
