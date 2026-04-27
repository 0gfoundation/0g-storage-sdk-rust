use crate::common::utils::duration_from_str;
use crate::indexer::client::IndexerClient;
use crate::node::client_zgs::must_new_zgs_clients;
use crate::transfer::downloader::{Downloader, FragmentDecryptConfig};
use crate::transfer::encryption::decrypt_file;
use anyhow::{Context, Result};
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

    #[arg(long, help = "Merkle root to download a single file")]
    pub root: Option<String>,

    #[arg(
        long,
        value_delimiter = ',',
        help = "Comma-separated list of Merkle roots for a fragmented large file (downloaded in order and concatenated)"
    )]
    pub roots: Vec<String>,

    #[arg(
        long,
        num_args = 1,
        default_value = "false",
        help = "Whether to download with merkle proof for validation"
    )]
    pub proof: bool,

    #[arg(long, help = "Number of routines for downloading simultaneously")]
    pub routines: Option<usize>,

    #[arg(long, value_parser = duration_from_str, help = "cli task timeout, 0 for no timeout")]
    pub timeout: Option<Duration>,

    #[arg(
        long,
        help = "Hex-encoded AES-256 encryption key (64 hex chars, optional 0x prefix)"
    )]
    pub encryption_key: Option<String>,
}

pub async fn run_download(args: &DownloadArgs) -> Result<()> {
    // Resolve encryption key once (used for both single and fragment downloads)
    let enc_key: Option<[u8; 32]> = if let Some(key_hex) = &args.encryption_key {
        let key_hex = key_hex.strip_prefix("0x").unwrap_or(key_hex);
        let key_bytes = hex::decode(key_hex).context("Invalid encryption key hex")?;
        if key_bytes.len() != 32 {
            anyhow::bail!(
                "Encryption key must be 32 bytes (64 hex chars), got {} bytes",
                key_bytes.len()
            );
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&key_bytes);
        Some(key)
    } else {
        None
    };

    // Determine mode: single root vs. list of roots
    let roots_provided = !args.roots.is_empty();
    let root_provided = args.root.is_some();

    if root_provided && roots_provided {
        anyhow::bail!("Specify either --root or --roots, not both");
    }
    if !root_provided && !roots_provided {
        anyhow::bail!("Specify either --root (single file) or --roots (fragmented file)");
    }

    let routines = args.routines.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });

    if roots_provided {
        // --- Fragmented download ---
        let roots: Result<Vec<H256>> = args
            .roots
            .iter()
            .map(|s| {
                H256::from_str(s).map_err(|e| anyhow::anyhow!("Invalid root hash {:?}: {}", s, e))
            })
            .collect();
        let roots = roots?;

        let cfg = match &enc_key {
            Some(k) => FragmentDecryptConfig::Symmetric { key: *k },
            None => FragmentDecryptConfig::None,
        };

        if let Some(indexer_url) = &args.indexer {
            let indexer_client = IndexerClient::new(indexer_url).await?;
            indexer_client
                .download_fragments(roots, &args.file, args.proof, cfg)
                .await?;
        } else {
            let clients = must_new_zgs_clients(&args.node).await;
            let downloader = Downloader::new(clients, routines)?;
            downloader
                .download_fragments(roots, &args.file, args.proof, cfg)
                .await?;
        }
    } else {
        // --- Single file download ---
        let root = H256::from_str(args.root.as_ref().unwrap())?;

        if let Some(indexer_url) = &args.indexer {
            let indexer_client = IndexerClient::new(indexer_url).await?;
            indexer_client
                .download(root, &args.file, args.proof)
                .await?;
        } else {
            let clients = must_new_zgs_clients(&args.node).await;
            let downloader = Downloader::new(clients, routines)?;
            downloader.download(root, &args.file, args.proof).await?;
        }

        // Decrypt single file if key provided
        if let Some(key) = enc_key {
            let encrypted = std::fs::read(&args.file).context("Failed to read downloaded file")?;
            let decrypted = decrypt_file(&key, &encrypted)?;
            std::fs::write(&args.file, &decrypted).context("Failed to write decrypted file")?;
            log::info!(
                "Decrypted file ({} -> {} bytes)",
                encrypted.len(),
                decrypted.len()
            );
        }
    }

    Ok(())
}
