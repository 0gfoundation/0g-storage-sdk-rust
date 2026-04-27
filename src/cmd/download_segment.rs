use crate::common::utils::duration_from_str;
use crate::core::dataflow::DEFAULT_SEGMENT_SIZE;
use crate::indexer::client::IndexerClient;
use crate::node::client_zgs::must_new_zgs_clients;
use crate::transfer::downloader::Downloader;
use crate::transfer::encryption::{decrypt_segment, EncryptionHeader, ENCRYPTION_HEADER_SIZE};
use anyhow::{Context, Result};
use clap::Args;
use ethers::types::H256;
use std::str::FromStr;
use std::time::Duration;

#[derive(Args)]
pub struct DownloadSegmentArgs {
    #[arg(long, value_delimiter = ',', help = "ZeroGStorage storage node URL")]
    pub node: Vec<String>,

    #[arg(long, help = "ZeroGStorage indexer URL")]
    pub indexer: Option<String>,

    #[arg(long, help = "Merkle root of the file")]
    pub root: Option<String>,

    #[arg(long, help = "Transaction sequence number")]
    pub tx_seq: Option<u64>,

    #[arg(long, help = "Segment index to download (0-based)")]
    pub segment_index: u64,

    #[arg(
        long,
        num_args = 1,
        default_value = "true",
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

    #[arg(
        long,
        help = "Decrypt v2 (ECIES) ciphertext using --private-key as the recipient \
                wallet private key (mutually exclusive with --encryption-key)"
    )]
    pub decrypt: bool,

    #[arg(long, help = "Hex-encoded recipient secp256k1 wallet private key (used with --decrypt)")]
    pub private_key: Option<String>,
}

/// Parse and validate the encryption key from hex string.
fn parse_encryption_key(key_hex: &str) -> Result<[u8; 32]> {
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
    Ok(key)
}

pub async fn run_download_segment(args: &DownloadSegmentArgs) -> Result<()> {
    let root = args.root.as_ref().map(|r| H256::from_str(r)).transpose()?;
    let encryption_key = args
        .encryption_key
        .as_ref()
        .map(|k| parse_encryption_key(k))
        .transpose()?;

    let wallet_priv: Option<[u8; 32]> = if args.decrypt {
        if args.encryption_key.is_some() {
            anyhow::bail!("--decrypt and --encryption-key are mutually exclusive");
        }
        let pk_hex = args
            .private_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--decrypt requires --private-key"))?;
        let pk_hex = pk_hex.strip_prefix("0x").unwrap_or(pk_hex);
        let pk_bytes = hex::decode(pk_hex).context("Invalid private key hex")?;
        if pk_bytes.len() != 32 {
            anyhow::bail!("--private-key must be 32 bytes (64 hex chars)");
        }
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&pk_bytes);
        Some(buf)
    } else {
        None
    };

    if let Some(indexer_url) = &args.indexer {
        let indexer_client = IndexerClient::new(indexer_url).await?;
        let ctx = indexer_client
            .get_download_context(root, args.tx_seq)
            .await?;

        let segment = if encryption_key.is_some() || wallet_priv.is_some() {
            // For decryption, we always need segment 0 to get the header
            let seg0 = ctx.download_segment(0, args.proof).await?;
            if seg0.len() < ENCRYPTION_HEADER_SIZE {
                anyhow::bail!("Segment 0 too short for encryption header");
            }
            let header = EncryptionHeader::parse(&seg0)?;
            let aes_key = match header.version {
                crate::transfer::encryption::ENCRYPTION_VERSION_V1 => *encryption_key
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("v1 ciphertext but --encryption-key not provided"))?,
                crate::transfer::encryption::ENCRYPTION_VERSION_V2 => {
                    let priv_key = wallet_priv
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("v2 ciphertext but --private-key not provided"))?;
                    let eph = header
                        .ephemeral_pub
                        .ok_or_else(|| anyhow::anyhow!("v2 header missing ephemeral pubkey"))?;
                    crate::transfer::ecies::derive_ecies_decrypt_key(priv_key, &eph)?
                }
                v => anyhow::bail!("Unsupported encryption version: {}", v),
            };

            if args.segment_index == 0 {
                decrypt_segment(&aes_key, 0, DEFAULT_SEGMENT_SIZE as u64, &seg0, &header)
            } else {
                let seg = ctx.download_segment(args.segment_index, args.proof).await?;
                decrypt_segment(
                    &aes_key,
                    args.segment_index,
                    DEFAULT_SEGMENT_SIZE as u64,
                    &seg,
                    &header,
                )
            }
        } else {
            ctx.download_segment(args.segment_index, args.proof).await?
        };

        log::info!(
            "Downloaded segment {} ({} bytes), file root: {:?}",
            args.segment_index,
            segment.len(),
            ctx.file_root()
        );

        println!("{}", hex::encode(&segment));
    } else {
        let clients = must_new_zgs_clients(&args.node).await;
        let routines = args.routines.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });
        let downloader = Downloader::new(clients, routines)?;

        let file_info = if let Some(root) = root {
            downloader.query_file(root).await?
        } else if let Some(tx_seq) = args.tx_seq {
            downloader.query_file_by_tx_seq(tx_seq).await?
        } else {
            anyhow::bail!("Must specify either root or tx_seq");
        };

        let file_root = file_info.tx.data_merkle_root;

        let segment = if encryption_key.is_some() || wallet_priv.is_some() {
            // For decryption, we always need segment 0 to get the header
            let seg0 = downloader
                .download_segment(file_root, file_info.tx.seq, 0, args.proof, &file_info)
                .await?;
            if seg0.len() < ENCRYPTION_HEADER_SIZE {
                anyhow::bail!("Segment 0 too short for encryption header");
            }
            let header = EncryptionHeader::parse(&seg0)?;
            let aes_key = match header.version {
                crate::transfer::encryption::ENCRYPTION_VERSION_V1 => *encryption_key
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("v1 ciphertext but --encryption-key not provided"))?,
                crate::transfer::encryption::ENCRYPTION_VERSION_V2 => {
                    let priv_key = wallet_priv
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("v2 ciphertext but --private-key not provided"))?;
                    let eph = header
                        .ephemeral_pub
                        .ok_or_else(|| anyhow::anyhow!("v2 header missing ephemeral pubkey"))?;
                    crate::transfer::ecies::derive_ecies_decrypt_key(priv_key, &eph)?
                }
                v => anyhow::bail!("Unsupported encryption version: {}", v),
            };

            if args.segment_index == 0 {
                decrypt_segment(&aes_key, 0, DEFAULT_SEGMENT_SIZE as u64, &seg0, &header)
            } else {
                let seg = downloader
                    .download_segment(
                        file_root,
                        file_info.tx.seq,
                        args.segment_index,
                        args.proof,
                        &file_info,
                    )
                    .await?;
                decrypt_segment(
                    &aes_key,
                    args.segment_index,
                    DEFAULT_SEGMENT_SIZE as u64,
                    &seg,
                    &header,
                )
            }
        } else {
            downloader
                .download_segment(
                    file_root,
                    file_info.tx.seq,
                    args.segment_index,
                    args.proof,
                    &file_info,
                )
                .await?
        };

        log::info!(
            "Downloaded segment {} ({} bytes), file root: {:?}",
            args.segment_index,
            segment.len(),
            file_root
        );

        println!("{}", hex::encode(&segment));
    }

    Ok(())
}
