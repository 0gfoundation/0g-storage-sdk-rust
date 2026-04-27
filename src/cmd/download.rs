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
        help = "[v1 symmetric] Hex-encoded 32-byte AES key (64 hex chars, optional 0x prefix). For asymmetric (ECIES), use --decrypt --private-key instead. Mutually exclusive with --decrypt."
    )]
    pub encryption_key: Option<String>,

    #[arg(
        long,
        help = "[v2 asymmetric/ECIES] Decrypt ciphertext using --private-key as the recipient wallet private key. Mutually exclusive with --encryption-key (v1)."
    )]
    pub decrypt: bool,

    #[arg(long, help = "[v2 asymmetric/ECIES] Hex-encoded 32-byte secp256k1 recipient wallet private key. Used with --decrypt.")]
    pub private_key: Option<String>,
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

    if args.decrypt && args.encryption_key.is_some() {
        anyhow::bail!("--decrypt and --encryption-key are mutually exclusive");
    }
    let wallet_priv: Option<[u8; 32]> = if args.decrypt {
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

        let cfg = match (&enc_key, &wallet_priv) {
            (Some(k), None) => FragmentDecryptConfig::Symmetric { key: *k },
            (None, Some(priv_key)) => {
                FragmentDecryptConfig::Ecies { wallet_priv: *priv_key }
            }
            (None, None) => FragmentDecryptConfig::None,
            (Some(_), Some(_)) => unreachable!("guarded above"),
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
                "Decrypted (v1) file ({} -> {} bytes)",
                encrypted.len(),
                decrypted.len()
            );
        } else if let Some(priv_key) = wallet_priv {
            use crate::transfer::ecies::derive_ecies_decrypt_key;
            use crate::transfer::encryption::{crypt_at, EncryptionHeader};
            let encrypted = std::fs::read(&args.file).context("Failed to read downloaded file")?;
            let header = EncryptionHeader::parse(&encrypted)?;
            let eph = header
                .ephemeral_pub
                .ok_or_else(|| anyhow::anyhow!("downloaded file is not v2 ECIES ciphertext"))?;
            let aes_key = derive_ecies_decrypt_key(&priv_key, &eph)?;
            let header_size = header.size();
            let mut decrypted = encrypted[header_size..].to_vec();
            crypt_at(&aes_key, &header.nonce, 0, &mut decrypted);
            std::fs::write(&args.file, &decrypted).context("Failed to write decrypted file")?;
            log::info!(
                "Decrypted (v2/ECIES) file ({} -> {} bytes)",
                encrypted.len(),
                decrypted.len()
            );
        }
    }

    Ok(())
}
