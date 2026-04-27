use super::download::download_file::DownloadingFile;
use crate::transfer::ecies::derive_ecies_decrypt_key;
use super::encryption::{
    crypt_at, decrypt_fragment_data, decrypt_segment, EncryptionHeader, ENCRYPTION_VERSION_V1,
    ENCRYPTION_VERSION_V2,
};
use crate::common::shard::ShardConfig;
use crate::core::dataflow::{
    merkle_tree, num_splits, padded_segment_root, DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_MAX_CHUNKS,
    DEFAULT_SEGMENT_SIZE,
};
use crate::core::file::File;
use crate::node::client_zgs::ZgsClient;
use crate::node::types::FileInfo;

use anyhow::{Context, Result};
use ethers::types::H256;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct Downloader {
    pub clients: Vec<ZgsClient>,
    routines: usize,
}

struct DownloadSegmentConfig<'a> {
    clients: &'a [ZgsClient],
    shard_configs: &'a [ShardConfig],
    task: u64,
    routine: usize,
    offset: u64,
    start_segment_index: u64,
    end_segment_index: u64,
    num_chunks: u64,
    tx_seq: u64,
    with_proof: bool,
    root: H256,
    file_size: usize,
    strip_padding: bool,
}

impl Downloader {
    pub fn new(clients: Vec<ZgsClient>, routines: usize) -> Result<Self> {
        if clients.is_empty() {
            anyhow::bail!("Storage node not specified");
        }

        Ok(Self { clients, routines })
    }

    /// Create a no-op downloader for unit tests that only exercise pure logic
    /// (e.g. `resolve_decryption_key`) without touching the network.
    #[cfg(test)]
    pub(crate) fn new_test_only() -> Self {
        Self {
            clients: vec![],
            routines: 1,
        }
    }

    pub async fn download(&self, root: H256, file: &PathBuf, with_proof: bool) -> Result<()> {
        // Query file info from storage node
        let info = self
            .query_file(root)
            .await
            .context("Failed to query file info")?;

        // Check file existence before downloading
        self.check_existence(root, file)
            .await
            .context("Failed to check file existence")?;

        // Download segments
        self.download_file(file, root, info.tx.size as usize, with_proof, &info)
            .await
            .context("Failed to download file")?;

        // Validate the downloaded file
        self.validate_download_file(root, file, info.tx.size)
            .await
            .context("Failed to validate downloaded file")?;

        Ok(())
    }

    /// Download a list of fragment files (each identified by its root hash) and
    /// concatenate them into a single output file.
    ///
    /// This mirrors the Go SDK's `DownloadFragments` behaviour: each root hash
    /// corresponds to one fragment of a large file that was split before upload.
    /// The fragments are downloaded sequentially and appended in order.
    ///
    /// When `encryption_key` is provided the fragments are treated as parts of a
    /// continuous AES-256-CTR encrypted stream: the 17-byte header is extracted
    /// from fragment 0 and the CTR byte-offset is tracked across all fragments so
    /// the plaintext is reconstructed correctly.
    pub async fn download_fragments(
        &self,
        roots: Vec<H256>,
        file: &PathBuf,
        with_proof: bool,
        encryption_key: Option<&[u8; 32]>,
    ) -> Result<()> {
        if roots.is_empty() {
            anyhow::bail!("No root hashes provided");
        }

        let mut out = std::fs::File::create(file)
            .with_context(|| format!("Failed to create output file {:?}", file))?;

        let mut header: Option<EncryptionHeader> = None;
        let mut data_offset: u64 = 0;

        for (i, root) in roots.iter().enumerate() {
            // Use a temp path next to the target file
            let temp_path = file.with_extension(format!("part.{}", i));

            self.download(*root, &temp_path, with_proof)
                .await
                .with_context(|| format!("Failed to download fragment {}", i))?;

            let fragment_data = std::fs::read(&temp_path)
                .with_context(|| format!("Failed to read fragment {}", i))?;
            let _ = std::fs::remove_file(&temp_path);

            if let Some(key) = encryption_key {
                // Parse header from the very first fragment
                if i == 0 {
                    header = Some(
                        EncryptionHeader::parse(&fragment_data)
                            .context("Failed to parse encryption header from fragment 0")?,
                    );
                }
                let hdr = header.as_ref().unwrap();
                let (plaintext, new_offset) =
                    decrypt_fragment_data(key, hdr, &fragment_data, i == 0, data_offset)
                        .with_context(|| format!("Failed to decrypt fragment {}", i))?;
                data_offset = new_offset;
                out.write_all(&plaintext)
                    .with_context(|| format!("Failed to write fragment {}", i))?;
            } else {
                out.write_all(&fragment_data)
                    .with_context(|| format!("Failed to write fragment {}", i))?;
            }

            log::info!("Downloaded and appended fragment {}/{}", i + 1, roots.len());
        }

        log::info!(
            "Completed assembling {} fragments into {:?}",
            roots.len(),
            file
        );
        Ok(())
    }

    /// Download a specific segment by transaction sequence
    ///
    /// # Arguments
    /// * `root` - The root hash of the file
    /// * `tx_seq` - The transaction sequence number
    /// * `segment_index` - File-relative segment index (0-based)
    /// * `with_proof` - Whether to download with merkle proof verification
    /// * `file_info` - File information containing size, start_entry_index, etc.
    ///
    /// # Returns
    /// The segment data as a vector of bytes
    pub async fn download_segment(
        &self,
        root: H256,
        tx_seq: u64,
        segment_index: u64,
        with_proof: bool,
        file_info: &FileInfo,
    ) -> Result<Vec<u8>> {
        let shard_configs = get_shard_configs(&self.clients);

        let start_segment_index =
            file_info.tx.start_entry_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let end_segment_index = (file_info.tx.start_entry_index
            + num_splits(file_info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64
            - 1)
            / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let num_segments = end_segment_index - start_segment_index + 1;
        let num_chunks = num_splits(file_info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64;

        // Validate file-relative segment_index is within range
        if segment_index >= num_segments {
            anyhow::bail!(
                "Segment index {} out of range [0, {})",
                segment_index,
                num_segments
            );
        }

        let segment = SegmentDownloader::download_segment_static(DownloadSegmentConfig {
            clients: &self.clients,
            shard_configs: &shard_configs,
            task: segment_index,
            routine: 0,
            offset: 0,
            start_segment_index,
            end_segment_index,
            num_chunks,
            tx_seq,
            with_proof,
            root,
            file_size: file_info.tx.size as usize,
            strip_padding: true,
        })
        .await?;

        Ok(segment)
    }

    /// Like `download_segment` but returns the full padded data without stripping
    /// padding from the last segment. Useful for entry-based storage (e.g. KV flow store).
    pub async fn download_segment_raw(
        &self,
        root: H256,
        tx_seq: u64,
        segment_index: u64,
        with_proof: bool,
        file_info: &FileInfo,
    ) -> Result<Vec<u8>> {
        let shard_configs = get_shard_configs(&self.clients);

        let start_segment_index =
            file_info.tx.start_entry_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let end_segment_index = (file_info.tx.start_entry_index
            + num_splits(file_info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64
            - 1)
            / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let num_segments = end_segment_index - start_segment_index + 1;
        let num_chunks = num_splits(file_info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64;

        if segment_index >= num_segments {
            anyhow::bail!(
                "Segment index {} out of range [0, {})",
                segment_index,
                num_segments
            );
        }

        let segment = SegmentDownloader::download_segment_static(DownloadSegmentConfig {
            clients: &self.clients,
            shard_configs: &shard_configs,
            task: segment_index,
            routine: 0,
            offset: 0,
            start_segment_index,
            end_segment_index,
            num_chunks,
            tx_seq,
            with_proof,
            root,
            file_size: file_info.tx.size as usize,
            strip_padding: false,
        })
        .await?;

        Ok(segment)
    }

    pub async fn query_file(&self, root: H256) -> Result<FileInfo> {
        for client in &self.clients {
            match client.get_file_info(root).await {
                Ok(Some(info)) => {
                    return Ok(info);
                }
                Ok(None) => {
                    log::error!("File[{:?}] not found on node[{}]", root, client.url);
                    continue;
                }
                Err(e) => {
                    log::error!("Failed to get file info from node[{}]: {}", client.url, e);
                    continue;
                }
            }
        }
        anyhow::bail!("File[{:?}] not found on any node", root)
    }

    pub async fn query_file_by_tx_seq(&self, tx_seq: u64) -> Result<FileInfo> {
        for client in &self.clients {
            match client.get_file_info_by_tx_seq(tx_seq).await {
                Ok(Some(info)) => {
                    return Ok(info);
                }
                Ok(None) => {
                    log::error!("File[tx_seq={}] not found on node[{}]", tx_seq, client.url);
                    continue;
                }
                Err(e) => {
                    log::error!("Failed to get file info from node[{}]: {}", client.url, e);
                    continue;
                }
            }
        }
        anyhow::bail!("File[tx_seq={}] not found on any node", tx_seq)
    }

    async fn check_existence(&self, root: H256, file: &PathBuf) -> Result<()> {
        if !file.exists() {
            return Ok(());
        }
        let data = Arc::new(File::open(file)?);

        let tree = Arc::new(merkle_tree(data.clone()).await?);

        if tree.root() == root {
            anyhow::bail!("File already exists");
        } else {
            anyhow::bail!("File already exists with different hash");
        }
    }

    async fn download_file(
        &self,
        file: &Path,
        root: H256,
        size: usize,
        with_proof: bool,
        info: &FileInfo,
    ) -> Result<()> {
        let mut file = DownloadingFile::create(file, root, size)?;
        log::info!(
            "Begin to download file from {} storage nodes",
            self.clients.len()
        );

        let shard_configs = get_shard_configs(&self.clients);

        let mut sd = SegmentDownloader::new(
            &self.clients,
            shard_configs,
            &mut file,
            with_proof,
            info,
            self.routines,
        )?;

        sd.download().await?;

        file.seal()?;

        log::info!("Completed to download file");
        Ok(())
    }

    async fn validate_download_file(
        &self,
        root: H256,
        file: &PathBuf,
        file_size: u64,
    ) -> Result<()> {
        if file.metadata()?.len() != file_size {
            anyhow::bail!(
                "File size mismatch: expected = {}, downloaded = {}",
                file_size,
                file.metadata()?.len()
            );
        }

        let data = Arc::new(File::open(file)?);

        let tree = Arc::new(merkle_tree(data.clone()).await?);

        if tree.root() != root {
            anyhow::bail!("Merkle root mismatch, downloaded = {:?}", tree.root());
        }

        log::info!("Succeeded to validate the downloaded file");

        Ok(())
    }
}

struct SegmentDownloader<'a> {
    clients: &'a Vec<ZgsClient>,
    shard_configs: Vec<ShardConfig>,
    file: &'a mut DownloadingFile,
    tx_seq: u64,
    start_segment_index: u64,
    end_segment_index: u64,
    with_proof: bool,
    offset: u64,
    num_chunks: u64,
    routines: usize,
}

impl<'a> SegmentDownloader<'a> {
    fn new(
        clients: &'a Vec<ZgsClient>,
        shard_configs: Vec<ShardConfig>,
        file: &'a mut DownloadingFile,
        with_proof: bool,
        info: &FileInfo,
        routines: usize,
    ) -> Result<Self> {
        let start_segment_index = info.tx.start_entry_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let end_segment_index = (info.tx.start_entry_index
            + num_splits(info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64
            - 1)
            / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let offset = (file.metadata().offset / DEFAULT_SEGMENT_SIZE) as u64;

        log::info!(
            "Start downloading file: size={}, startEntryIndex={}, numChunks={}, startSegmentIndex={}, endSegmentIndex={}",
            info.tx.size,
            info.tx.start_entry_index,
            num_splits(info.tx.size as usize, DEFAULT_CHUNK_SIZE),
            start_segment_index,
            end_segment_index
        );

        Ok(Self {
            clients,
            shard_configs,
            file,
            tx_seq: info.tx.seq,
            start_segment_index,
            end_segment_index,
            with_proof,
            offset,
            num_chunks: num_splits(info.tx.size as usize, DEFAULT_CHUNK_SIZE) as u64,
            routines,
        })
    }

    async fn download(&mut self) -> Result<()> {
        let num_tasks = self.end_segment_index - self.start_segment_index + 1 - self.offset;

        // Clone data needed for parallel download
        let clients = self.clients.clone();
        let shard_configs = self.shard_configs.clone();
        let tx_seq = self.tx_seq;
        let start_segment_index = self.start_segment_index;
        let end_segment_index = self.end_segment_index;
        let offset = self.offset;
        let num_chunks = self.num_chunks;
        let with_proof = self.with_proof;
        let root = self.file.metadata().root;
        let file_size = self.file.metadata().size;
        let routines = self.routines;

        // Download segments with controlled concurrency
        let mut results: Vec<(u64, Vec<u8>)> = stream::iter(0..num_tasks)
            .map(|task| {
                let clients = clients.clone();
                let shard_configs = shard_configs.clone();
                let routine = (task % routines as u64) as usize;

                async move {
                    let segment = Self::download_segment_static(DownloadSegmentConfig {
                        clients: &clients,
                        shard_configs: &shard_configs,
                        task,
                        routine,
                        offset,
                        start_segment_index,
                        end_segment_index,
                        num_chunks,
                        tx_seq,
                        with_proof,
                        root,
                        file_size,
                        strip_padding: true,
                    })
                    .await?;
                    Ok::<(u64, Vec<u8>), anyhow::Error>((task, segment))
                }
            })
            .buffer_unordered(routines)
            .try_collect()
            .await?;

        // Sort results by task index to maintain order
        results.sort_by_key(|(task, _)| *task);

        // Write segments in order
        for (_, segment) in results {
            self.file.write(&segment)?;
        }

        Ok(())
    }

    async fn download_segment_static(config: DownloadSegmentConfig<'_>) -> Result<Vec<u8>> {
        let segment_index = config.offset + config.task;
        // there is no not-aligned & segment-crossed file
        let start_index = segment_index * DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        let mut end_index = start_index + DEFAULT_SEGMENT_MAX_CHUNKS as u64;
        if end_index > config.num_chunks {
            end_index = config.num_chunks;
        }

        // Round-robin node selection starting from routine index
        for i in 0..config.clients.len() {
            let node_index = (config.routine + i) % config.clients.len();
            let shard_config = &config.shard_configs[node_index];

            if (config.start_segment_index + segment_index) % shard_config.num_shard
                != shard_config.shard_id
            {
                continue;
            }

            let client = &config.clients[node_index];

            let segment = if config.with_proof {
                Self::download_with_proof_static(
                    client,
                    config.root,
                    start_index,
                    end_index,
                    config.tx_seq,
                    config.file_size,
                )
                .await?
            } else if let Some(data) = client
                .download_segment_by_tx_seq(config.tx_seq, start_index, end_index)
                .await?
            {
                Some(data.0)
            } else {
                None
            };

            if let Some(mut segment) = segment {
                if segment.len() % DEFAULT_CHUNK_SIZE != 0 {
                    log::warn!(
                        "Invalid segment length from node {}, segment {}/({}-{}), chunks [{}, {})",
                        node_index,
                        config.start_segment_index + segment_index,
                        config.start_segment_index,
                        config.end_segment_index,
                        start_index,
                        end_index
                    );
                    continue;
                }

                // Remove paddings for the last chunk
                if config.strip_padding
                    && config.start_segment_index + segment_index == config.end_segment_index
                {
                    let last_chunk_size = config.file_size % DEFAULT_CHUNK_SIZE;
                    if last_chunk_size > 0 {
                        let paddings = DEFAULT_CHUNK_SIZE - last_chunk_size;
                        segment.truncate(segment.len() - paddings);
                    }
                }

                log::debug!(
                    "Succeeded to download segment from node {}, segment {}/({}-{}), chunks [{}, {})",
                    node_index,
                    config.start_segment_index + segment_index,
                    config.start_segment_index,
                    config.end_segment_index,
                    start_index,
                    end_index
                );
                return Ok(segment);
            } else {
                log::warn!(
                    "Segment not found on node {}, segment {}/({}-{}), chunks [{}, {})",
                    node_index,
                    config.start_segment_index + segment_index,
                    config.start_segment_index,
                    config.end_segment_index,
                    start_index,
                    end_index
                );
            }
        }

        anyhow::bail!("Failed to download segment {}", segment_index)
    }

    async fn download_with_proof_static(
        client: &ZgsClient,
        root: H256,
        start_index: u64,
        end_index: u64,
        tx_seq: u64,
        file_size: usize,
    ) -> Result<Option<Vec<u8>>> {
        let segment_index = start_index / DEFAULT_SEGMENT_MAX_CHUNKS as u64;

        let segment = client
            .download_segment_with_proof_by_tx_seq(tx_seq, segment_index)
            .await?;

        if let Some(segment) = segment {
            let expected_data_len = (end_index - start_index) * DEFAULT_CHUNK_SIZE as u64;
            if expected_data_len as usize != segment.data.len() {
                anyhow::bail!(
                    "Downloaded data length mismatch, expected = {}, actual = {}",
                    expected_data_len,
                    segment.data.len()
                );
            }

            let (segment_root, num_segments_flow_padded) =
                padded_segment_root(segment_index, &segment.data, file_size as u64);

            segment
                .proof
                .validate_hash(root, segment_root, segment_index, num_segments_flow_padded)
                .context("Failed to validate proof")?;

            Ok(Some(segment.data))
        } else {
            Ok(None)
        }
    }
}

pub struct DownloadContext {
    downloader: Downloader,
    file_info: FileInfo,
    file_root: H256,
    /// v1: 32-byte symmetric AES key supplied out-of-band.
    encryption_key: Option<[u8; 32]>,
    /// v2: 32-byte secp256k1 wallet private key. Used to derive the AES key
    /// from the ephemeral pubkey carried in the v2 header.
    wallet_private_key: Option<[u8; 32]>,
    /// Header + AES key, jointly cached after the first segment-0 download.
    /// Stored as a single Option so concurrent segment downloads cannot
    /// observe a torn state where one is set without the other.
    cached: Mutex<Option<(EncryptionHeader, [u8; 32])>>,
}

impl DownloadContext {
    pub fn new(
        clients: Vec<ZgsClient>,
        routines: usize,
        file_info: FileInfo,
        file_root: H256,
    ) -> Result<Self> {
        let downloader = Downloader::new(clients, routines)?;
        Ok(Self {
            downloader,
            file_info,
            file_root,
            encryption_key: None,
            wallet_private_key: None,
            cached: Mutex::new(None),
        })
    }

    /// Enable decryption with the given AES-256 key.
    /// Segment 0 must be downloaded first to extract the encryption header.
    pub fn with_encryption(mut self, key: [u8; 32]) -> Self {
        self.encryption_key = Some(key);
        self
    }

    /// Enable decryption for v2 (ECIES) ciphertext using the recipient's
    /// 32-byte secp256k1 wallet private key. The AES key is derived after
    /// segment 0 is downloaded and the v2 header is parsed.
    /// If both `with_encryption` and `with_wallet_private_key` are configured,
    /// the on-wire header version determines which key is used.
    pub fn with_wallet_private_key(mut self, priv_key: [u8; 32]) -> Self {
        self.wallet_private_key = Some(priv_key);
        self
    }

    /// Download a segment, stripping padding from the last segment.
    /// If encryption is enabled, the data is decrypted and the header is stripped from segment 0.
    pub async fn download_segment(&self, segment_index: u64, with_proof: bool) -> Result<Vec<u8>> {
        let raw = self
            .downloader
            .download_segment(
                self.file_root,
                self.file_info.tx.seq,
                segment_index,
                with_proof,
                &self.file_info,
            )
            .await?;

        let encryption_active =
            self.encryption_key.is_some() || self.wallet_private_key.is_some();
        if encryption_active {
            let (header, key) = self.resolve_decryption_key(segment_index, &raw)?;
            Ok(decrypt_segment(
                &key,
                segment_index,
                DEFAULT_SEGMENT_SIZE as u64,
                &raw,
                &header,
            ))
        } else {
            Ok(raw)
        }
    }

    /// Download a segment without stripping padding. If encryption is enabled,
    /// data is decrypted but the encryption header is preserved in segment 0
    /// (i.e. segment 0 returns `[header][decrypted_plaintext]`).
    /// This is useful for entry-based storage where byte layout must match on-chain tx size.
    pub async fn download_segment_padded(
        &self,
        segment_index: u64,
        with_proof: bool,
    ) -> Result<Vec<u8>> {
        let raw = self
            .downloader
            .download_segment_raw(
                self.file_root,
                self.file_info.tx.seq,
                segment_index,
                with_proof,
                &self.file_info,
            )
            .await?;

        let encryption_active =
            self.encryption_key.is_some() || self.wallet_private_key.is_some();
        if encryption_active {
            let (header, key) = self.resolve_decryption_key(segment_index, &raw)?;
            if segment_index == 0 {
                let mut result = raw;
                let header_size = header.size();
                crypt_at(&key, &header.nonce, 0, &mut result[header_size..]);
                Ok(result)
            } else {
                Ok(decrypt_segment(
                    &key,
                    segment_index,
                    DEFAULT_SEGMENT_SIZE as u64,
                    &raw,
                    &header,
                ))
            }
        } else {
            Ok(raw)
        }
    }

    /// Parse the encryption header from segment 0 (caching it), then return
    /// the (header, AES key) pair to use for decrypting this and subsequent
    /// segments.  Dispatches on header version: v1 returns the configured
    /// symmetric key, v2 derives an AES key from the wallet private key +
    /// ephemeral pubkey.  The lock is held for the entire parse+derive+cache
    /// write so concurrent callers cannot observe torn state.
    pub(crate) fn resolve_decryption_key(
        &self,
        segment_index: u64,
        segment_data: &[u8],
    ) -> Result<(EncryptionHeader, [u8; 32])> {
        let mut guard = self.cached.lock().unwrap();
        if let Some(cached) = guard.as_ref() {
            return Ok(cached.clone());
        }
        if segment_index != 0 {
            anyhow::bail!("Encryption header not yet available; download segment 0 first");
        }
        let header = EncryptionHeader::parse(segment_data)?;
        let key = match header.version {
            ENCRYPTION_VERSION_V1 => *self
                .encryption_key
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!(
                    "v1 ciphertext but no symmetric encryption_key configured"
                ))?,
            ENCRYPTION_VERSION_V2 => {
                let priv_key = self.wallet_private_key.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("v2 ciphertext but no wallet_private_key configured")
                })?;
                let eph = header.ephemeral_pub.ok_or_else(|| {
                    anyhow::anyhow!("v2 header missing ephemeral pubkey")
                })?;
                derive_ecies_decrypt_key(priv_key, &eph)?
            }
            other => anyhow::bail!("Unsupported encryption version: {}", other),
        };
        *guard = Some((header.clone(), key));
        Ok((header, key))
    }

    /// Returns the parsed encryption header, if segment 0 has been downloaded.
    pub fn encryption_header(&self) -> Option<EncryptionHeader> {
        self.cached.lock().unwrap().as_ref().map(|(h, _)| h.clone())
    }

    pub fn file_root(&self) -> H256 {
        self.file_root
    }

    pub fn file_info(&self) -> &FileInfo {
        &self.file_info
    }

    #[cfg(test)]
    pub(crate) fn test_only_with_wallet_priv(priv_key: [u8; 32]) -> Self {
        // No-network context for unit-testing dispatch logic.
        // The downloader/file_info/file_root fields are constructed with sentinel
        // values; the test only exercises resolve_decryption_key which doesn't touch them.
        use crate::node::types::Transaction;
        use ethers::types::U256;
        Self {
            downloader: Downloader::new_test_only(),
            file_info: FileInfo {
                tx: Transaction {
                    stream_ids: vec![],
                    data: vec![],
                    data_merkle_root: H256::zero(),
                    start_entry_index: 0,
                    size: 0,
                    seq: 0,
                },
                finalized: false,
                is_cached: false,
                uploaded_seg_num: 0,
            },
            file_root: H256::zero(),
            encryption_key: None,
            wallet_private_key: Some(priv_key),
            cached: Mutex::new(None),
        }
    }
}

pub fn get_shard_configs(clients: &[ZgsClient]) -> Vec<ShardConfig> {
    clients.iter().map(|c| c.shard_config().clone()).collect()
}

#[cfg(test)]
mod ecies_dispatch_tests {
    use super::*;
    use crate::transfer::ecies::derive_ecies_encrypt_key;
    use crate::transfer::encryption::{
        crypt_at, EncryptionHeader, ENCRYPTION_HEADER_SIZE_V2,
    };
    use k256::SecretKey;
    use rand::rngs::OsRng;

    /// Verify the version-dispatch path of `DownloadContext` itself by exercising
    /// the header-cache helper directly (no network).
    #[test]
    fn resolves_v2_key_from_wallet_private_key() {
        let recipient_priv = SecretKey::random(&mut OsRng);
        let recipient_pub = recipient_priv.public_key().to_sec1_bytes();
        let (aes_key, eph) = derive_ecies_encrypt_key(&recipient_pub).unwrap();
        let header = EncryptionHeader::new_ecies(eph);

        let plaintext = b"v2 ctx test payload".to_vec();
        let mut encrypted = plaintext.clone();
        crypt_at(&aes_key, &header.nonce, 0, &mut encrypted);
        let mut seg0 = header.to_bytes();
        seg0.extend_from_slice(&encrypted);

        let mut priv_bytes = [0u8; 32];
        priv_bytes.copy_from_slice(&recipient_priv.to_bytes());
        let ctx = DownloadContext::test_only_with_wallet_priv(priv_bytes);

        let resolved = ctx
            .resolve_decryption_key(0, &seg0)
            .expect("ctx must derive aes key from segment 0");
        assert_eq!(resolved.1, aes_key);

        let cached_header = ctx.encryption_header().unwrap();
        assert_eq!(cached_header.version, 2);
        assert_eq!(cached_header.size(), ENCRYPTION_HEADER_SIZE_V2);
    }
}
