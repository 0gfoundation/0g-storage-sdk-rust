# 0g-storage-sdk-rust

Rust CLI and SDK for interacting with the [0G Storage](https://0g.ai) network.

## Build

```bash
cargo build --release
```

The binary is at `target/release/zg-storage-client`.

## CLI

All commands share these global flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--log-level` | `info` | Log verbosity (`debug`, `info`, `warn`, `error`) |
| `--rpc-retry-count` | `5` | RPC retry attempts |
| `--rpc-retry-interval` | `5s` | Delay between retries |
| `--rpc-timeout` | `30s` | Timeout per RPC call |
| `--gas-limit` | — | Override transaction gas limit |
| `--gas-price` | — | Override transaction gas price |

### upload

Upload a file to the 0G Storage network.

```bash
zg-storage-client upload \
  --url    <blockchain-rpc>   \   # e.g. http://localhost:8545
  --key    <private-key-hex>  \
  --node   <node-url>[,...]   \   # storage node(s), or use --indexer
  --file   <path>

# Via indexer (auto-selects nodes)
zg-storage-client upload --url ... --key ... --indexer <indexer-url> --file <path>
```

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--node` | — | Comma-separated storage node URLs |
| `--indexer` | — | Indexer URL (alternative to `--node`) |
| `--expected-replica` | `1` | Required number of replicas |
| `--skip-tx` | `true` | Skip on-chain TX if file already exists |
| `--finality-required` | `false` | Wait for full finalization before returning |
| `--task-size` | `10` | Segments per RPC upload batch |
| `--fee` | `0` | Upload fee in A0GI |
| `--fragment-size` | — | Split into fragments of this size (bytes) for large files; returns comma-separated roots |
| `--encryption-key` | — | 64-char hex AES-256 key; file is encrypted before upload |
| `--flow-address` | — | Override Flow contract address |
| `--market-address` | — | Override Market contract address |
| `--timeout` | `0` | CLI task timeout (`0` = no timeout) |

On success, prints `root = 0x<merkle-root>` (single file) or `roots = <r1>,<r2>,...` (fragmented).

---

### download

Download a file from the 0G Storage network.

```bash
# Single file
zg-storage-client download \
  --node  <node-url>[,...] \
  --root  <merkle-root>    \
  --file  <output-path>

# Fragmented large file (multiple roots, concatenated in order)
zg-storage-client download \
  --node  <node-url>[,...] \
  --roots <root1>,<root2>,<root3>,... \
  --file  <output-path>

# Via indexer
zg-storage-client download --indexer <indexer-url> --root <root> --file <path>
```

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--root` | — | Single Merkle root (mutually exclusive with `--roots`) |
| `--roots` | — | Comma-separated Merkle roots for a fragmented file |
| `--node` | — | Comma-separated storage node URLs |
| `--indexer` | — | Indexer URL (alternative to `--node`) |
| `--proof` | `false` | Verify Merkle proofs during download |
| `--routines` | CPU count | Parallel download goroutines |
| `--encryption-key` | — | 64-char hex AES-256 key; decrypts after download |
| `--timeout` | — | CLI task timeout |

When `--roots` is used the fragments are downloaded sequentially and concatenated. For encrypted fragmented files the AES-256-CTR keystream is tracked continuously across all fragments so the original plaintext is reconstructed correctly.

---

### download-segment

Download a single segment (256 KB chunk) by index.

```bash
zg-storage-client download-segment \
  --node           <node-url>[,...] \
  --root           <merkle-root>    \   # or --tx-seq <n>
  --segment-index  <n>              \
  --proof          true

# Output: hex-encoded segment bytes printed to stdout
```

| Flag | Default | Description |
|------|---------|-------------|
| `--root` | — | File Merkle root |
| `--tx-seq` | — | Transaction sequence number (alternative to `--root`) |
| `--segment-index` | — | 0-based segment index |
| `--proof` | `true` | Verify Merkle proof |
| `--encryption-key` | — | Decrypt the segment after download |

---

### indexer

Start a local indexer service that tracks storage nodes and serves file location queries.

```bash
zg-storage-client indexer \
  --endpoint <port>             \
  --trusted  <node-url>[,...]   \
  --node     <discovery-node>
```

---

### kv-write / kv-read

Write and read key-value data stored in the 0G KV stream.

```bash
zg-storage-client kv-write \
  --url          <blockchain-rpc> \
  --key          <private-key>    \
  --node         <node-url>       \
  --stream-id    <stream-id>      \
  --stream-keys  <k1,k2,...>      \
  --stream-values <v1,v2,...>

zg-storage-client kv-read \
  --node        <node-url>  \
  --stream-id   <stream-id> \
  --stream-keys <k1,k2,...>
```

---

## SDK

The library exposes the core types for embedding in other Rust programs.

### Uploading a file

```rust
use std::sync::Arc;
use zg_storage_client::{
    core::file::File,
    node::client_zgs::must_new_zgs_clients,
    transfer::uploader::Uploader,
    cmd::upload::{UploadOption, FinalityRequirement},
};

let clients = must_new_zgs_clients(&["http://node:5678".to_string()]).await;
let uploader = Uploader::new_with_addresses(web3_client, clients, None, None).await?;
let file = Arc::new(File::open(&path)?);
let opt = UploadOption::default();
uploader.upload(file, &opt).await?;
```

### Downloading a file

```rust
use zg_storage_client::transfer::downloader::Downloader;

let downloader = Downloader::new(clients, num_routines)?;

// Single file
downloader.download(root, &output_path, with_proof).await?;

// Fragmented large file — downloads each fragment and concatenates
downloader.download_fragments(roots, &output_path, with_proof, None).await?;

// Fragmented + encrypted
downloader.download_fragments(roots, &output_path, with_proof, Some(&key)).await?;
```

### Downloading via indexer

```rust
use zg_storage_client::indexer::client::IndexerClient;

let client = IndexerClient::new("http://indexer:12345").await?;

// Single file
client.download(root, &output_path, with_proof).await?;

// Fragmented large file
client.download_fragments(roots, &output_path, with_proof, None).await?;
```

### Segment-level access

For streaming or random-access workloads use `DownloadContext` to avoid repeated indexer queries:

```rust
let ctx = client.get_download_context(Some(root), None).await?;
let seg0 = ctx.download_segment(0, with_proof).await?;   // 256 KB
let seg1 = ctx.download_segment(1, with_proof).await?;
```

`DownloadContext::with_encryption(key)` enables transparent per-segment decryption.

### Encryption

Files are encrypted with AES-256-CTR before upload. The 17-byte header
(`version || nonce[16]`) is prepended to the ciphertext and stored as part of
the file on-chain.

```rust
use zg_storage_client::transfer::encryption::{decrypt_file, decrypt_fragment_data, EncryptionHeader};

// Decrypt a single downloaded file
let plaintext = decrypt_file(&key, &encrypted_bytes)?;

// Decrypt individual fragments of a large encrypted file
let header = EncryptionHeader::parse(&fragment0_bytes)?;
let (plain0, offset) = decrypt_fragment_data(&key, &header, &fragment0_bytes, true,  0)?;
let (plain1, _)      = decrypt_fragment_data(&key, &header, &fragment1_bytes, false, offset)?;
```

---

## Large file support

Files larger than the fragment size (default **4 GiB**, aligned to the next
power of two) are split before upload. Each fragment becomes an independent
0G Storage file with its own Merkle root.

**Upload** — pass `--fragment-size <bytes>` to the CLI. The command prints the
comma-separated list of roots:

```
roots = 0xabc...,0xdef...,0x123...
```

**Download** — pass `--roots <root1>,<root2>,...` (comma-separated, same order
as upload). The SDK downloads each fragment in order and concatenates them into
the output file. For encrypted files the AES-256-CTR stream is re-joined
correctly across fragment boundaries.

---

## Testing

### Unit tests

```bash
cargo test
```

### Integration tests

The Python integration tests require a running 0G blockchain node and storage
nodes. See `tests/Makefile` for setup details.

```bash
cd tests
python test_all.py --cli ../target/release/zg-storage-client
```

Key test files:

| File | What it tests |
|------|---------------|
| `cli_file_upload_download_test.py` | Single-file upload/download |
| `cli_file_encrypted_upload_download_test.py` | Encrypted single-file upload/download across many sizes |
| `splitable_upload_test.py` | Plain fragmented upload/download (50 MB, 4 MB fragments) |
| `cli_encrypted_splitable_upload_download_test.py` | Encrypted fragmented upload/download with boundary-condition fragment counts |
| `cli_download_segment_test.py` | Segment-level download |
| `indexer_test.py` | Indexer node selection and upload/download via indexer |
| `batch_upload_test.py` | Batch (multi-file) upload |
