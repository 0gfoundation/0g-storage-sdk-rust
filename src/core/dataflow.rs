use super::iterator::Iterator as CustomIterator;
use super::merkle::tree::Tree;
use super::merkle::tree_builder::TreeBuilder;
use anyhow::{anyhow, Result};
use ethers::types::H256;
use futures::future::join_all;
use lazy_static::lazy_static;
use std::sync::Arc;
use tiny_keccak::{Hasher, Keccak};

pub const DEFAULT_CHUNK_SIZE: usize = 256;
pub const DEFAULT_SEGMENT_MAX_CHUNKS: usize = 1024;
pub const DEFAULT_SEGMENT_SIZE: usize = DEFAULT_CHUNK_SIZE * DEFAULT_SEGMENT_MAX_CHUNKS;

lazy_static! {
    static ref EMPTY_CHUNK: [u8; DEFAULT_CHUNK_SIZE] = [0; DEFAULT_CHUNK_SIZE];
    static ref EMPTY_CHUNK_HASH: H256 = {
        let mut hasher = Keccak::v256();
        hasher.update(&EMPTY_CHUNK[..]);
        let mut hash = [0u8; 32];
        hasher.finalize(&mut hash);
        H256::from(hash)
    };
}

pub trait IterableData: Send + Sync {
    fn num_chunks(&self) -> u64;
    fn num_segments(&self) -> u64;
    fn size(&self) -> i64;
    fn padded_size(&self) -> u64;
    fn iterate(&self, offset: i64, batch: i64, flow_padding: bool) -> Box<dyn CustomIterator + '_>;
    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize>;
}

pub async fn merkle_tree(data: Arc<dyn IterableData>) -> Result<Tree> {
    let num_segments = num_segments_padded(data.as_ref());

    let tasks: Vec<_> = (0..num_segments)
        .map(|i| {
            let data = Arc::clone(&data);
            tokio::spawn(async move {
                let offset = i as i64 * DEFAULT_SEGMENT_SIZE as i64;
                let result = read_at(
                    data.as_ref(),
                    DEFAULT_SEGMENT_SIZE,
                    offset,
                    data.padded_size(),
                )
                .map(|buf| segment_root(&buf))?;
                Ok::<_, anyhow::Error>((i, result))
            })
        })
        .collect();

    // Wait for all tasks to complete and collect results
    let mut results: Vec<(usize, H256)> = join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    // Sort results by segment index
    results.sort_by_key(|(i, _)| *i);

    // Build the Merkle tree
    let mut builder = TreeBuilder::new();
    for (_, hash) in results {
        builder.append_hash(hash);
    }

    builder
        .build()
        .ok_or_else(|| anyhow!("Failed to build tree"))
}

pub fn num_splits(total: i64, unit: usize) -> u64 {
    ((total - 1) / unit as i64 + 1) as u64
}

pub fn num_segments_padded(data: &dyn IterableData) -> usize {
    ((data.padded_size() - 1) / DEFAULT_SEGMENT_SIZE as u64 + 1) as usize
}

pub fn segment_root(chunks: &[u8]) -> H256 {
    let mut builder = TreeBuilder::new();

    for chunk in chunks.chunks(DEFAULT_CHUNK_SIZE) {
        if chunk.len() == DEFAULT_CHUNK_SIZE {
            builder.append(chunk);
        } else {
            let mut padded_chunk = [0u8; DEFAULT_CHUNK_SIZE];
            padded_chunk[..chunk.len()].copy_from_slice(chunk);
            builder.append(&padded_chunk);
        }
    }

    builder
        .build()
        .map(|tree| tree.root())
        .unwrap_or(*EMPTY_CHUNK_HASH)
}

pub fn read_at(
    data: &dyn IterableData,
    read_size: usize,
    offset: i64,
    padded_size: u64,
) -> Result<Vec<u8>> {
    if offset < 0 || offset as u64 >= padded_size {
        return Err(anyhow!("Invalid offset: {}", offset));
    }

    let max_available_length = padded_size.saturating_sub(offset as u64);
    let expected_buf_size = std::cmp::min(max_available_length as usize, read_size);

    if offset >= data.size() {
        return Ok(vec![0; expected_buf_size]);
    }

    let mut buf = vec![0; expected_buf_size];
    match data.read(&mut buf, offset) {
        Ok(_) => Ok(buf),
        Err(e) => {
            if buf.is_empty() {
                Ok(vec![]) // Return empty vector if no data could be read
            } else {
                Err(anyhow!("Error reading data: {}", e))
            }
        }
    }
}

pub fn async_read_at(
    data: Arc<dyn IterableData>,
    read_size: usize,
    offset: i64,
    padded_size: u64,
) -> Result<Vec<u8>> {
    if offset < 0 || offset as u64 >= padded_size {
        return Err(anyhow!("Invalid offset: {}", offset));
    }

    let max_available_length = padded_size.saturating_sub(offset as u64);
    let expected_buf_size = std::cmp::min(max_available_length as usize, read_size);

    if offset >= data.size() {
        return Ok(vec![0; expected_buf_size]);
    }

    let mut buf = vec![0; expected_buf_size];
    match data.read(&mut buf, offset) {
        Ok(_) => Ok(buf),
        Err(e) => {
            if buf.is_empty() {
                Ok(vec![]) // Return empty vector if no data could be read
            } else {
                Err(anyhow!("Error reading data: {}", e))
            }
        }
    }
}