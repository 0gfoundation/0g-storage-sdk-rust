use super::merkle::tree::Tree;
use super::merkle::tree_builder::TreeBuilder;
use super::{flow::compute_padded_size, iterator::Iterator as CustomIterator};
use anyhow::{anyhow, Context, Result};
use ethers::types::H256;
use futures::future::try_join_all;
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
    log::debug!(
        "data_size_origin: {}, num_segments_padded: {}, data_size_padded: {}",
        data.size(),
        num_segments,
        data.padded_size()
    );

    let segment_tasks = (0..num_segments).map(|i| {
        let data = Arc::clone(&data);
        async move {
            let offset = i as i64 * DEFAULT_SEGMENT_SIZE as i64;
            let remaining_size = data.padded_size().saturating_sub(offset as u64) as usize;
            let segment_size = std::cmp::min(DEFAULT_SEGMENT_SIZE, remaining_size);

            let buf = async_read_at(data.clone(), segment_size, offset, data.padded_size())
                .await
                .context("Failed to read segment data")?;

            let hash = segment_root(&buf, 0);

            log::debug!(
                "Processed segment {}: offset = {}, size = {}, root = {:?}",
                i,
                offset,
                segment_size,
                hash
            );

            Ok::<H256, anyhow::Error>(hash)
        }
    });

    let hashes = try_join_all(segment_tasks)
        .await
        .context("Failed to process one or more segments")?;

    let mut builder = TreeBuilder::new();
    for hash in hashes {
        builder.append_hash(hash);
    }

    builder
        .build()
        .ok_or_else(|| anyhow!("Failed to build tree"))
}

pub fn num_splits(total: usize, unit: usize) -> usize {
    total.div_ceil(unit)
}

pub fn num_segments_padded(data: &dyn IterableData) -> usize {
    num_splits(data.padded_size() as usize, DEFAULT_SEGMENT_SIZE)
}

pub fn segment_root(chunks: &[u8], empty_chunks_padded: u64) -> H256 {
    let mut builder = TreeBuilder::new();

    for offset in (0..chunks.len()).step_by(DEFAULT_CHUNK_SIZE) {
        let end = (offset + DEFAULT_CHUNK_SIZE).min(chunks.len());
        let chunk = &chunks[offset..end];
        builder.append(chunk);
    }

    for _ in 0..empty_chunks_padded {
        builder.append_hash(*EMPTY_CHUNK_HASH);
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
    data.read(&mut buf, offset)
        .map(|_| buf.clone())
        .or_else(|e| {
            if buf.is_empty() {
                Ok(vec![])
            } else {
                Err(anyhow!("Error reading data: {}", e))
            }
        })
}

pub async fn async_read_at(
    data: Arc<dyn IterableData>,
    read_size: usize,
    offset: i64,
    padded_size: u64,
) -> Result<Vec<u8>> {
    if offset < 0 || offset as u64 >= padded_size {
        return Err(anyhow!(
            "Invalid offset: {}, padded size: {}",
            offset,
            padded_size
        ));
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

pub fn segment_range(start_chunk_index: usize, file_size: usize) -> (usize, usize) {
    let total_chunks = num_splits(file_size, DEFAULT_CHUNK_SIZE);
    let start_segment_index = start_chunk_index / DEFAULT_SEGMENT_MAX_CHUNKS;

    let end_chunk_index = start_chunk_index + total_chunks - 1;
    let end_segment_index = end_chunk_index / DEFAULT_SEGMENT_MAX_CHUNKS;

    (start_segment_index, end_segment_index)
}

pub fn padded_segment_root(segment_index: u64, chunks: &[u8], file_size: u64) -> (H256, u64) {
    let num_chunks = num_splits(file_size as usize, DEFAULT_CHUNK_SIZE) as u64;
    let (num_chunks_flow_padded, _) = compute_padded_size(num_chunks);
    let num_segments_flow_padded =
        (num_chunks_flow_padded - 1) / DEFAULT_SEGMENT_MAX_CHUNKS as u64 + 1;

    let start_index = segment_index * DEFAULT_SEGMENT_MAX_CHUNKS as u64;
    let end_index = std::cmp::min(start_index + DEFAULT_SEGMENT_MAX_CHUNKS as u64, num_chunks);

    let mut empty_chunks_padded = 0;

    let num_seg_chunks = end_index - start_index;
    if num_seg_chunks < DEFAULT_SEGMENT_MAX_CHUNKS as u64 {
        if segment_index < num_segments_flow_padded - 1
            || num_chunks_flow_padded % DEFAULT_SEGMENT_MAX_CHUNKS as u64 == 0
        {
            empty_chunks_padded = DEFAULT_SEGMENT_MAX_CHUNKS as u64 - num_seg_chunks;
        } else {
            let last_segment_chunks = num_chunks_flow_padded % DEFAULT_SEGMENT_MAX_CHUNKS as u64;
            if num_seg_chunks < last_segment_chunks {
                empty_chunks_padded = last_segment_chunks - num_seg_chunks;
            }
        }
    }

    (
        segment_root(chunks, empty_chunks_padded),
        num_segments_flow_padded,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::in_mem::DataInMemory;

    #[tokio::test]
    async fn test_file_merkle_tree() {
        let data = DataInMemory::new(b"test data for merkle tree".to_vec()).unwrap();
        let tree = merkle_tree(Arc::new(data)).await.unwrap();
        log::info!("File root: {:?}", tree.root());
    }
}
