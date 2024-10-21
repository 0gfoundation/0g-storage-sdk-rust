use super::iterator::Iterator as CustomIterator;
use super::merkle::tree::Tree;
use super::merkle::tree_builder::TreeBuilder;
use anyhow::{anyhow, Context, Result};
use ethers::types::H256;
use futures::future::{join_all, try_join_all};
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
    log::debug!("data_size_origin: {:?}", data.size());
    log::debug!("num_segments_padded: {:?}", num_segments);
    log::debug!("data_size_padded: {:?}", data.padded_size());

    let segment_tasks = (0..num_segments).map(|i| {
        let data = Arc::clone(&data);
        async move {
            let offset = i as i64 * DEFAULT_SEGMENT_SIZE as i64;
            let remaining_size = data.padded_size().saturating_sub(offset as u64) as usize;
            let segment_size = std::cmp::min(DEFAULT_SEGMENT_SIZE, remaining_size) as usize;
            
            let buf = async_read_at(data.clone(), segment_size, offset, data.padded_size())
                .await
                .context("context")?;

            let hash = segment_root(&buf);

            log::debug!(
                "Processing segment {}: offset = {}, size = {}, root = {:?}",
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
    (total - 1) / unit + 1
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

pub async fn async_read_at(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::file::File;
    use std::path::Path;

    #[tokio::test]
    async fn test_file_merkle_tree() {
        let data = File::open(Path::new("tmp123456")).unwrap();
        let tree = merkle_tree(Arc::new(data)).await.unwrap();
        log::info!("file root: {:?}", tree.root());
    }
}
