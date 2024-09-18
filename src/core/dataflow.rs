use super::merkle::tree_builder::TreeBuilder;
use super::merkle::tree::Tree;
use ethers::types::H256;
use tiny_keccak::{Hasher, Keccak};
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::join_all;
use anyhow::{Result, anyhow};
use lazy_static::lazy_static;
use super::iterator::Iterator as CustomIterator;

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
    let builder = Arc::new(Mutex::new(TreeBuilder::new()));
    let tasks = (0..num_segments_padded(data.as_ref())).map(|i| {
        let data = Arc::clone(&data);
        let builder = Arc::clone(&builder);
        tokio::spawn(async move {
            let offset = i as i64 * DEFAULT_SEGMENT_SIZE as i64;
            let buf = read_at(data.as_ref(), DEFAULT_SEGMENT_SIZE, offset, data.padded_size())?;
            let hash = segment_root(&buf);
            builder.lock().await.append_hash(hash);
            Ok::<_, anyhow::Error>(())
        })
    });

    join_all(tasks).await.into_iter().collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("Task error: {}", e))?;

    let tree = builder.lock().await.build().ok_or_else(|| anyhow!("Failed to build tree"))?;
    Ok(tree)
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

    builder.build().map(|tree| tree.root()).unwrap_or(*EMPTY_CHUNK_HASH)
}

pub fn read_at(data: &dyn IterableData, read_size: usize, offset: i64, padded_size: u64) -> Result<Vec<u8>> {
    if offset < 0 || offset as u64 >= padded_size {
        return Err(anyhow!("invalid offset"));
    }

    let max_available_length = padded_size - offset as u64;
    let expected_buf_size = std::cmp::min(max_available_length as usize, read_size);

    if offset >= data.size() {
        return Ok(vec![0; expected_buf_size]);
    }

    let mut buf = vec![0; expected_buf_size];
    data.read(&mut buf, offset)?;

    Ok(buf)
}