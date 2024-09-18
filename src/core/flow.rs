use ethers::types::{Bytes, H256, U256};
use anyhow::{Result, anyhow};
use log::debug;
use rayon::prelude::*;
use std::sync::Arc;
use super::dataflow::{IterableData, DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_MAX_CHUNKS, read_at, segment_root};
use super::merkle::tree_builder::TreeBuilder;
use crate::contract::flow::{Submission, SubmissionNode};

pub struct Flow {
    data: Arc<dyn IterableData>,
    tags: Vec<u8>,
}

impl Flow {
    pub fn new(data: Arc<dyn IterableData>, tags: Vec<u8>) -> Self {
        Flow { data, tags }
    }

    pub fn create_submission(&self) -> Result<Submission> {
        let mut submission = Submission {
            length: U256::from(self.data.size()),
            tags: Bytes::from(self.tags.clone()),
            nodes: Vec::new(),
        };

        let mut offset = 0;
        for chunks in self.split_nodes() {
            let node = self.create_node(offset, chunks)?;
            submission.nodes.push(node);
            offset += chunks * DEFAULT_CHUNK_SIZE as i64;
        }

        Ok(submission)
    }

    fn split_nodes(&self) -> Vec<i64> {
        let mut nodes = Vec::new();
        let chunks = self.data.num_chunks();
        let (padded_chunks, chunks_next_pow2) = compute_padded_size(chunks);
        let mut next_chunk_size = chunks_next_pow2;

        let mut padded_chunks = padded_chunks;
        while padded_chunks > 0 {
            if padded_chunks >= next_chunk_size {
                padded_chunks -= next_chunk_size;
                nodes.push(next_chunk_size as i64);
            }
            next_chunk_size /= 2;
        }

        debug!("SplitNodes: chunks={}, nodeSize={:?}", chunks, nodes);

        nodes
    }

    fn create_node(&self, offset: i64, chunks: i64) -> Result<SubmissionNode> {
        let batch = chunks.min(DEFAULT_SEGMENT_MAX_CHUNKS as i64);
        self.create_segment_node(offset, DEFAULT_CHUNK_SIZE as i64 * batch, DEFAULT_CHUNK_SIZE as i64 * chunks)
    }

    fn create_segment_node(&self, offset: i64, batch: i64, size: i64) -> Result<SubmissionNode> {
        let builder = Arc::new(std::sync::Mutex::new(TreeBuilder::new()));
        let data = self.data.as_ref();
    
        let num_segments = (size - 1) / batch + 1;
        (0..num_segments).into_par_iter().try_for_each(|i| {
            let segment_offset = offset + i * batch;
            let segment_size = batch.min(size - i * batch);
            let buf = read_at(data, segment_size as usize, segment_offset, data.padded_size())?;
            let hash = segment_root(&buf);
            builder.lock().unwrap().append_hash(hash);
            Ok::<_, anyhow::Error>(())
        })?;
    
        let tree = builder.lock().unwrap().build().ok_or_else(|| anyhow!("Failed to build tree"))?;
        let num_chunks = size / DEFAULT_CHUNK_SIZE as i64;
        let height = (num_chunks as f64).log2() as u64;
        let root = H256::from(tree.root());
        let root_bytes = root.as_bytes();
        let mut root_array = [0u8; 32];
        root_array.copy_from_slice(root_bytes);
    
        Ok(SubmissionNode {
            root: root_array,
            height: U256::from(height),
        })
    }
}

fn next_pow2(input: u64) -> u64 {
    let mut x = input;
    x -= 1;
    x |= x >> 32;
    x |= x >> 16;
    x |= x >> 8;
    x |= x >> 4;
    x |= x >> 2;
    x |= x >> 1;
    x += 1;
    x
}

pub fn compute_padded_size(chunks: u64) -> (u64, u64) {
    let chunks_next_pow2 = next_pow2(chunks);
    if chunks_next_pow2 == chunks {
        return (chunks_next_pow2, chunks_next_pow2);
    }

    let min_chunk = if chunks_next_pow2 >= 16 {
        chunks_next_pow2 / 16
    } else {
        1
    };

    let padded_chunks = ((chunks - 1) / min_chunk + 1) * min_chunk;
    (padded_chunks, chunks_next_pow2)
}