use super::dataflow::{
    async_read_at, read_at, segment_root, IterableData, DEFAULT_CHUNK_SIZE,
    DEFAULT_SEGMENT_MAX_CHUNKS,
};
use super::merkle::tree_builder::TreeBuilder;
use crate::contract::flow::{Submission, SubmissionNode};
use anyhow::{anyhow, Result};
use ethers::types::{Bytes, H256, U256};
use futures::future::{join_all, try_join_all};
use log::debug;
use std::borrow::Borrow;
use std::sync::Arc;

pub struct Flow {
    data: Arc<dyn IterableData>,
    tags: Vec<u8>,
}

impl Flow {
    pub fn new(data: Arc<dyn IterableData>, tags: Vec<u8>) -> Self {
        Flow { data, tags }
    }

    pub async fn create_submission(&self) -> Result<Submission> {
        let mut submission = Submission {
            length: U256::from(self.data.size()),
            tags: Bytes::from(self.tags.clone()),
            nodes: Vec::new(),
        };
        log::debug!("submission: {:?}", submission);
        let mut offset = 0;
        for chunks in self.split_nodes() {
            debug!("chunks: {:?}", chunks);
            let node = self.create_node(offset, chunks).await?;
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

    async fn create_node(&self, offset: i64, chunks: i64) -> Result<SubmissionNode> {
        let batch = chunks.min(DEFAULT_SEGMENT_MAX_CHUNKS as i64);
        self.create_segment_node(
            offset,
            DEFAULT_CHUNK_SIZE as i64 * batch,
            DEFAULT_CHUNK_SIZE as i64 * chunks,
        )
        .await
    }

    pub async fn create_segment_node(
        &self,
        offset: i64,
        batch: i64,
        size: i64,
    ) -> Result<SubmissionNode> {
        let num_segments = (size - 1) / batch + 1;

        let tasks: Vec<_> = (0..num_segments)
            .map(|i| {
                let data = Arc::clone(&self.data);
                async move {
                    let segment_offset = offset + i * batch;
                    let segment_size = batch.min(size - i * batch);

                    log::debug!(
                        "Processing segment {}/{}: offset={}, size={}",
                        i + 1,
                        num_segments,
                        segment_offset,
                        segment_size
                    );

                    let buf = async_read_at(
                        data.clone(),
                        segment_size as usize,
                        segment_offset,
                        data.padded_size(),
                    )
                    .await?;

                    let hash = segment_root(&buf);
                    Ok::<_, anyhow::Error>(hash)
                }
            })
            .collect();

        // Use join_all to concurrently process all segments
        let hashes = try_join_all(tasks).await?;

        // Build the Merkle tree
        let mut builder = TreeBuilder::new();
        for hash in hashes {
            builder.append_hash(hash);
        }

        let tree = builder
            .build()
            .ok_or_else(|| anyhow!("Failed to build tree"))?;

        let num_chunks = size / DEFAULT_CHUNK_SIZE as i64;
        let height = (num_chunks as f64).log2() as u64;
        let mut root_bytes = [0u8; 32];
        root_bytes.copy_from_slice(tree.root().as_bytes());

        Ok(SubmissionNode {
            root: root_bytes,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::file::File;
    use std::path::Path;

    #[test]
    fn test_new_flow() {
        let upload_file = Path::new("tmp123456");
        let data = File::open(upload_file).unwrap();
        let tag = "test_file".as_bytes().to_vec();
        let flow = Flow::new(Arc::new(data), tag);
        println!("Flow: {:?}", String::from_utf8(flow.tags));
    }

    #[tokio::test]
    async fn test_create_submission() {
        let upload_file = Path::new("tmp123456");
        let data = File::open(upload_file).unwrap();
        let tag = "test_file".as_bytes().to_vec();
        let flow = Flow::new(Arc::new(data), tag);

        let submission = flow.create_submission().await.unwrap();
        print!("Submission: {:?}", submission);
    }
}
