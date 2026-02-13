use super::dataflow::{
    async_read_at, segment_root, IterableData, DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_MAX_CHUNKS,
};
use super::merkle::tree_builder::TreeBuilder;
use crate::contracts::flow::{Submission, SubmissionData, SubmissionNode};
use anyhow::{anyhow, Result};
use ethers::types::{Address, Bytes, U256};
use futures::future::try_join_all;
use log::{debug, trace};
use std::sync::Arc;

pub struct Flow {
    data: Arc<dyn IterableData>,
    tags: Vec<u8>,
}

impl Flow {
    pub fn new(data: Arc<dyn IterableData>, tags: Vec<u8>) -> Self {
        Flow { data, tags }
    }

    pub async fn create_submission(&self, submitter: Address) -> Result<Submission> {
        let submission = Submission {
            data: SubmissionData {
                length: U256::from(self.data.size()),
                tags: Bytes::from(self.tags.clone()),
                nodes: Vec::new(),
            },
            submitter,
        };
        debug!("Creating submission: {:?}", submission);

        let mut offset = 0;
        let nodes = try_join_all(
            self.split_nodes()
                .into_iter()
                .enumerate()
                .map(|(i, chunks)| {
                    log::debug!("{}th node contain {} chunks", i, chunks);
                    let node = self.create_node(offset, chunks);
                    offset += DEFAULT_CHUNK_SIZE as i64 * chunks;
                    node
                }),
        )
        .await?;

        Ok(Submission {
            data: SubmissionData {
                nodes,
                ..submission.data
            },
            ..submission
        })
    }

    fn split_nodes(&self) -> Vec<i64> {
        let chunks = self.data.num_chunks();
        let (padded_chunks, chunks_next_pow2) = compute_padded_size(chunks);
        let mut nodes = Vec::new();
        let mut remaining_chunks = padded_chunks;
        let mut next_chunk_size = chunks_next_pow2;

        while remaining_chunks > 0 {
            if remaining_chunks >= next_chunk_size {
                remaining_chunks -= next_chunk_size;
                nodes.push(next_chunk_size as i64);
            }
            next_chunk_size /= 2;
        }

        log::info!("Split nodes: chunks={}, node_sizes={:?}", chunks, nodes);
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

                    trace!(
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

                    let hash = segment_root(&buf, 0);
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

        Ok(SubmissionNode {
            root: *tree.root().as_fixed_bytes(),
            height: U256::from(height),
        })
    }
}

fn next_pow2(input: u64) -> u64 {
    1u64.checked_shl(64 - input.leading_zeros()).unwrap_or(0)
}

pub fn compute_padded_size(chunks: u64) -> (u64, u64) {
    let chunks_next_pow2 = next_pow2(chunks);
    if chunks_next_pow2 == chunks {
        return (chunks_next_pow2, chunks_next_pow2);
    }

    let min_chunk = chunks_next_pow2.max(16) / 16;
    let padded_chunks = ((chunks - 1) / min_chunk + 1) * min_chunk;
    (padded_chunks, chunks_next_pow2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::in_mem::DataInMemory;

    #[test]
    fn test_new_flow() {
        let data = DataInMemory::new(b"test data for flow".to_vec()).unwrap();
        let tag = "test_file".as_bytes().to_vec();
        let flow = Flow::new(Arc::new(data), tag);
        log::info!("Flow: {:?}", String::from_utf8(flow.tags));
    }

    #[tokio::test]
    async fn test_create_submission() {
        let data = DataInMemory::new(b"test data for submission".to_vec()).unwrap();
        let tag = "test_file".as_bytes().to_vec();
        let flow = Flow::new(Arc::new(data), tag);

        // Use a dummy address for testing
        let submitter = Address::zero();
        let submission = flow.create_submission(submitter).await.unwrap();
        log::info!("Submission: {:?}", submission);
    }
}
