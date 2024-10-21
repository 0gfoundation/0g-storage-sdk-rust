use anyhow::Result;
use super::dataflow::{num_splits, DEFAULT_CHUNK_SIZE};
use super::flow::compute_padded_size;

pub trait Iterator {
    fn next(&mut self) -> Result<bool>;
    fn current(&self) -> &[u8];
}

pub fn iterator_padded_size(data_size: usize, flow_padding: bool) -> u64 {
    let chunks = num_splits(data_size, DEFAULT_CHUNK_SIZE);
    let padded_size = if flow_padding {
        let (padded_chunks, _) = compute_padded_size(chunks as u64);
        padded_chunks * DEFAULT_CHUNK_SIZE as u64
    } else {
        chunks as u64 * DEFAULT_CHUNK_SIZE as u64
    };
    padded_size
}