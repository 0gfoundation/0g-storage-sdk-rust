use std::sync::Arc;
use anyhow::{Result, anyhow, Context};
use ethers::types::H256;
use super::dataflow::{IterableData, DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_SIZE, merkle_tree};
use super::iterator::{Iterator as CustomIterator, iterator_padded_size};

pub struct DataInMemory {
    underlying: Vec<u8>,
    padded_size: u64,
}

impl DataInMemory {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        if data.is_empty() {
            return Err(anyhow!("data is empty"));
        }

        Ok(DataInMemory {
            padded_size: iterator_padded_size(data.len(), true),
            underlying: data,
        })
    }

    pub async fn merkle_root(data: Arc<Self>) -> Result<H256> {
        let tree = merkle_tree(data)
            .await
            .context("Failed to generate Merkle tree")?;
        Ok(tree.root())
    }
}

impl IterableData for DataInMemory {
    fn num_chunks(&self) -> u64 {
        (self.underlying.len() as f64 / DEFAULT_CHUNK_SIZE as f64).ceil() as u64
    }

    fn num_segments(&self) -> u64 {
        (self.underlying.len() as f64 / DEFAULT_SEGMENT_SIZE as f64).ceil() as u64
    }

    fn size(&self) -> i64 {
        self.underlying.len() as i64
    }

    fn padded_size(&self) -> u64 {
        self.padded_size
    }

    fn iterate(&self, offset: i64, batch: i64, flow_padding: bool) -> Box<dyn CustomIterator + '_> {
        assert!(
            batch % DEFAULT_CHUNK_SIZE as i64 == 0,
            "Batch size must align with chunk size"
        );
        
        Box::new(MemoryDataIterator::new(
            &self.underlying,
            offset,
            self.underlying.len() as i64,
            iterator_padded_size(self.underlying.len(), flow_padding),
            batch,
        ))
    }

    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        if offset < 0 || offset as usize >= self.underlying.len() {
            return Ok(0);
        }
        
        let available_data = &self.underlying[offset as usize..];
        let n = std::cmp::min(buf.len(), available_data.len());
        buf[..n].copy_from_slice(&available_data[..n]);
        
        Ok(n)
    }
}

struct MemoryDataIterator<'a> {
    data: &'a [u8],
    buf: Vec<u8>,
    buf_size: usize,
    data_size: i64,
    padded_size: u64,
    offset: i64,
}

impl<'a> MemoryDataIterator<'a> {
    fn new(data: &'a [u8], offset: i64, data_size: i64, padded_size: u64, batch: i64) -> Self {
        MemoryDataIterator {
            data,
            buf: vec![0; batch as usize],
            buf_size: 0,
            data_size,
            padded_size,
            offset,
        }
    }

    fn clear_buffer(&mut self) {
        self.buf_size = 0;
    }

    fn padding_zeros(&mut self, length: usize) {
        self.buf[self.buf_size..self.buf_size + length].fill(0);
        self.buf_size += length;
        self.offset += length as i64;
    }
}

impl<'a> CustomIterator for MemoryDataIterator<'a> {
    fn next(&mut self) -> Result<bool> {
        if self.offset < 0 || self.offset as u64 >= self.padded_size {
            return Ok(false);
        }

        let max_available_length = self.padded_size - self.offset as u64;
        let expected_buf_size = std::cmp::min(max_available_length as usize, self.buf.len());

        self.clear_buffer();

        if self.offset >= self.data_size {
            self.padding_zeros(expected_buf_size);
            return Ok(true);
        }

        let start = self.offset as usize;
        let available_data = &self.data[start..];
        let n = std::cmp::min(expected_buf_size, available_data.len());
        
        self.buf[..n].copy_from_slice(&available_data[..n]);
        self.buf_size = n;
        self.offset += n as i64;

        if n < expected_buf_size {
            self.padding_zeros(expected_buf_size - n);
        }

        Ok(true)
    }

    fn current(&self) -> &[u8] {
        &self.buf[..self.buf_size]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_in_memory_empty() {
        let result = DataInMemory::new(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_data_in_memory_basic() {
        let data = vec![1, 2, 3, 4, 5];
        let mem_data = DataInMemory::new(data.clone()).unwrap();
        
        assert_eq!(mem_data.size(), 5);
        assert!(mem_data.padded_size() >= 5);
        
        let mut buf = vec![0; 3];
        let n = mem_data.read(&mut buf, 1).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], &[2, 3, 4]);
    }

    #[test]
    fn test_memory_iterator() {
        let data = vec![1, 2, 3, 4, 5];
        let mem_data = DataInMemory::new(data).unwrap();
        
        let mut iterator = mem_data.iterate(0, DEFAULT_CHUNK_SIZE as i64, true);
        assert!(iterator.next().unwrap());
        let current = iterator.current();
        assert!(!current.is_empty());
    }
}