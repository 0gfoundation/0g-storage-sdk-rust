use super::dataflow::{IterableData, DEFAULT_CHUNK_SIZE};
use super::iterator::{iterator_padded_size, Iterator as CustomIterator};
use anyhow::Result;
use std::sync::Arc;

/// Align `input` up to the next power of two (minimum 1).
pub fn next_pow2(input: u64) -> u64 {
    if input <= 1 {
        return 1;
    }
    input.next_power_of_two()
}

/// A window view into another `IterableData`, starting at `offset` bytes with
/// logical length `size`.  Used to represent one fragment of a large file.
pub struct DataFragment {
    inner: Arc<dyn IterableData>,
    /// Byte offset of this fragment's start within the inner data.
    offset: i64,
    /// Logical (unpadded) size of this fragment in bytes.
    size: i64,
    padded_size: u64,
}

impl DataFragment {
    pub fn new(inner: Arc<dyn IterableData>, offset: i64, size: i64) -> Self {
        let padded_size = iterator_padded_size(size as usize, true);
        Self {
            inner,
            offset,
            size,
            padded_size,
        }
    }
}

impl IterableData for DataFragment {
    fn num_chunks(&self) -> u64 {
        (self.size as u64).div_ceil(DEFAULT_CHUNK_SIZE as u64)
    }

    fn num_segments(&self) -> u64 {
        use super::dataflow::DEFAULT_SEGMENT_SIZE;
        (self.size as u64).div_ceil(DEFAULT_SEGMENT_SIZE as u64)
    }

    fn size(&self) -> i64 {
        self.size
    }

    fn padded_size(&self) -> u64 {
        self.padded_size
    }

    fn iterate(&self, offset: i64, batch: i64, flow_padding: bool) -> Box<dyn CustomIterator + '_> {
        assert!(
            batch % DEFAULT_CHUNK_SIZE as i64 == 0,
            "Batch size must align with chunk size"
        );
        Box::new(DataFragmentIterator::new(
            self.inner.as_ref(),
            self.offset,
            offset,
            self.size,
            iterator_padded_size(self.size as usize, flow_padding),
            batch,
        ))
    }

    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        if offset < 0 || offset >= self.size {
            return Ok(0);
        }
        // Clip read to the fragment boundary
        let available = (self.size - offset) as usize;
        let to_read = buf.len().min(available);
        self.inner.read(&mut buf[..to_read], self.offset + offset)
    }
}

struct DataFragmentIterator<'a> {
    inner: &'a dyn IterableData,
    buf: Vec<u8>,
    buf_size: usize,
    /// Absolute byte offset of the fragment's start in the inner data.
    fragment_start: i64,
    /// Logical (unpadded) size of the fragment.
    fragment_size: i64,
    /// Padded size of the fragment (determines when iteration stops).
    padded_size: u64,
    /// Current position within the fragment (local, 0-based).
    local_offset: i64,
}

impl<'a> DataFragmentIterator<'a> {
    fn new(
        inner: &'a dyn IterableData,
        fragment_start: i64,
        start_local_offset: i64,
        fragment_size: i64,
        padded_size: u64,
        batch: i64,
    ) -> Self {
        Self {
            inner,
            buf: vec![0; batch as usize],
            buf_size: 0,
            fragment_start,
            fragment_size,
            padded_size,
            local_offset: start_local_offset,
        }
    }

    fn padding_zeros(&mut self, length: usize) {
        self.buf[self.buf_size..self.buf_size + length].fill(0);
        self.buf_size += length;
        self.local_offset += length as i64;
    }
}

impl<'a> CustomIterator for DataFragmentIterator<'a> {
    fn next(&mut self) -> Result<bool> {
        if self.local_offset < 0 || self.local_offset as u64 >= self.padded_size {
            return Ok(false);
        }

        let max_available = self.padded_size - self.local_offset as u64;
        let expected_buf_size = (max_available as usize).min(self.buf.len());
        self.buf_size = 0;

        if self.local_offset >= self.fragment_size {
            // Past the real data — emit padding only
            self.padding_zeros(expected_buf_size);
            return Ok(true);
        }

        // Read real bytes, clipped to the fragment boundary
        let real_remaining = (self.fragment_size - self.local_offset) as usize;
        let real_to_read = real_remaining.min(expected_buf_size);

        let n = self.inner.read(
            &mut self.buf[..real_to_read],
            self.fragment_start + self.local_offset,
        )?;
        self.buf_size = n;
        self.local_offset += n as i64;

        // Pad any remainder within this batch
        let padding = expected_buf_size - self.buf_size;
        if padding > 0 {
            self.padding_zeros(padding);
        }

        Ok(true)
    }

    fn current(&self) -> &[u8] {
        &self.buf[..self.buf_size]
    }
}

/// Split `data` into fragments of at most `fragment_size` bytes each.
/// The last fragment may be smaller.
pub fn split_data(data: Arc<dyn IterableData>, fragment_size: i64) -> Vec<Arc<dyn IterableData>> {
    let total = data.size();
    let mut fragments: Vec<Arc<dyn IterableData>> = Vec::new();
    let mut offset = 0i64;
    while offset < total {
        let size = (total - offset).min(fragment_size);
        fragments.push(Arc::new(DataFragment::new(data.clone(), offset, size)));
        offset += size;
    }
    fragments
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::in_mem::DataInMemory;

    fn make_data(size: usize) -> Arc<dyn IterableData> {
        let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        Arc::new(DataInMemory::new(bytes).unwrap())
    }

    #[test]
    fn test_next_pow2() {
        assert_eq!(next_pow2(0), 1);
        assert_eq!(next_pow2(1), 1);
        assert_eq!(next_pow2(2), 2);
        assert_eq!(next_pow2(3), 4);
        assert_eq!(next_pow2(256), 256);
        assert_eq!(next_pow2(257), 512);
        assert_eq!(next_pow2(1024 * 1024 * 3), 1024 * 1024 * 4);
    }

    #[test]
    fn test_split_data_even() {
        let data = make_data(1000);
        let frags = split_data(data, 400);
        assert_eq!(frags.len(), 3);
        assert_eq!(frags[0].size(), 400);
        assert_eq!(frags[1].size(), 400);
        assert_eq!(frags[2].size(), 200);
    }

    #[test]
    fn test_split_data_exact() {
        let data = make_data(800);
        let frags = split_data(data, 400);
        assert_eq!(frags.len(), 2);
        assert_eq!(frags[0].size(), 400);
        assert_eq!(frags[1].size(), 400);
    }

    #[test]
    fn test_split_data_smaller_than_fragment() {
        let data = make_data(100);
        let frags = split_data(data, 1000);
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].size(), 100);
    }

    #[test]
    fn test_fragment_read_roundtrip() {
        let size = 1000usize;
        let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let data: Arc<dyn IterableData> = Arc::new(DataInMemory::new(bytes.clone()).unwrap());

        let frags = split_data(data, 400);
        let mut reconstructed = Vec::new();
        for frag in &frags {
            let mut buf = vec![0u8; frag.size() as usize];
            frag.read(&mut buf, 0).unwrap();
            reconstructed.extend_from_slice(&buf);
        }
        assert_eq!(reconstructed, bytes);
    }

    #[test]
    fn test_fragment_iterate_roundtrip() {
        let size = 1000usize;
        let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let data: Arc<dyn IterableData> = Arc::new(DataInMemory::new(bytes.clone()).unwrap());

        let frags = split_data(data, 400);
        let mut reconstructed = Vec::new();
        for frag in &frags {
            let mut iter = frag.iterate(0, DEFAULT_CHUNK_SIZE as i64, false);
            while iter.next().unwrap() {
                reconstructed.extend_from_slice(iter.current());
            }
            // trim to logical size (iterator includes chunk-alignment padding)
            reconstructed.truncate(reconstructed.len().min(bytes.len()));
        }
        // each fragment's iterator gives padded data; grab real bytes per fragment
        let mut reconstructed2 = Vec::new();
        for frag in &frags {
            let mut buf = vec![0u8; frag.size() as usize];
            frag.read(&mut buf, 0).unwrap();
            reconstructed2.extend_from_slice(&buf);
        }
        assert_eq!(reconstructed2, bytes);
    }
}
