use super::dataflow::{IterableData, DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_SIZE};
use super::iterator::{iterator_padded_size, Iterator as CustomIterator};
use crate::transfer::encryption::{crypt_at, EncryptionHeader, ENCRYPTION_HEADER_SIZE};
use anyhow::Result;
use std::sync::Arc;

pub struct EncryptedData {
    inner: Arc<dyn IterableData>,
    key: [u8; 32],
    header: EncryptionHeader,
    encrypted_size: i64,
    padded_size: u64,
}

impl EncryptedData {
    pub fn new(inner: Arc<dyn IterableData>, key: [u8; 32]) -> Result<Self> {
        let header = EncryptionHeader::new();
        let encrypted_size = inner.size() + ENCRYPTION_HEADER_SIZE as i64;
        let padded_size = iterator_padded_size(encrypted_size as usize, true);

        Ok(EncryptedData {
            inner,
            key,
            header,
            encrypted_size,
            padded_size,
        })
    }

    pub fn header(&self) -> &EncryptionHeader {
        &self.header
    }
}

impl IterableData for EncryptedData {
    fn num_chunks(&self) -> u64 {
        (self.encrypted_size as f64 / DEFAULT_CHUNK_SIZE as f64).ceil() as u64
    }

    fn num_segments(&self) -> u64 {
        (self.encrypted_size as f64 / DEFAULT_SEGMENT_SIZE as f64).ceil() as u64
    }

    fn size(&self) -> i64 {
        self.encrypted_size
    }

    fn padded_size(&self) -> u64 {
        self.padded_size
    }

    fn iterate(&self, offset: i64, batch: i64, flow_padding: bool) -> Box<dyn CustomIterator + '_> {
        assert!(
            batch % DEFAULT_CHUNK_SIZE as i64 == 0,
            "Batch size must align with chunk size"
        );

        Box::new(EncryptedDataIterator::new(
            self,
            offset,
            self.encrypted_size,
            iterator_padded_size(self.encrypted_size as usize, flow_padding),
            batch,
        ))
    }

    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        if offset < 0 || offset >= self.encrypted_size {
            return Ok(0);
        }

        let header_size = ENCRYPTION_HEADER_SIZE as i64;
        let mut written = 0;

        // If offset falls within the header region
        if offset < header_size {
            let header_bytes = self.header.to_bytes();
            let header_start = offset as usize;
            let header_end = std::cmp::min(ENCRYPTION_HEADER_SIZE, header_start + buf.len());
            let n = header_end - header_start;
            buf[..n].copy_from_slice(&header_bytes[header_start..header_end]);
            written += n;
        }

        // If we still have room in buf and there's data beyond the header
        if written < buf.len() {
            let data_offset = if offset < header_size {
                0i64
            } else {
                offset - header_size
            };

            let remaining_buf = &mut buf[written..];
            let inner_read = self.inner.read(remaining_buf, data_offset)?;

            // Encrypt the data we just read
            if inner_read > 0 {
                crypt_at(
                    &self.key,
                    &self.header.nonce,
                    data_offset as u64,
                    &mut buf[written..written + inner_read],
                );
            }

            written += inner_read;
        }

        Ok(written)
    }
}

struct EncryptedDataIterator<'a> {
    data: &'a EncryptedData,
    buf: Vec<u8>,
    buf_size: usize,
    data_size: i64,
    padded_size: u64,
    offset: i64,
}

impl<'a> EncryptedDataIterator<'a> {
    fn new(
        data: &'a EncryptedData,
        offset: i64,
        data_size: i64,
        padded_size: u64,
        batch: i64,
    ) -> Self {
        EncryptedDataIterator {
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

impl<'a> CustomIterator for EncryptedDataIterator<'a> {
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

        let n = self
            .data
            .read(&mut self.buf[..expected_buf_size], self.offset)?;
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
    use crate::core::in_mem::DataInMemory;
    use crate::transfer::encryption::decrypt_file;

    #[test]
    fn test_encrypted_data_size() {
        let original = vec![1u8; 1000];
        let inner = Arc::new(DataInMemory::new(original.clone()).unwrap());
        let key = [0x42u8; 32];
        let encrypted = EncryptedData::new(inner.clone(), key).unwrap();

        assert_eq!(
            encrypted.size(),
            inner.size() + ENCRYPTION_HEADER_SIZE as i64
        );
    }

    #[test]
    fn test_encrypted_data_read_header() {
        let original = vec![1u8; 100];
        let inner = Arc::new(DataInMemory::new(original).unwrap());
        let key = [0x42u8; 32];
        let encrypted = EncryptedData::new(inner, key).unwrap();

        // Read just the header
        let mut buf = vec![0u8; ENCRYPTION_HEADER_SIZE];
        let n = encrypted.read(&mut buf, 0).unwrap();
        assert_eq!(n, ENCRYPTION_HEADER_SIZE);
        assert_eq!(buf[0], 1); // version
        assert_eq!(&buf[1..17], &encrypted.header().nonce);
    }

    #[test]
    fn test_encrypted_data_roundtrip() {
        let original = b"hello world encryption test with EncryptedData wrapper".to_vec();
        let inner = Arc::new(DataInMemory::new(original.clone()).unwrap());
        let key = [0x42u8; 32];
        let encrypted = EncryptedData::new(inner, key).unwrap();

        // Read full encrypted stream
        let encrypted_size = encrypted.size() as usize;
        let mut encrypted_buf = vec![0u8; encrypted_size];
        let n = encrypted.read(&mut encrypted_buf, 0).unwrap();
        assert_eq!(n, encrypted_size);

        // Decrypt and verify
        let decrypted = decrypt_file(&key, &encrypted_buf).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn test_encrypted_data_read_at_offset() {
        let original = vec![0xABu8; 500];
        let inner = Arc::new(DataInMemory::new(original.clone()).unwrap());
        let key = [0x42u8; 32];
        let encrypted = EncryptedData::new(inner, key).unwrap();

        // Read full encrypted data
        let encrypted_size = encrypted.size() as usize;
        let mut full_buf = vec![0u8; encrypted_size];
        encrypted.read(&mut full_buf, 0).unwrap();

        // Read in two parts and verify they match
        let split = 100;
        let mut part1 = vec![0u8; split];
        let mut part2 = vec![0u8; encrypted_size - split];
        encrypted.read(&mut part1, 0).unwrap();
        encrypted.read(&mut part2, split as i64).unwrap();

        assert_eq!(&full_buf[..split], &part1);
        assert_eq!(&full_buf[split..], &part2);
    }

    #[test]
    fn test_encrypted_data_iterate() {
        let original = vec![0x55u8; 300];
        let inner = Arc::new(DataInMemory::new(original.clone()).unwrap());
        let key = [0x42u8; 32];
        let encrypted = EncryptedData::new(inner, key).unwrap();

        let mut iterator = encrypted.iterate(0, DEFAULT_CHUNK_SIZE as i64, true);
        let mut collected = Vec::new();

        while iterator.next().unwrap() {
            collected.extend_from_slice(iterator.current());
        }

        // Collected should be padded_size bytes
        assert_eq!(collected.len(), encrypted.padded_size() as usize);

        // First encrypted_size bytes should match what read() returns
        let encrypted_size = encrypted.size() as usize;
        let mut read_buf = vec![0u8; encrypted_size];
        encrypted.read(&mut read_buf, 0).unwrap();
        assert_eq!(&collected[..encrypted_size], &read_buf);
    }
}
