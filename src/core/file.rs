use anyhow::{anyhow, Context, Result};
use ethers::types::H256;
use std::fs::File as StdFile;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use super::dataflow::{merkle_tree, IterableData, DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_SIZE};
use super::iterator::{iterator_padded_size, Iterator as CustomIterator};

pub struct File {
    pub info: std::fs::Metadata,
    pub underlying: StdFile,
    pub padded_size: u64,
}

impl File {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = StdFile::open(&path).context("Failed to open file")?;
        let info = file.metadata().context("Failed to get file metadata")?;

        if info.is_dir() {
            return Err(anyhow!("Expected a file, but got a directory"));
        }

        if info.len() == 0 {
            return Err(anyhow!("File is empty"));
        }

        let padded_size = iterator_padded_size(info.len() as usize, true);

        Ok(File {
            info,
            underlying: file,
            padded_size,
        })
    }

    pub fn exists<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().exists()
    }

    pub async fn merkle_root<P: AsRef<Path>>(path: P) -> Result<H256> {
        let file = Self::open(path).context("Failed to open file for Merkle root calculation")?;
        let tree = merkle_tree(Arc::new(file))
            .await
            .context("Failed to generate Merkle tree")?;
        Ok(tree.root())
    }
}

impl IterableData for File {
    fn num_chunks(&self) -> u64 {
        (self.info.len() as f64 / DEFAULT_CHUNK_SIZE as f64).ceil() as u64
    }

    fn num_segments(&self) -> u64 {
        (self.info.len() as f64 / DEFAULT_SEGMENT_SIZE as f64).ceil() as u64
    }

    fn size(&self) -> i64 {
        self.info.len() as i64
    }

    fn padded_size(&self) -> u64 {
        self.padded_size
    }

    fn iterate(&self, offset: i64, batch: i64, flow_padding: bool) -> Box<dyn CustomIterator + '_> {
        assert!(
            batch % DEFAULT_CHUNK_SIZE as i64 == 0,
            "Batch size must align with chunk size"
        );
        Box::new(FileIterator::new(
            &self.underlying,
            offset,
            self.info.len() as i64,
            iterator_padded_size(self.info.len() as usize, flow_padding),
            batch,
        ))
    }

    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        let mut file = self
            .underlying
            .try_clone()
            .context("Failed to clone file handle")?;
        file.seek(SeekFrom::Start(offset as u64))
            .context("Failed to seek in file")?;
        let n = file.read(buf).context("Failed to read from file")?;
        if n == 0 && offset < self.size() {
            return Err(anyhow!(
                "Unexpected EOF, offset: {}, data_size: {}",
                offset,
                self.size()
            ));
        }
        Ok(n)
    }
}

struct FileIterator<'a> {
    file: &'a StdFile,
    buf: Vec<u8>,
    buf_size: usize,
    file_size: i64,
    padded_size: u64,
    offset: i64,
}

impl<'a> FileIterator<'a> {
    fn new(file: &'a StdFile, offset: i64, file_size: i64, padded_size: u64, batch: i64) -> Self {
        FileIterator {
            file,
            buf: vec![0; batch as usize],
            buf_size: 0,
            file_size,
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

impl<'a> CustomIterator for FileIterator<'a> {
    fn next(&mut self) -> Result<bool> {
        if self.offset < 0 || self.offset as u64 >= self.padded_size {
            return Ok(false);
        }

        let max_available_length = self.padded_size - self.offset as u64;
        let expected_buf_size = std::cmp::min(max_available_length as usize, self.buf.len());

        self.clear_buffer();

        if self.offset >= self.file_size {
            self.padding_zeros(expected_buf_size);
            return Ok(true);
        }

        let mut file = self
            .file
            .try_clone()
            .context("Failed to clone file handle")?;
        file.seek(SeekFrom::Start(self.offset as u64))
            .context("Failed to seek in file")?;
        let n = file
            .read(&mut self.buf[..expected_buf_size])
            .context("Failed to read from file")?;
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
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn create_temp_file_with_content(content: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file
    }

    #[test]
    fn test_file_open_success() {
        let content = b"Hello, world!";
        let file = create_temp_file_with_content(content);
        let result = File::open(file.path());
        assert!(result.is_ok(), "Should open successfully");
    }

    #[test]
    fn test_file_open_empty_error() {
        let empty_file = NamedTempFile::new().unwrap();
        let result = File::open(empty_file.path());
        assert!(result.is_err(), "Opening empty file should return an error");
    }

    #[test]
    fn test_file_open_dir_error() {
        let temp_dir = TempDir::new().unwrap();
        let result = File::open(temp_dir.path());
        assert!(
            result.is_err(),
            "Opening a directory should return an error"
        );
    }

    #[tokio::test]
    async fn test_merkle_root() {
        let content = b"Some data to create a Merkle root";
        let file = create_temp_file_with_content(content);
        let result = File::merkle_root(file.path()).await;
        assert!(result.is_ok(), "Merkle root calculation should succeed");
        let root = result.unwrap();
        log::info!("File root is: {:?}", root);
    }

    #[test]
    fn test_file_iterate() {
        let content = b"12345678901234567890"; // 20 bytes
        let file = create_temp_file_with_content(content);
        let file_struct = File::open(file.path()).unwrap();
        log::info!("Padded length: {}", file_struct.padded_size);

        let mut iterator = file_struct.iterate(0, 256, true);
        let mut collected_data = Vec::new();

        while iterator.next().unwrap() {
            collected_data.extend_from_slice(iterator.current());
        }

        assert_eq!(
            &collected_data[..content.len()],
            content,
            "Iterated data should match file content"
        );
    }
}
