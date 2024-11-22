use anyhow::{bail, ensure, Context, Result};
use ethers::types::H256;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

const METADATA_SIZE: usize = 32 + 8 + 8; // H256 length + u64 + u64

#[derive(Debug, PartialEq, Eq)]
pub struct Metadata {
    pub root: H256,    // file merkle root
    pub size: usize,   // file size to download
    pub offset: usize, // offset to write for the next time
}

impl Metadata {
    pub fn new(root: H256, size: usize) -> Self {
        Self {
            root,
            size,
            offset: 0,
        }
    }

    pub fn load(file: &mut File) -> Result<Self> {
        let metadata_len = file
            .metadata()
            .context("Failed to get file metadata")?
            .len() as usize;
        ensure!(
            metadata_len >= METADATA_SIZE,
            "File size too small: {}",
            metadata_len
        );

        let mut buf = [0u8; METADATA_SIZE];
        file.seek(SeekFrom::End(-(METADATA_SIZE as i64)))
            .context("Failed to seek to metadata")?;
        file.read_exact(&mut buf)
            .context("Failed to read metadata from file")?;

        Self::deserialize(&buf)
    }

    pub fn serialize(&self) -> [u8; METADATA_SIZE] {
        let mut encoded = [0u8; METADATA_SIZE];
        encoded[..32].copy_from_slice(self.root.as_bytes());
        encoded[32..40].copy_from_slice(&(self.size as u64).to_be_bytes());
        encoded[40..].copy_from_slice(&(self.offset as u64).to_be_bytes());
        encoded
    }

    pub fn deserialize(encoded: &[u8; METADATA_SIZE]) -> Result<Self> {
        Ok(Self {
            root: H256::from_slice(&encoded[..32]),
            size: u64::from_be_bytes(encoded[32..40].try_into()?) as usize,
            offset: u64::from_be_bytes(encoded[40..].try_into()?) as usize,
        })
    }

    pub fn extend(&self, file: &mut File) -> Result<()> {
        let file_size = file
            .metadata()
            .context("Failed to get file metadata")?
            .len() as usize;

        if file_size > 0 && file_size != self.size {
            bail!(
                "Invalid file size, expected = {}, actual = {}",
                self.size,
                file_size
            );
        }

        file.set_len((self.size + METADATA_SIZE) as u64)
            .context("Failed to truncate file to extend metadata")?;

        file.seek(SeekFrom::Start(self.size as u64))
            .context("Failed to seek to metadata position")?;
        file.write_all(&self.serialize())
            .context("Failed to write metadata")?;

        Ok(())
    }

    pub fn write(&mut self, file: &mut File, data: &[u8]) -> Result<()> {
        ensure!(
            self.offset + data.len() <= self.size,
            "Written data out of bound, offset = {}, dataLen = {}, fileSize = {}",
            self.offset,
            data.len(),
            self.size
        );

        file.seek(SeekFrom::Start(self.offset as u64))
            .context("Failed to seek to write position")?;
        file.write_all(data).context("Failed to write data")?;

        log::info!("offset_before_write: {}", self.offset);
        self.offset += data.len();
        log::info!("offset_after_write: {}, data_len: {}", self.offset, data.len());

        let offset_bytes = (self.offset as u64).to_be_bytes();
        file.seek(SeekFrom::Start((self.size + METADATA_SIZE - 8) as u64))
            .context("Failed to seek to offset position in metadata")?;
        file.write_all(&offset_bytes)
            .context("Failed to update offset of metadata")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use std::io::Seek;
    use tempfile::NamedTempFile;

    #[test]
    fn test_metadata_serde() {
        let test_hash = H256::from(hex!(
            "c8ad6d515dddd96e2e3cf28735944d631621d89f78f3379ffcd0262a6d1f7092"
        ));
        let md = Metadata {
            root: test_hash,
            size: 1234567,
            offset: 456,
        };

        let encoded = md.serialize();

        let md2 = Metadata::deserialize(&encoded).unwrap();
        assert_eq!(md, md2);
    }

    #[test]
    fn test_metadata() {
        let test_hash = H256::from(hex!(
            "c8ad6d515dddd96e2e3cf28735944d631621d89f78f3379ffcd0262a6d1f7092"
        ));

        // Create temp file for testing
        let mut tmp_file = NamedTempFile::new().unwrap();
        log::info!("File size: {}", tmp_file.as_file().metadata().unwrap().len());

        // Extend file with metadata
        let mut md = Metadata::new(test_hash, 12345);
        md.extend(tmp_file.as_file_mut()).unwrap();
        log::info!("File size: {}", tmp_file.as_file().metadata().unwrap().len());

        // Check file size after metadata extended
        let file_size = tmp_file.as_file().metadata().unwrap().len();
        assert_eq!(md.size + METADATA_SIZE, file_size as usize);

        // Write some data and update metadata
        let data = b"hello, world";
        md.write(tmp_file.as_file_mut(), data).unwrap();
        assert_eq!(data.len(), md.offset);

        // Seek to the beginning of the file
        tmp_file.seek(SeekFrom::Start(0)).unwrap();

        // Load metadata from file
        let md2 = Metadata::load(tmp_file.as_file_mut()).unwrap();
        assert_eq!(md, md2);

        // Clean up
        drop(tmp_file);
    }
}
