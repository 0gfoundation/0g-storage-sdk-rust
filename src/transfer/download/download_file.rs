use std::fs::{File, OpenOptions, rename};
use std::path::PathBuf;
use anyhow::{Result, Context, ensure};
use ethers::types::H256;
use super::metadata::Metadata;

const DOWNLOADING_FILE_SUFFIX: &str = ".download";

pub struct DownloadingFile {
    file_name: String,
    underlying: Option<File>,
    metadata: Metadata,
}

impl DownloadingFile {
    pub fn create(file: &PathBuf, root: H256, size: usize) -> Result<Self> {
        let file_name = file.to_str().unwrap();
        let download_filename = format!("{}{}", file_name, DOWNLOADING_FILE_SUFFIX);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&download_filename)
            .context("Failed to open file")?;

        let metadata = if file.metadata()?.len() == 0 {
            let metadata = Metadata::new(root, size);
            metadata.extend(&mut file)?;
            metadata
        } else {
            Metadata::load(&mut file)?
        };

        ensure!(metadata.root == root, "Root mismatch, expected = {:?}, actual = {:?}", root, metadata.root);
        ensure!(metadata.size == size, "File size mismatch, expected = {}, actual = {}", size, metadata.size);

        Ok(Self {
            file_name: file_name.to_string(),
            underlying: Some(file),
            metadata,
        })
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        let file = self.underlying.as_mut()
            .ok_or_else(|| anyhow::anyhow!("File already sealed"))?;
        
        self.metadata.write(file, data)
    }

    pub fn seal(&mut self) -> Result<()> {
        ensure!(self.metadata.offset >= self.metadata.size, 
            "Download incomplete, offset = {}, size = {}", self.metadata.offset, self.metadata.size);

        let file = self.underlying.as_mut()
            .ok_or_else(|| anyhow::anyhow!("File already sealed"))?;

        file.set_len(self.metadata.size as u64)
            .context("Failed to truncate metadata")?;

        drop(self.underlying.take());

        rename(format!("{}{}", self.file_name, DOWNLOADING_FILE_SUFFIX), &self.file_name)
            .context("Failed to rename downloading file")?;

        Ok(())
    }
}

impl Drop for DownloadingFile {
    fn drop(&mut self) {
        if let Some(file) = self.underlying.take() {
            drop(file);
        }
    }
}