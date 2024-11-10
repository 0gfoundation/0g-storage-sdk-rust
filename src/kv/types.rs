use std::convert::TryInto;
use sha2::{Sha256, Digest};
use ethers::types::{H256, Address};
use thiserror::Error;
use byteorder::{BigEndian, ByteOrder};

// Constants
const HASH_LENGTH: usize = 32;
const ADDRESS_LENGTH: usize = 20;

// Stream domain constant
lazy_static::lazy_static! {
    pub static ref STREAM_DOMAIN: H256 = {
        let mut hasher = Sha256::new();
        hasher.update(b"STREAM");
        H256::from_slice(&hasher.finalize())
    };
}

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("size too large")]
    SizeTooLarge,
    #[error("key too large")]
    KeyTooLarge,
    #[error("key is empty")]
    KeyIsEmpty,
    #[error("hex decode error: {0}")]
    HexError(#[from] hex::FromHexError),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum AccessControlType {
    // Admin role
    GrantAdminRole = 0x00,
    RenounceAdminRole = 0x01,

    // set/unset special key
    SetKeyToSpecial = 0x10,
    SetKeyToNormal = 0x11,

    // Write role for all keys
    GrantWriteRole = 0x20,
    RevokeWriteRole = 0x21,
    RenounceWriteRole = 0x22,

    // Write role for special key
    GrantSpecialWriteRole = 0x30,
    RevokeSpecialWriteRole = 0x31,
    RenounceSpecialWriteRole = 0x32,
}

#[derive(Debug, Clone)]
pub struct StreamRead {
    pub stream_id: H256,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StreamWrite {
    pub stream_id: H256,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct AccessControl {
    pub control_type: AccessControlType,
    pub stream_id: H256,
    pub account: Option<Address>,
    pub key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct StreamData {
    pub version: u64,
    pub reads: Vec<StreamRead>,
    pub writes: Vec<StreamWrite>,
    pub controls: Vec<AccessControl>,
}

pub fn create_tags(stream_ids: &[H256]) -> Vec<u8> {
    let mut result = Vec::with_capacity(HASH_LENGTH * (1 + stream_ids.len()));
    result.extend_from_slice(STREAM_DOMAIN.as_bytes());
    
    for id in stream_ids {
        result.extend_from_slice(id.as_bytes());
    }
    
    result
}

impl StreamData {
    /// Returns the serialized data size in bytes
    pub fn size(&self) -> usize {
        let mut size = 8; // version

        // reads
        size += 4; // size
        for read in &self.reads {
            size += HASH_LENGTH + 3 + read.key.len();
        }

        // writes
        size += 4; // size
        for write in &self.writes {
            size += HASH_LENGTH + 3 + write.key.len() + 8 + write.data.len();
        }

        // acls
        size += 4; // size
        for control in &self.controls {
            size += 1 + HASH_LENGTH; // type + stream_id

            if control.account.is_some() {
                size += ADDRESS_LENGTH;
            }

            if let Some(key) = &control.key {
                size += 3 + key.len();
            }
        }

        size
    }

    fn encode_size24(&self, size: usize) -> Result<[u8; 3], StreamError> {
        if size == 0 {
            return Err(StreamError::KeyIsEmpty);
        }
        
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, size as u32);
        
        if buf[0] != 0 {
            return Err(StreamError::KeyTooLarge);
        }
        
        Ok([buf[1], buf[2], buf[3]])
    }

    fn encode_size32(&self, size: usize) -> [u8; 4] {
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, size as u32);
        buf
    }

    fn encode_size64(&self, size: usize) -> [u8; 8] {
        let mut buf = [0u8; 8];
        BigEndian::write_u64(&mut buf, size as u64);
        buf
    }

    pub fn encode(&self) -> Result<Vec<u8>, StreamError> {
        let mut encoded = Vec::with_capacity(self.size());

        // version
        encoded.extend_from_slice(&self.encode_size64(self.version as usize));

        // reads
        encoded.extend_from_slice(&self.encode_size32(self.reads.len()));
        for read in &self.reads {
            encoded.extend_from_slice(read.stream_id.as_bytes());
            encoded.extend_from_slice(&self.encode_size24(read.key.len())?);
            encoded.extend_from_slice(&read.key);
        }

        // writes
        encoded.extend_from_slice(&self.encode_size32(self.writes.len()));
        for write in &self.writes {
            encoded.extend_from_slice(write.stream_id.as_bytes());
            encoded.extend_from_slice(&self.encode_size24(write.key.len())?);
            encoded.extend_from_slice(&write.key);
            encoded.extend_from_slice(&self.encode_size64(write.data.len()));
        }

        // Write all data blocks after the metadata
        for write in &self.writes {
            encoded.extend_from_slice(&write.data);
        }

        // acls
        encoded.extend_from_slice(&self.encode_size32(self.controls.len()));
        for control in &self.controls {
            encoded.push(control.control_type as u8);
            encoded.extend_from_slice(control.stream_id.as_bytes());

            if let Some(key) = &control.key {
                encoded.extend_from_slice(&self.encode_size24(key.len())?);
                encoded.extend_from_slice(key);
            }

            if let Some(account) = &control.account {
                encoded.extend_from_slice(account.as_bytes());
            }
        }

        Ok(encoded)
    }
}

// Optional: Implement decode functionality if needed
impl StreamData {
    pub fn decode(data: &[u8]) -> Result<Self, StreamError> {
        // Implementation of decode logic here
        // This would reverse the encode process
        todo!("Implement decode functionality")
    }
}