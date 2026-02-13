use ethers::types::{Address, H256};
use hex::{decode as hex_decode, encode as hex_encode};
use std::cmp::Ordering;
use std::collections::HashMap;

use super::types::{
    AccessControl, AccessControlType, StreamData, StreamError, StreamRead, StreamWrite,
    STREAM_DOMAIN,
};

// Constants
const MAX_SET_SIZE: usize = 1 << 16; // 64K
const MAX_KEY_SIZE: usize = 1 << 24; // 16.7M
pub const MAX_QUERY_SIZE: usize = 1024 * 256;

#[derive(Debug)]
pub struct StreamDataBuilder {
    version: u64,
    stream_ids: HashMap<H256, bool>,
    controls: Vec<AccessControl>,
    reads: HashMap<H256, HashMap<String, bool>>,
    writes: HashMap<H256, HashMap<String, Vec<u8>>>,
}

impl StreamDataBuilder {
    pub fn new(version: u64) -> Self {
        Self {
            version,
            stream_ids: HashMap::new(),
            controls: Vec::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
        }
    }

    pub fn build(&self, sorted: Option<bool>) -> Result<StreamData, StreamError> {
        let mut data = StreamData {
            version: self.version,
            controls: self.build_access_control()?,
            reads: Vec::new(),
            writes: Vec::new(),
        };

        // Build reads
        for (stream_id, keys) in &self.reads {
            for k in keys.keys() {
                let key = hex_decode(k.trim_start_matches("0x"))?;
                if key.len() > MAX_KEY_SIZE {
                    return Err(StreamError::KeyTooLarge);
                }
                if key.is_empty() {
                    return Err(StreamError::KeyIsEmpty);
                }
                data.reads.push(StreamRead {
                    stream_id: *stream_id,
                    key,
                });

                if data.reads.len() > MAX_SET_SIZE {
                    return Err(StreamError::SizeTooLarge);
                }
            }
        }

        // Build writes
        for (stream_id, keys) in &self.writes {
            for (k, v) in keys {
                let key = hex_decode(k.trim_start_matches("0x"))?;
                if key.len() > MAX_KEY_SIZE {
                    return Err(StreamError::KeyTooLarge);
                }
                if key.is_empty() {
                    return Err(StreamError::KeyIsEmpty);
                }
                data.writes.push(StreamWrite {
                    stream_id: *stream_id,
                    key,
                    data: v.clone(),
                });

                if data.writes.len() > MAX_SET_SIZE {
                    return Err(StreamError::SizeTooLarge);
                }
            }
        }

        // Sort if requested
        if sorted.unwrap_or(false) {
            data.reads.sort_by(|a, b| {
                let stream_cmp = format!("{:?}", a.stream_id).cmp(&format!("{:?}", b.stream_id));
                if stream_cmp == Ordering::Equal {
                    hex_encode(&a.key).cmp(&hex_encode(&b.key))
                } else {
                    stream_cmp
                }
            });

            data.writes.sort_by(|a, b| {
                let stream_cmp = format!("{:?}", a.stream_id).cmp(&format!("{:?}", b.stream_id));
                if stream_cmp == Ordering::Equal {
                    hex_encode(&a.key).cmp(&hex_encode(&b.key))
                } else {
                    stream_cmp
                }
            });
        }

        Ok(data)
    }

    fn add_stream_id(&mut self, stream_id: H256) {
        self.stream_ids.insert(stream_id, true);
    }

    pub fn build_tags(&self, sorted: Option<bool>) -> Vec<u8> {
        let mut ids: Vec<H256> = self.stream_ids.keys().cloned().collect();

        if sorted.unwrap_or(false) {
            ids.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
        }

        create_tags(&ids)
    }

    pub fn set_version(&mut self, version: u64) -> &mut Self {
        self.version = version;
        self
    }

    pub fn watch(&mut self, stream_id: H256, key: &[u8]) -> &mut Self {
        let key_hex = format!("0x{}", hex_encode(key));
        self.reads
            .entry(stream_id)
            .or_default()
            .insert(key_hex, true);
        self
    }

    pub fn set(&mut self, stream_id: H256, key: &[u8], data: Vec<u8>) -> &mut Self {
        self.add_stream_id(stream_id);
        let key_hex = format!("0x{}", hex_encode(key));
        println!("key hex: {:?}", key_hex);
        self.writes
            .entry(stream_id)
            .or_default()
            .insert(key_hex, data);
        self
    }

    fn build_access_control(&self) -> Result<Vec<AccessControl>, StreamError> {
        if self.controls.len() > MAX_SET_SIZE {
            return Err(StreamError::SizeTooLarge);
        }
        Ok(self.controls.clone())
    }

    fn with_control(
        &mut self,
        control_type: AccessControlType,
        stream_id: H256,
        account: Option<Address>,
        key: Option<Vec<u8>>,
    ) -> &mut Self {
        self.add_stream_id(stream_id);
        self.controls.push(AccessControl {
            control_type,
            stream_id,
            account,
            key,
        });
        self
    }

    // Access control methods
    pub fn grant_admin_role(&mut self, stream_id: H256, account: Address) -> &mut Self {
        self.with_control(
            AccessControlType::GrantAdminRole,
            stream_id,
            Some(account),
            None,
        )
    }

    pub fn renounce_admin_role(&mut self, stream_id: H256) -> &mut Self {
        self.with_control(AccessControlType::RenounceAdminRole, stream_id, None, None)
    }

    pub fn set_key_to_special(&mut self, stream_id: H256, key: Vec<u8>) -> &mut Self {
        self.with_control(
            AccessControlType::SetKeyToSpecial,
            stream_id,
            None,
            Some(key),
        )
    }
}

fn create_tags(stream_ids: &[H256]) -> Vec<u8> {
    // Pre-allocate capacity for the result vector
    let mut result = Vec::with_capacity(H256::len_bytes() * (1 + stream_ids.len()));

    // Add StreamDomain hash to result
    result.extend_from_slice(STREAM_DOMAIN.as_bytes());

    // Add each stream_id to result
    for stream_id in stream_ids {
        result.extend_from_slice(stream_id.as_bytes());
    }

    result
}
