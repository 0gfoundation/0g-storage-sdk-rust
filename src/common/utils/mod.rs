pub mod schedule;

use anyhow::Result;
use hex;
use std::str::FromStr;
use std::time::Duration;

pub fn duration_from_str(s: &str) -> Result<Duration, std::num::ParseIntError> {
    let secs = u64::from_str(s)?;
    Ok(Duration::from_secs(secs))
}

pub fn max_u64() -> u64 {
    u64::MAX
}

pub fn pad_to_32_bytes(hex_str: &str) -> Result<Vec<u8>> {
    let mut bytes = hex::decode(hex_str)?;

    // 确保字节长度为32字节
    while bytes.len() < 32 {
        bytes.insert(0, 0);
    }

    Ok(bytes)
}
