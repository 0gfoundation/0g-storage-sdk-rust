use anyhow::Result;
use clap::Args;
use ethers::types::H256;
use std::time::Duration;
use std::collections::HashMap;
use crate::common::utils::duration_from_str;
use crate::node::client_kv::KvClient;
use crate::node::types::{ValueSegment, Segment};
use crate::kv::builder::MAX_QUERY_SIZE;
use crate::common::utils::pad_to_32_bytes;

#[derive(Args)]
pub struct KvReadArgs {
    #[arg(
        long,
        default_value = "0x00",
        help = "Stream to read/write",
        required = true
    )]
    pub stream_id: String,

    #[arg(long, help = "KV keys", value_delimiter = ',', required = true)]
    pub stream_keys: Vec<String>,

    #[arg(long, default_value = "18446744073709551615", help = "Key version")]
    pub version: u64,

    #[arg(long, help = "KV node url", required = true)]
    pub node: String,

    #[arg(long, default_value = "0", value_parser = duration_from_str, help = "Cli task timeout, 0 for no timeout")]
    pub timeout: Duration,
}

pub async fn run_kv_read(args: &KvReadArgs) -> Result<String> {
    let kv_client = KvClient::new(&args.node)?;
    let stream_id = H256::from_slice(pad_to_32_bytes(&args.stream_id.trim_start_matches("0x"))?.as_slice());
    let mut result = HashMap::new(); 

    for key in args.stream_keys.iter() {
        let mut val = ValueSegment {
            version: args.version,
            data: vec![],
            size: 0,
        };

        loop {
            let seg = kv_client.get_value(
                stream_id,
                Segment(key.as_bytes().to_vec()),
                val.data.len() as u64,
                MAX_QUERY_SIZE as u64, 
                Some(val.version),
            ).await?;
            println!("seg: {:?}", seg);
            if val.version == u64::MAX {
                val.version = seg.version;
            } else if val.version != seg.version {
                val.version = seg.version;
                val.data.clear();
            }
    
            val.size = seg.size;
            val.data.extend(seg.data);
            if val.data.len() as u64 == val.size as u64 {
                result.insert(key.to_string(), val.data);
                break;
            }
        }
    }
    Ok(serde_json::to_string(&result)?)
}
