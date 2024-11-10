use ethers::types::{Address, H256, U256};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::IpAddr;

use crate::core::merkle::proof::Proof;
use crate::common::shard::ShardConfig;

// NetworkProtocolVersion: P2P network protocol version.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolVersion {
    pub major: u8,
    pub minor: u8,
    pub build: u8,
}

// NetworkIdentity: Network identity of 0g storage node to distinguish different networks.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NetworkIdentity {
    pub chain_id: u64,
    pub flow_address: Address,
    pub p2p_protocol_version: ProtocolVersion,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub connected_peers: u32,
    pub log_sync_height: u64,
    pub log_sync_block: H256,
    pub network_identity: NetworkIdentity,
}

// Transaction: On-chain transaction about a file.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub stream_ids: Vec<U256>,
    pub data: Vec<u8>,
    pub data_merkle_root: H256,
    pub start_entry_index: u64,
    pub size: u64,
    pub seq: u64,
}

// FileInfo: Information about a file responded from 0g storage node.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FileInfo {
    pub tx: Transaction,
    pub finalized: bool,
    pub is_cached: bool,
    pub uploaded_seg_num: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SegmentWithProof {
    /// File merkle root.
    pub root: H256,
    #[serde(with = "base64")]
    /// With fixed data size except the last segment.
    pub data: Vec<u8>,
    /// Segment index.
    pub index: usize,
    /// File merkle proof whose leaf node is segment root.
    pub proof: Proof,
    /// File size
    pub file_size: usize,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Segment(#[serde(with = "base64")] pub Vec<u8>);

mod base64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    pub version: String,
    pub os: String,
    pub protocol: String,
    pub agent: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PeerConnectionStatus {
    pub status: String,
    pub connections_in: u8,
    pub connections_out: u8,
    pub last_seen_secs: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfo {
    pub client: Client,
    pub connection_status: PeerConnectionStatus,
    pub listening_addresses: Vec<Vec<u8>>,
    pub seen_ips: HashSet<IpAddr>,
    pub is_trusted: bool,
    pub connection_direction: Option<String>, // Incoming/Outgoing
    pub enr: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocationInfo {
    pub ip: IpAddr,
    pub shard_config: ShardConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValueSegment {
    // key version
    pub version: u64,
    // data
    #[serde(with = "base64")]
    pub data: Vec<u8>,
    // value total size
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValueSegment {
    // key version
    pub version: u64,
    // key
    #[serde(with = "base64")]
    pub key: Vec<u8>,
    // data
    #[serde(with = "base64")]
    pub data: Vec<u8>,
    // value total size
    pub size: u64,
}