use ethers::types::{Address, H256, U256};
use serde::{Deserialize, Serialize};

// NetworkProtocolVersion: P2P network protocol version.
#[derive(Serialize, Deserialize, Debug)]
pub struct ProtocolVersion {
    pub major: u8,
    pub minor: u8,
    pub build: u8,
}

// NetworkIdentity: Network identity of 0g storage node to distinguish different networks.
#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkIdentity {
    pub chainId: u64,
    pub flowAddress: Address,
    pub p2pProtocolVersion: ProtocolVersion,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Status {
    pub connectedPeers: u32,
    pub logSyncHeight: u64,
    pub logSyncBlock: H256,
    pub networkIdentity: NetworkIdentity,
}

// Transaction: On-chain transaction about a file.
#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub streamIds: Vec<U256>,
    pub data: Vec<u8>,
    pub dataMerkleRoot: H256,
    pub startEntryIndex: u64,
    pub size: u64,
    pub seq: u64,
}

// FileInfo: Information about a file responded from 0g storage node.
#[derive(Serialize, Deserialize, Debug)]
pub struct FileInfo {
    pub tx: Transaction,
    pub finalized: bool,
    pub isCached: bool,
    pub uploadedSegNum: u64,
}
