use ethers::types::H256;
use tiny_keccak::{Hasher, Keccak};
use serde::{Serialize, Deserialize};
use anyhow::{Result, anyhow};

#[derive(Serialize, Deserialize, Debug)]
pub struct Proof {
    pub lemma: Vec<H256>,
    pub path: Vec<bool>,
}

impl Proof {
    pub fn validate_format(&self) -> Result<()> {
        let num_siblings = self.path.len();

        if num_siblings == 0 {
            if self.lemma.len() != 1 {
                return Err(anyhow!("invalid merkle proof format"));
            }
            return Ok(());
        }

        if num_siblings + 2 != self.lemma.len() {
            return Err(anyhow!("invalid merkle proof format"));
        }

        Ok(())
    }

    pub fn validate(&self, root: H256, content: &[u8], position: u64, num_leaf_nodes: u64) -> Result<()> {
        let mut hasher = Keccak::v256();
        hasher.update(content);
        let mut content_hash = [0u8; 32];
        hasher.finalize(&mut content_hash);
        let content_hash = H256::from(content_hash);

        self.validate_hash(root, content_hash, position, num_leaf_nodes)
    }

    pub fn validate_hash(&self, root: H256, content_hash: H256, position: u64, num_leaf_nodes: u64) -> Result<()> {
        self.validate_format()?;

        if content_hash != self.lemma[0] {
            return Err(anyhow!("merkle proof content mismatch"));
        }

        if self.lemma.len() > 1 && root != *self.lemma.last().unwrap() {
            return Err(anyhow!("merkle proof root mismatch"));
        }

        let proof_pos = self.calculate_proof_position(num_leaf_nodes);
        if proof_pos != position {
            return Err(anyhow!("merkle proof position mismatch"));
        }

        if !self.validate_root() {
            return Err(anyhow!("failed to validate merkle proof"));
        }

        Ok(())
    }

    fn calculate_proof_position(&self, mut num_leaf_nodes: u64) -> u64 {
        let mut position = 0;

        for (i, &is_left) in self.path.iter().rev().enumerate() {
            let left_side_depth = (num_leaf_nodes as f64).log2().ceil() as u64;
            let left_side_leaf_nodes = 2u64.pow(left_side_depth as u32) / 2;

            if is_left {
                num_leaf_nodes = left_side_leaf_nodes;
            } else {
                position += left_side_leaf_nodes;
                num_leaf_nodes -= left_side_leaf_nodes;
            }
        }

        position
    }

    fn validate_root(&self) -> bool {
        let mut hash = self.lemma[0];

        for (i, &is_left) in self.path.iter().enumerate() {
            let mut hasher = Keccak::v256();
            if is_left {
                hasher.update(hash.as_bytes());
                hasher.update(self.lemma[i + 1].as_bytes());
            } else {
                hasher.update(self.lemma[i + 1].as_bytes());
                hasher.update(hash.as_bytes());
            }
            let mut new_hash = [0u8; 32];
            hasher.finalize(&mut new_hash);
            hash = H256::from(new_hash);
        }

        hash == *self.lemma.last().unwrap()
    }
}