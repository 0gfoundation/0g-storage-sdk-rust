use ethers::types::H256;
use std::sync::{Arc, Mutex};
use tiny_keccak::{Hasher, Keccak};

pub struct Node {
    pub parent: Option<Arc<Mutex<Node>>>,
    pub left: Option<Arc<Mutex<Node>>>,
    pub right: Option<Arc<Mutex<Node>>>,
    pub hash: H256,
}

impl Node {
    pub fn new(hash: H256) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Node {
            parent: None,
            left: None,
            right: None,
            hash,
        }))
    }

    pub fn new_leaf(content: &[u8]) -> Arc<Mutex<Self>> {
        let mut hasher = Keccak::v256();
        hasher.update(content);
        let mut hash = [0u8; 32];
        hasher.finalize(&mut hash);

        Node::new(H256::from(hash))
    }

    pub fn new_interior(left: Arc<Mutex<Node>>, right: Arc<Mutex<Node>>) -> Arc<Mutex<Self>> {
        let mut hasher = Keccak::v256();
        hasher.update(left.lock().unwrap().hash.as_bytes());
        hasher.update(right.lock().unwrap().hash.as_bytes());
        let mut hash = [0u8; 32];
        hasher.finalize(&mut hash);

        let node = Node::new(H256::from(hash));

        {
            let mut node_guard = node.lock().unwrap();
            node_guard.left = Some(left.clone());
            node_guard.right = Some(right.clone());
        }

        // Set parent for left and right nodes
        left.lock().unwrap().parent = Some(node.clone());
        right.lock().unwrap().parent = Some(node.clone());

        node
    }

    pub fn is_left_side(&self) -> bool {
        if let Some(ref parent) = self.parent {
            if let Some(ref left) = parent.lock().unwrap().left {
                return Arc::ptr_eq(left, &Arc::new(Mutex::new(self.clone())));
            }
        }
        false
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            parent: self.parent.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
            hash: self.hash,
        }
    }
}
