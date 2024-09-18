use super::node::Node;
use super::proof::Proof;
use ethers::types::H256;
use std::sync::{Arc, Mutex};

pub struct Tree {
    pub root: Arc<Mutex<Node>>,
    pub leaf_nodes: Vec<Arc<Mutex<Node>>>,
}

impl Tree {
    pub fn root(&self) -> H256 {
        self.root.lock().unwrap().hash
    }

    pub fn proof_at(&self, i: usize) -> Proof {
        assert!(i < self.leaf_nodes.len(), "index out of bound");

        // only single root node
        if self.leaf_nodes.len() == 1 {
            return Proof {
                lemma: vec![self.root.lock().unwrap().hash],
                path: vec![],
            };
        }

        let mut proof = Proof {
            lemma: vec![self.leaf_nodes[i].lock().unwrap().hash],
            path: vec![],
        };

        let mut current = Arc::clone(&self.leaf_nodes[i]);
        loop {
            let parent = {
                let node = current.lock().unwrap();
                if let Some(ref parent) = node.parent {
                    Arc::clone(parent)
                } else {
                    break;
                }
            };

            let (is_left, sibling_hash) = {
                let parent_guard = parent.lock().unwrap();
                let is_left = Arc::ptr_eq(&current, parent_guard.left.as_ref().unwrap());
                let sibling_hash = if is_left {
                    parent_guard.right.as_ref().unwrap().lock().unwrap().hash
                } else {
                    parent_guard.left.as_ref().unwrap().lock().unwrap().hash
                };
                (is_left, sibling_hash)
            };

            proof.lemma.push(sibling_hash);
            proof.path.push(is_left);

            current = parent;
        }

        // append the root node hash
        proof.lemma.push(self.root.lock().unwrap().hash);

        proof
    }
}