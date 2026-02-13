use super::node::Node;
use super::tree::Tree;
use ethers::types::H256;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct TreeBuilder {
    leaf_nodes: Vec<Arc<Mutex<Node>>>,
}

impl Default for TreeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TreeBuilder {
    pub fn new() -> Self {
        TreeBuilder {
            leaf_nodes: Vec::new(),
        }
    }

    pub fn append(&mut self, content: &[u8]) {
        let node = Node::new_leaf(content);
        self.leaf_nodes.push(node);
    }

    pub fn append_hash(&mut self, hash: H256) {
        let node = Node::new(hash);
        self.leaf_nodes.push(node);
    }

    pub fn build(&self) -> Option<Tree> {
        let num_leaf_nodes = self.leaf_nodes.len();
        if num_leaf_nodes == 0 {
            return None;
        }

        let mut queue: VecDeque<Arc<Mutex<Node>>> = VecDeque::new();

        for chunk in self.leaf_nodes.chunks(2) {
            match chunk {
                [left, right] => {
                    let node = Node::new_interior(Arc::clone(left), Arc::clone(right));
                    queue.push_back(node);
                }
                [single] => {
                    queue.push_back(Arc::clone(single));
                }
                _ => unreachable!(),
            }
        }

        while queue.len() > 1 {
            let mut new_queue = VecDeque::new();

            while let Some(left) = queue.pop_front() {
                if let Some(right) = queue.pop_front() {
                    let node = Node::new_interior(left, right);
                    new_queue.push_back(node);
                } else {
                    new_queue.push_back(left);
                }
            }

            queue = new_queue;
        }

        Some(Tree {
            root: queue.pop_front().unwrap(),
            leaf_nodes: self.leaf_nodes.clone(),
        })
    }
}
