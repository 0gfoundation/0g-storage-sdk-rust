use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, rc::Rc};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShardConfig {
    pub shard_id: u64,
    pub num_shard: u64,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shard_id: 0,
            num_shard: 1,
        }
    }
}

impl ShardConfig {
    pub fn has_segment(&self, segment_index: u64) -> bool {
        self.num_shard < 2 || segment_index % self.num_shard == self.shard_id
    }

    pub fn is_valid(&self) -> bool {
        self.num_shard > 0
            && (self.num_shard & (self.num_shard - 1) == 0)
            && self.shard_id < self.num_shard
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardedNode {
    url: String,
    config: ShardConfig,
    latency: i64,
    since: i64,
}

struct ShardSegmentTreeNode {
    children: [Option<Box<ShardSegmentTreeNode>>; 2],
    num_shard: u32,
    lazy_tags: u32,
    replica: u32,
}

impl ShardSegmentTreeNode {
    fn new(num_shard: u32) -> Self {
        Self {
            children: [None, None],
            num_shard,
            lazy_tags: 0,
            replica: 0,
        }
    }

    fn pushdown(&mut self) {
        if self.children[0].is_none() {
            for i in 0..2 {
                self.children[i] = Some(Box::new(ShardSegmentTreeNode::new(self.num_shard << 1)));
            }
        }
        for child in self.children.iter_mut().filter_map(|c| c.as_mut()) {
            child.replica += self.lazy_tags;
            child.lazy_tags += self.lazy_tags;
        }
        self.lazy_tags = 0;
    }

    fn insert(&mut self, num_shard: u32, shard_id: u32, expected_replica: u32) -> bool {
        if self.replica >= expected_replica {
            return false;
        }
        if self.num_shard == num_shard {
            self.replica += 1;
            self.lazy_tags += 1;
            return true;
        }
        self.pushdown();
        let inserted = self.children[shard_id as usize % 2]
            .as_mut()
            .unwrap()
            .insert(num_shard, shard_id >> 1, expected_replica);
        self.replica = std::cmp::min(
            self.children[0].as_ref().unwrap().replica,
            self.children[1].as_ref().unwrap().replica,
        );
        inserted
    }
}

pub fn select(nodes: &mut [ShardedNode], expected_replica: u32, random: bool) -> (Vec<ShardedNode>, bool) {
    let mut selected = Vec::new();
    if expected_replica == 0 {
        return (selected, true);
    }

    if random {
        let mut rng = StdRng::from_entropy();
        nodes.shuffle(&mut rng);
    } else {
        nodes.sort_by(|a, b| {
            a.config
                .num_shard
                .cmp(&b.config.num_shard)
                .then_with(|| a.config.shard_id.cmp(&b.config.shard_id))
        });
    }

    let mut root = ShardSegmentTreeNode::new(1);

    for node in nodes.iter() {
        if root.insert(
            node.config.num_shard as u32,
            node.config.shard_id as u32,
            expected_replica,
        ) {
            selected.push(node.clone());
        }
        if root.replica >= expected_replica {
            return (selected, true);
        }
    }

    (Vec::new(), false)
}

pub fn check_replica(shard_configs: &[ShardConfig], expected_replica: u32) -> bool {
    let sharded_nodes: Vec<ShardedNode> = shard_configs
        .iter()
        .map(|config| ShardedNode {
            url: String::new(),
            config: config.clone(),
            latency: 0,
            since: 0,
        })
        .collect();

    return select(&mut sharded_nodes.clone(), expected_replica, false).1
}

#[cfg(test)]
mod tests {
    use super::*;
    use ctor::ctor;

    fn make_shard_node(num_shard: u64, shard_id: u64) -> ShardedNode {
        ShardedNode {
            url: String::new(),
            config: ShardConfig {
                num_shard,
                shard_id,
            },
            latency: 0,
            since: 0,
        }
    }

    #[test]
    fn test_select() {
        let mut sharded_nodes = vec![
            make_shard_node(4, 0),
            make_shard_node(4, 2),
            make_shard_node(4, 3),
            make_shard_node(1, 0),
            make_shard_node(2, 0),
            make_shard_node(8, 1),
            make_shard_node(8, 5),
            make_shard_node(16, 0),
            make_shard_node(16, 1),
            make_shard_node(16, 2),
            make_shard_node(16, 3),
            make_shard_node(16, 4),
            make_shard_node(16, 5),
            make_shard_node(16, 6),
            make_shard_node(16, 7),
            make_shard_node(16, 8),
            make_shard_node(16, 9),
            make_shard_node(16, 10),
            make_shard_node(16, 11),
            make_shard_node(16, 12),
            make_shard_node(16, 13),
            make_shard_node(16, 14),
            make_shard_node(16, 15),
        ];

        let selected = select(&mut sharded_nodes, 2, false);
        assert!(!selected.0.is_empty());
        log::debug!("{:?}", selected);

        assert_eq!(selected.0.len(), 5);
        assert_eq!(selected.0[0], make_shard_node(1, 0));
        assert_eq!(selected.0[1], make_shard_node(2, 0));
        assert_eq!(selected.0[2], make_shard_node(4, 3));
        assert_eq!(selected.0[3], make_shard_node(8, 1));
        assert_eq!(selected.0[4], make_shard_node(8, 5));

        let selected = select(&mut sharded_nodes, 3, false);
        log::debug!("{:?}", selected);
        assert!(!selected.0.is_empty());
        assert_eq!(selected.0.len(), 15);

        let selected = select(&mut sharded_nodes, 4, false);
        log::debug!("{:?}", selected);
        assert!(!selected.1);
    }
}
