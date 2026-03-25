use sha2::{Digest, Sha256};
use std::sync::OnceLock;

pub fn combine_hashes(left: &str, right: &str) -> String {
    let combined = format!("{left}{right}");
    let digest = Sha256::digest(combined.as_bytes());
    hex::encode(digest)
}

pub fn compute_merkle_root(hashes: &[String]) -> String {
    if hashes.is_empty() {
        return empty_root();
    }

    let mut layer: Vec<String> = hashes.to_vec();

    while layer.len() > 1 {
        let mut next = Vec::new();
        for chunk in layer.chunks(2) {
            let right = chunk.get(1).unwrap_or(&chunk[0]);
            next.push(combine_hashes(&chunk[0], right));
        }
        layer = next;
    }

    layer.first().cloned().unwrap_or_else(empty_root)
}

#[derive(Debug, Clone, Default)]
pub struct IncrementalMerkleTree {
    layers: Vec<Vec<String>>,
}

impl IncrementalMerkleTree {
    pub fn from_hashes(hashes: &[String]) -> Self {
        let mut tree = Self::default();
        for hash in hashes {
            tree.push(hash.clone());
        }
        tree
    }

    pub fn push(&mut self, hash: String) -> String {
        if self.layers.is_empty() {
            self.layers.push(Vec::new());
        }
        self.layers[0].push(hash);

        if self.layers[0].len() == 1 && self.layers.len() == 1 {
            return self.layers[0][0].clone();
        }

        let mut level = 0;
        loop {
            let child_layer_len = self.layers[level].len();
            let parent_index = (child_layer_len - 1) / 2;
            let left_index = parent_index * 2;
            let left = self.layers[level][left_index].clone();
            let right = self.layers[level]
                .get(left_index + 1)
                .cloned()
                .unwrap_or_else(|| left.clone());
            let parent = combine_hashes(&left, &right);

            if self.layers.len() == level + 1 {
                self.layers.push(Vec::new());
            }

            if parent_index < self.layers[level + 1].len() {
                self.layers[level + 1][parent_index] = parent;
            } else {
                self.layers[level + 1].push(parent);
            }

            if level + 1 == self.layers.len() - 1 && self.layers[level + 1].len() == 1 {
                break;
            }

            level += 1;
        }

        self.root()
    }

    pub fn root(&self) -> String {
        self.layers
            .last()
            .and_then(|layer| layer.first())
            .cloned()
            .unwrap_or_else(empty_root)
    }
}

pub fn empty_root() -> String {
    static EMPTY: OnceLock<String> = OnceLock::new();
    EMPTY
        .get_or_init(|| hex::encode(Sha256::digest(&[])))
        .clone()
}
