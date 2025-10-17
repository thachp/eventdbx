use sha2::{Digest, Sha256};
use std::sync::OnceLock;

pub fn compute_merkle_root(hashes: &[String]) -> String {
    if hashes.is_empty() {
        return empty_root();
    }

    let mut layer: Vec<String> = hashes.to_vec();

    while layer.len() > 1 {
        let mut next = Vec::new();
        for chunk in layer.chunks(2) {
            let combined = if chunk.len() == 1 {
                format!("{}{}", chunk[0], chunk[0])
            } else {
                format!("{}{}", chunk[0], chunk[1])
            };
            let digest = Sha256::digest(combined.as_bytes());
            next.push(hex::encode(digest));
        }
        layer = next;
    }

    layer.first().cloned().unwrap_or_else(empty_root)
}

pub fn empty_root() -> String {
    static EMPTY: OnceLock<String> = OnceLock::new();
    EMPTY
        .get_or_init(|| hex::encode(Sha256::digest(&[])))
        .clone()
}
