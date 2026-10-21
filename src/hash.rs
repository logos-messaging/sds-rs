use sha2::{Digest, Sha256};

/// SHA-256 over length-prefixed fields, truncated to a big-endian u64.
pub fn hash_fields(fields: &[&str]) -> u64 {
    let mut hasher = Sha256::new();
    for field in fields {
        hasher.update((field.len() as u32).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    read_word(&hasher.finalize(), 0)
}

/// The two base hashes used for Kirsch–Mitzenmacher double hashing in the bloom filter.
pub fn bloom_hashes(message_id: &str) -> (u64, u64) {
    let digest = Sha256::digest(message_id.as_bytes());
    (read_word(&digest, 0), read_word(&digest, 8))
}

fn read_word(digest: &[u8], offset: usize) -> u64 {
    let word = digest[offset..offset + 8]
        .try_into()
        .expect("SHA-256 digest is 32 bytes");
    u64::from_be_bytes(word)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_prefix_separates_fields() {
        assert_ne!(hash_fields(&["ab", "c"]), hash_fields(&["a", "bc"]));
    }

    #[test]
    fn matches_test_vectors() {
        assert_eq!(hash_fields(&["alice"]), 0xe3e40cc57896dcda);
        assert_eq!(hash_fields(&["alice", "msg-1"]), 0x653f909d8e44fca9);
        assert_eq!(
            bloom_hashes("msg-1"),
            (0x3a0de37932e8b197, 0x46f20b22414f862f)
        );
    }
}
