use std::collections::VecDeque;
use std::f64::consts::LN_2;

use crate::hash::bloom_hashes;

#[derive(Clone, Debug)]
pub struct BloomFilter {
    bits: Vec<u8>,
    hash_count: u32,
}

impl BloomFilter {
    pub fn with_capacity(capacity: usize, error_rate: f64) -> Self {
        Self {
            bits: vec![0; byte_count_for(capacity, error_rate)],
            hash_count: hash_count_for(capacity, error_rate),
        }
    }

    pub fn from_bytes(bits: Vec<u8>, hash_count: u32) -> Self {
        Self { bits, hash_count }
    }

    pub fn insert(&mut self, message_id: &str) {
        for index in self.bit_indexes(message_id) {
            self.bits[index / 8] |= 1 << (index % 8);
        }
    }

    pub fn contains(&self, message_id: &str) -> bool {
        if self.bits.is_empty() {
            return false;
        }
        self.bit_indexes(message_id)
            .all(|index| self.bits[index / 8] & (1 << (index % 8)) != 0)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    fn bit_indexes(&self, message_id: &str) -> impl Iterator<Item = usize> {
        let (h1, h2) = bloom_hashes(message_id);
        let bit_count = (self.bits.len() * 8) as u64;
        (0..u64::from(self.hash_count))
            .map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % bit_count) as usize)
    }
}

/// Peers decode each other's filters with this, so it must depend only on shared config.
pub fn hash_count_for(capacity: usize, error_rate: f64) -> u32 {
    let bit_count = byte_count_for(capacity, error_rate) * 8;
    (bit_count as f64 / capacity as f64 * LN_2).round().max(1.0) as u32
}

fn byte_count_for(capacity: usize, error_rate: f64) -> usize {
    let bit_count = (-(capacity as f64) * error_rate.ln() / (LN_2 * LN_2)).ceil() as usize;
    bit_count.div_ceil(8)
}

#[derive(Clone, Debug)]
pub struct RollingBloomFilter {
    filter: BloomFilter,
    recent_ids: VecDeque<String>,
    capacity: usize,
    error_rate: f64,
}

impl RollingBloomFilter {
    pub fn new(capacity: usize, error_rate: f64) -> Self {
        Self {
            filter: BloomFilter::with_capacity(capacity, error_rate),
            recent_ids: VecDeque::with_capacity(capacity),
            capacity,
            error_rate,
        }
    }

    pub fn insert(&mut self, message_id: &str) {
        if self.recent_ids.len() >= self.capacity {
            self.roll();
        }
        self.recent_ids.push_back(message_id.to_string());
        self.filter.insert(message_id);
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.filter.as_bytes()
    }

    // Keeping the newest half means peers still see recent acks right after a roll.
    fn roll(&mut self) {
        while self.recent_ids.len() > self.capacity / 2 {
            self.recent_ids.pop_front();
        }
        self.filter = BloomFilter::with_capacity(self.capacity, self.error_rate);
        for message_id in &self.recent_ids {
            self.filter.insert(message_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_default_filter() {
        let filter = BloomFilter::with_capacity(200, 0.001);
        assert_eq!(filter.as_bytes().len(), 360);
        assert_eq!(filter.hash_count, 10);
        assert_eq!(hash_count_for(200, 0.001), 10);
    }

    #[test]
    fn contains_inserted_ids() {
        let mut filter = BloomFilter::with_capacity(200, 0.001);
        filter.insert("msg-1");
        assert!(filter.contains("msg-1"));
        assert!(!filter.contains("msg-2"));
    }

    #[test]
    fn decodes_from_bytes() {
        let mut filter = BloomFilter::with_capacity(200, 0.001);
        filter.insert("msg-1");
        let decoded = BloomFilter::from_bytes(filter.as_bytes().to_vec(), 10);
        assert!(decoded.contains("msg-1"));
    }

    #[test]
    fn empty_filter_contains_nothing() {
        assert!(!BloomFilter::from_bytes(Vec::new(), 10).contains("msg-1"));
    }

    #[test]
    fn rolling_filter_keeps_newest_half() {
        let mut filter = RollingBloomFilter::new(4, 0.001);
        for id in ["a", "b", "c", "d", "e"] {
            filter.insert(id);
        }
        let snapshot =
            BloomFilter::from_bytes(filter.as_bytes().to_vec(), hash_count_for(4, 0.001));
        assert!(!snapshot.contains("a"));
        assert!(snapshot.contains("c"));
        assert!(snapshot.contains("e"));
    }
}
