use std::collections::VecDeque;

use crate::header::{Header, HistoryEntry};
use crate::record::SDSRecord;
use crate::storage::Storage;

pub struct PendingDelivery {
    pub header: Header,
    pub lamport_timestamp: u64,
    pub record: SDSRecord,
    pub retrieval_hint: Option<Vec<u8>>,
    pub received_at_ms: u64,
}

#[derive(Default)]
pub struct IncomingBuffer {
    pending: VecDeque<PendingDelivery>,
}

impl IncomingBuffer {
    pub fn contains(&self, message_id: &str) -> bool {
        self.pending
            .iter()
            .any(|delivery| delivery.header.message_id == message_id)
    }

    /// Returns the ID of the oldest message if it was dropped to stay within capacity.
    pub fn add(&mut self, delivery: PendingDelivery, capacity: usize) -> Option<String> {
        self.pending.push_back(delivery);
        if self.pending.len() <= capacity {
            return None;
        }
        self.pending
            .pop_front()
            .map(|dropped| dropped.header.message_id)
    }

    pub fn take_ready(&mut self, storage: &impl Storage) -> Option<PendingDelivery> {
        let index = self
            .pending
            .iter()
            .position(|delivery| has_dependencies(&delivery.header, storage))?;
        self.pending.remove(index)
    }

    /// Dependencies that are neither delivered nor waiting in this buffer.
    pub fn missing_dependencies(
        &self,
        header: &Header,
        storage: &impl Storage,
    ) -> Vec<HistoryEntry> {
        header
            .causal_history
            .iter()
            .filter(|entry| {
                !storage.contains(&entry.message_id) && !self.contains(&entry.message_id)
            })
            .cloned()
            .collect()
    }

    pub fn all_missing_dependencies(&self, storage: &impl Storage) -> Vec<HistoryEntry> {
        self.pending
            .iter()
            .flat_map(|delivery| self.missing_dependencies(&delivery.header, storage))
            .collect()
    }

    /// Returns the IDs of messages dropped for waiting longer than `timeout_ms`.
    pub fn expire(&mut self, now_ms: u64, timeout_ms: u64) -> Vec<String> {
        let mut expired = Vec::new();
        self.pending.retain(|delivery| {
            if now_ms < delivery.received_at_ms + timeout_ms {
                return true;
            }
            expired.push(delivery.header.message_id.clone());
            false
        });
        expired
    }
}

pub fn has_dependencies(header: &Header, storage: &impl Storage) -> bool {
    header
        .causal_history
        .iter()
        .all(|entry| storage.contains(&entry.message_id))
}
