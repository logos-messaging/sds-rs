use std::collections::VecDeque;

use crate::packet::Packet;
use crate::proto::{HistoryEntry, Message};
use crate::storage::Storage;

pub struct PendingDelivery {
    pub message: Message,
    pub lamport_timestamp: u64,
    pub packet: Packet,
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
            .any(|delivery| delivery.message.message_id == message_id)
    }

    /// Returns the ID of the oldest message if it was dropped to stay within capacity.
    pub fn add(&mut self, delivery: PendingDelivery, capacity: usize) -> Option<String> {
        self.pending.push_back(delivery);
        if self.pending.len() <= capacity {
            return None;
        }
        self.pending
            .pop_front()
            .map(|dropped| dropped.message.message_id)
    }

    pub fn take_ready(&mut self, storage: &impl Storage) -> Option<PendingDelivery> {
        let index = self
            .pending
            .iter()
            .position(|delivery| has_dependencies(&delivery.message, storage))?;
        self.pending.remove(index)
    }

    /// Dependencies that are neither delivered nor waiting in this buffer.
    pub fn missing_dependencies(
        &self,
        message: &Message,
        storage: &impl Storage,
    ) -> Vec<HistoryEntry> {
        message
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
            .flat_map(|delivery| self.missing_dependencies(&delivery.message, storage))
            .collect()
    }

    /// Returns the IDs of messages dropped for waiting longer than `timeout_ms`.
    pub fn expire(&mut self, now_ms: u64, timeout_ms: u64) -> Vec<String> {
        let mut expired = Vec::new();
        self.pending.retain(|delivery| {
            if now_ms < delivery.received_at_ms + timeout_ms {
                return true;
            }
            expired.push(delivery.message.message_id.clone());
            false
        });
        expired
    }
}

pub fn has_dependencies(message: &Message, storage: &impl Storage) -> bool {
    message
        .causal_history
        .iter()
        .all(|entry| storage.contains(&entry.message_id))
}
