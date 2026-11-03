use std::collections::VecDeque;

use crate::proto::HistoryEntry;

struct MissingMessage {
    entry: HistoryEntry,
    named_at_ms: u64,
}

/// Dependencies of delivered messages that have not arrived, oldest first.
#[derive(Default)]
pub struct MissingMessages {
    pending: VecDeque<MissingMessage>,
}

impl MissingMessages {
    /// Returns the IDs of the oldest entries dropped to stay within capacity.
    pub fn add(&mut self, entries: &[HistoryEntry], now_ms: u64, capacity: usize) -> Vec<String> {
        for entry in entries {
            // Named again, it restarts its wait, as a newly held message would.
            self.remove(&entry.message_id);
            self.pending.push_back(MissingMessage {
                entry: entry.clone(),
                named_at_ms: now_ms,
            });
        }
        let excess = self.pending.len().saturating_sub(capacity);
        self.pending
            .drain(..excess)
            .map(|missing| missing.entry.message_id)
            .collect()
    }

    pub fn remove(&mut self, message_id: &str) {
        self.pending
            .retain(|missing| missing.entry.message_id != message_id);
    }

    pub fn entries(&self) -> Vec<HistoryEntry> {
        self.pending
            .iter()
            .map(|missing| missing.entry.clone())
            .collect()
    }

    /// Returns the IDs of messages missing for longer than `timeout_ms`.
    pub fn expire(&mut self, now_ms: u64, timeout_ms: u64) -> Vec<String> {
        let mut expired = Vec::new();
        self.pending.retain(|missing| {
            if now_ms < missing.named_at_ms + timeout_ms {
                return true;
            }
            expired.push(missing.entry.message_id.clone());
            false
        });
        expired
    }
}
