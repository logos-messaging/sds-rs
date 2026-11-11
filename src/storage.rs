use std::collections::{BTreeMap, HashMap};

use crate::record::SDSRecord;

#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub message_id: String,
    pub lamport_timestamp: u64,
    pub sender_id: String,
    pub retrieval_hint: Option<Vec<u8>>,
    /// The record as sent or received, kept only when this participant may answer repair requests.
    pub record: Option<SDSRecord>,
}

/// The local log, ordered by lamport timestamp then message ID.
pub trait Storage {
    fn insert(&mut self, entry: LogEntry);
    fn contains(&self, message_id: &str) -> bool;
    fn get(&self, message_id: &str) -> Option<LogEntry>;
    /// Returns false when `message_id` is not in the log.
    fn set_retrieval_hint(&mut self, message_id: &str, retrieval_hint: Vec<u8>) -> bool;
    /// The newest `count` entries, oldest first.
    fn latest(&self, count: usize) -> Vec<LogEntry>;
    fn message_count(&self) -> usize;
    fn remove_oldest(&mut self) -> Option<LogEntry>;
}

#[derive(Debug, Default)]
pub struct InMemoryStorage {
    ordered: BTreeMap<(u64, String), LogEntry>,
    lamport_by_id: HashMap<String, u64>,
}

impl Storage for InMemoryStorage {
    fn insert(&mut self, entry: LogEntry) {
        if self.contains(&entry.message_id) {
            return;
        }
        let key = (entry.lamport_timestamp, entry.message_id.clone());
        self.lamport_by_id
            .insert(entry.message_id.clone(), entry.lamport_timestamp);
        self.ordered.insert(key, entry);
    }

    fn contains(&self, message_id: &str) -> bool {
        self.lamport_by_id.contains_key(message_id)
    }

    fn get(&self, message_id: &str) -> Option<LogEntry> {
        let lamport_timestamp = *self.lamport_by_id.get(message_id)?;
        self.ordered
            .get(&(lamport_timestamp, message_id.to_string()))
            .cloned()
    }

    fn set_retrieval_hint(&mut self, message_id: &str, retrieval_hint: Vec<u8>) -> bool {
        let Some(&lamport_timestamp) = self.lamport_by_id.get(message_id) else {
            return false;
        };
        let Some(entry) = self
            .ordered
            .get_mut(&(lamport_timestamp, message_id.to_string()))
        else {
            return false;
        };
        entry.retrieval_hint = Some(retrieval_hint);
        true
    }

    fn latest(&self, count: usize) -> Vec<LogEntry> {
        let mut entries: Vec<LogEntry> = self.ordered.values().rev().take(count).cloned().collect();
        entries.reverse();
        entries
    }

    fn message_count(&self) -> usize {
        self.ordered.len()
    }

    fn remove_oldest(&mut self) -> Option<LogEntry> {
        let (_, entry) = self.ordered.pop_first()?;
        self.lamport_by_id.remove(&entry.message_id);
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(message_id: &str, lamport_timestamp: u64) -> LogEntry {
        LogEntry {
            message_id: message_id.into(),
            lamport_timestamp,
            sender_id: "alice".into(),
            retrieval_hint: None,
            record: None,
        }
    }

    fn ids(entries: Vec<LogEntry>) -> Vec<String> {
        entries.into_iter().map(|entry| entry.message_id).collect()
    }

    #[test]
    fn orders_by_lamport_then_message_id() {
        let mut storage = InMemoryStorage::default();
        storage.insert(entry("c", 2));
        storage.insert(entry("b", 1));
        storage.insert(entry("a", 2));
        assert_eq!(ids(storage.latest(3)), ["b", "a", "c"]);
        assert_eq!(ids(storage.latest(2)), ["a", "c"]);
    }

    #[test]
    fn removes_oldest_entry() {
        let mut storage = InMemoryStorage::default();
        storage.insert(entry("b", 2));
        storage.insert(entry("a", 1));
        assert_eq!(storage.remove_oldest().unwrap().message_id, "a");
        assert!(!storage.contains("a"));
        assert_eq!(storage.message_count(), 1);
    }

    #[test]
    fn ignores_duplicate_inserts() {
        let mut storage = InMemoryStorage::default();
        storage.insert(entry("a", 1));
        storage.insert(entry("a", 5));
        assert_eq!(storage.get("a").unwrap().lamport_timestamp, 1);
        assert_eq!(storage.message_count(), 1);
    }

    #[test]
    fn sets_the_retrieval_hint_of_a_logged_entry() {
        let mut storage = InMemoryStorage::default();
        storage.insert(entry("a", 1));
        assert!(storage.set_retrieval_hint("a", b"h".to_vec()));
        assert!(!storage.set_retrieval_hint("b", b"h".to_vec()));
        assert_eq!(
            storage.get("a").unwrap().retrieval_hint,
            Some(b"h".to_vec())
        );
    }
}
