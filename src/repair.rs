use std::collections::HashMap;

use crate::config::RepairConfig;
use crate::hash::hash_fields;
use crate::proto::HistoryEntry;
use crate::storage::Storage;

struct PendingRequest {
    entry: HistoryEntry,
    request_at_ms: u64,
}

/// SDS-R state: missing entries we will ask for, and requests from others we may answer.
pub struct Repair {
    config: RepairConfig,
    participant_id: String,
    requests: HashMap<String, PendingRequest>,
    responses: HashMap<String, u64>,
}

impl Repair {
    pub fn new(config: RepairConfig, participant_id: String) -> Self {
        Self {
            config,
            participant_id,
            requests: HashMap::new(),
            responses: HashMap::new(),
        }
    }

    pub fn on_message_received(&mut self, message_id: &str) {
        self.requests.remove(message_id);
        self.responses.remove(message_id);
    }

    pub fn request_missing(&mut self, missing: &[HistoryEntry], now_ms: u64) {
        for entry in missing {
            let request_at_ms = now_ms + self.request_delay_ms(&entry.message_id);
            self.requests
                .entry(entry.message_id.clone())
                .or_insert_with(|| PendingRequest {
                    entry: entry.clone(),
                    request_at_ms,
                });
        }
    }

    pub fn has_due_requests(&self, now_ms: u64) -> bool {
        self.requests
            .values()
            .any(|request| request.request_at_ms <= now_ms)
    }

    /// Removes and returns the most overdue requests, up to the configured maximum.
    pub fn take_due_requests(&mut self, now_ms: u64) -> Vec<HistoryEntry> {
        let mut due: Vec<(u64, String)> = self
            .requests
            .values()
            .filter(|request| request.request_at_ms <= now_ms)
            .map(|request| (request.request_at_ms, request.entry.message_id.clone()))
            .collect();
        due.sort();
        due.truncate(self.config.max_repair_requests);
        due.into_iter()
            .filter_map(|(_, message_id)| self.requests.remove(&message_id))
            .map(|request| request.entry)
            .collect()
    }

    pub fn on_repair_requests(
        &mut self,
        requests: &[HistoryEntry],
        storage: &impl Storage,
        now_ms: u64,
    ) {
        for request in requests {
            self.requests.remove(&request.message_id);
            self.schedule_response(request, storage, now_ms);
        }
    }

    /// Removes and returns the IDs of messages now due for rebroadcast.
    pub fn take_due_responses(&mut self, now_ms: u64) -> Vec<String> {
        let due: Vec<String> = self
            .responses
            .iter()
            .filter(|(_, respond_at_ms)| **respond_at_ms <= now_ms)
            .map(|(message_id, _)| message_id.clone())
            .collect();
        for message_id in &due {
            self.responses.remove(message_id);
        }
        due
    }

    pub fn is_responder(&self, sender_id: &str, message_id: &str) -> bool {
        let groups = self.config.num_response_groups;
        let own_group = hash_fields(&[&self.participant_id, message_id]) % groups;
        own_group == hash_fields(&[sender_id, message_id]) % groups
    }

    fn schedule_response(&mut self, request: &HistoryEntry, storage: &impl Storage, now_ms: u64) {
        let Some(sender_id) = &request.sender_id else {
            return;
        };
        let has_cached_message = storage
            .get(&request.message_id)
            .is_some_and(|entry| entry.packet.is_some());
        if !has_cached_message || !self.is_responder(sender_id, &request.message_id) {
            return;
        }
        let respond_at_ms = now_ms + self.response_delay_ms(sender_id, &request.message_id);
        self.responses
            .entry(request.message_id.clone())
            .or_insert(respond_at_ms);
    }

    fn request_delay_ms(&self, message_id: &str) -> u64 {
        let window_ms = self.config.t_max_ms - self.config.t_min_ms;
        hash_fields(&[&self.participant_id, message_id]) % window_ms + self.config.t_min_ms
    }

    // The original sender has distance 0, so it answers first.
    fn response_delay_ms(&self, sender_id: &str, message_id: &str) -> u64 {
        let distance = hash_fields(&[&self.participant_id]) ^ hash_fields(&[sender_id]);
        distance.wrapping_mul(hash_fields(&[message_id])) % self.config.t_max_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repair(participant_id: &str) -> Repair {
        Repair::new(RepairConfig::default(), participant_id.into())
    }

    fn entry(message_id: &str) -> HistoryEntry {
        HistoryEntry {
            message_id: message_id.into(),
            retrieval_hint: None,
            sender_id: Some("alice".into()),
        }
    }

    #[test]
    fn request_delay_is_within_window() {
        let repair = repair("bob");
        let config = RepairConfig::default();
        for i in 0..100 {
            let delay = repair.request_delay_ms(&format!("msg-{i}"));
            assert!((config.t_min_ms..config.t_max_ms).contains(&delay));
        }
    }

    #[test]
    fn original_sender_responds_immediately() {
        assert_eq!(repair("alice").response_delay_ms("alice", "msg-1"), 0);
    }

    #[test]
    fn sender_is_always_in_own_response_group() {
        let mut repair = repair("alice");
        repair.config.num_response_groups = 7;
        for i in 0..20 {
            assert!(repair.is_responder("alice", &format!("msg-{i}")));
        }
    }

    #[test]
    fn takes_only_due_requests_up_to_max() {
        let mut repair = repair("bob");
        let missing: Vec<HistoryEntry> = (0..5).map(|i| entry(&format!("msg-{i}"))).collect();
        repair.request_missing(&missing, 0);
        assert!(repair.take_due_requests(0).is_empty());

        let due = repair.take_due_requests(RepairConfig::default().t_max_ms);
        assert_eq!(due.len(), 3);
        assert_eq!(repair.take_due_requests(u64::MAX).len(), 2);
    }

    #[test]
    fn receiving_message_cancels_request() {
        let mut repair = repair("bob");
        repair.request_missing(&[entry("msg-1")], 0);
        repair.on_message_received("msg-1");
        assert!(!repair.has_due_requests(u64::MAX));
    }
}
