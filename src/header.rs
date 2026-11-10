/// A message a header names, in its causal history or a repair request.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryEntry {
    pub message_id: String,
    pub retrieval_hint: Option<Vec<u8>>,
    pub sender_id: Option<String>,
}

/// The spec's `Message` without its content.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Header {
    pub sender_id: String,
    pub message_id: String,
    pub channel_id: String,
    pub lamport_timestamp: Option<u64>,
    pub causal_history: Vec<HistoryEntry>,
    pub bloom_filter: Option<Vec<u8>>,
    pub repair_request: Vec<HistoryEntry>,
}
