#[derive(Clone, PartialEq, prost::Message)]
pub struct HistoryEntry {
    #[prost(string, tag = "1")]
    pub message_id: String,
    #[prost(bytes = "vec", optional, tag = "2")]
    pub retrieval_hint: Option<Vec<u8>>,
    #[prost(string, optional, tag = "3")]
    pub sender_id: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Message {
    #[prost(string, tag = "1")]
    pub sender_id: String,
    #[prost(string, tag = "2")]
    pub message_id: String,
    #[prost(string, tag = "3")]
    pub channel_id: String,
    #[prost(uint64, optional, tag = "10")]
    pub lamport_timestamp: Option<u64>,
    #[prost(message, repeated, tag = "11")]
    pub causal_history: Vec<HistoryEntry>,
    #[prost(bytes = "vec", optional, tag = "12")]
    pub bloom_filter: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "13")]
    pub repair_request: Vec<HistoryEntry>,
    #[prost(bytes = "vec", optional, tag = "20")]
    pub content: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message as _;

    #[test]
    fn encodes_with_spec_field_numbers() {
        let message = Message {
            sender_id: "a".into(),
            message_id: "b".into(),
            channel_id: "c".into(),
            lamport_timestamp: Some(1),
            causal_history: vec![HistoryEntry {
                message_id: "d".into(),
                retrieval_hint: None,
                sender_id: None,
            }],
            bloom_filter: Some(vec![9]),
            repair_request: Vec::new(),
            content: Some(vec![7]),
        };

        let expected = [
            0x0a, 1, b'a', // 1: sender_id
            0x12, 1, b'b', // 2: message_id
            0x1a, 1, b'c', // 3: channel_id
            0x50, 1, // 10: lamport_timestamp
            0x5a, 3, 0x0a, 1, b'd', // 11: causal_history
            0x62, 1, 9, // 12: bloom_filter
            0xa2, 0x01, 1, 7, // 20: content
        ];
        assert_eq!(message.encode_to_vec(), expected);
        assert_eq!(Message::decode(&expected[..]).unwrap(), message);
    }
}
