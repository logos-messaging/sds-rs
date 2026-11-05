use std::collections::HashSet;

use crate::bloom::BloomFilter;
use crate::config::Config;
use crate::event::Event;
use crate::packet::Packet;
use crate::proto::Message;

// Possibly-acknowledged messages have likely reached someone, so unacknowledged ones resend first.
const POSSIBLY_ACKED_RESEND_FACTOR: u64 = 2;

struct PendingSend {
    message_id: String,
    packet: Packet,
    last_sent_ms: u64,
    resend_count: u32,
    bloom_ackers: HashSet<String>,
}

impl PendingSend {
    fn record_ack(&mut self, message: &Message, bloom: Option<&BloomFilter>) {
        if bloom.is_some_and(|bloom| bloom.contains(&self.message_id)) {
            self.bloom_ackers.insert(message.sender_id.clone());
        }
    }

    fn is_acked(&self, message: &Message, acks_required: usize) -> bool {
        let in_causal_history = message
            .causal_history
            .iter()
            .any(|entry| entry.message_id == self.message_id);
        in_causal_history || self.bloom_ackers.len() >= acks_required
    }

    fn resend_due_ms(&self, resend_interval_ms: u64) -> u64 {
        if self.bloom_ackers.is_empty() {
            return self.last_sent_ms + resend_interval_ms;
        }
        self.last_sent_ms + resend_interval_ms * POSSIBLY_ACKED_RESEND_FACTOR
    }
}

#[derive(Default)]
pub struct OutgoingBuffer {
    pending: Vec<PendingSend>,
}

impl OutgoingBuffer {
    pub fn add(&mut self, message_id: String, packet: Packet, now_ms: u64) {
        self.pending.push(PendingSend {
            message_id,
            packet,
            last_sent_ms: now_ms,
            resend_count: 0,
            bloom_ackers: HashSet::new(),
        });
    }

    /// Returns the IDs of messages that are now acknowledged.
    pub fn review_acks(
        &mut self,
        message: &Message,
        hash_count: u32,
        acks_required: usize,
    ) -> Vec<String> {
        let bloom = message
            .bloom_filter
            .clone()
            .map(|bits| BloomFilter::from_bytes(bits, hash_count));
        let mut acked = Vec::new();
        self.pending.retain_mut(|send| {
            send.record_ack(message, bloom.as_ref());
            if !send.is_acked(message, acks_required) {
                return true;
            }
            acked.push(send.message_id.clone());
            false
        });
        acked
    }

    pub fn sweep(&mut self, now_ms: u64, config: &Config) -> Vec<Event> {
        let mut events = Vec::new();
        self.pending.retain_mut(|send| {
            if now_ms < send.resend_due_ms(config.resend_interval_ms) {
                return true;
            }
            if send.resend_count >= config.max_resend_attempts {
                events.push(Event::SendFailed {
                    message_id: send.message_id.clone(),
                });
                return false;
            }
            send.resend_count += 1;
            send.last_sent_ms = now_ms;
            events.push(Event::Resend(send.packet.clone()));
            true
        });
        events
    }
}
