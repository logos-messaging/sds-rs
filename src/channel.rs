use prost::Message as _;
use rand_core::Rng;

use crate::bloom::{hash_count_for, RollingBloomFilter};
use crate::config::Config;
use crate::error::Error;
use crate::event::Event;
use crate::incoming::{has_dependencies, IncomingBuffer, PendingDelivery};
use crate::outgoing::OutgoingBuffer;
use crate::packet::Packet;
use crate::proto::{HistoryEntry, Message};
use crate::repair::Repair;
use crate::storage::{InMemoryStorage, LogEntry, Storage};

pub struct OutgoingMessage {
    pub message_id: String,
    pub content: Vec<u8>,
    pub retrieval_hint: Option<Vec<u8>>,
}

/// One participant's view of one SDS channel.
pub struct Channel<S: Storage = InMemoryStorage> {
    config: Config,
    storage: S,
    lamport_timestamp: u64,
    bloom: RollingBloomFilter,
    bloom_hash_count: u32,
    outgoing: OutgoingBuffer,
    incoming: IncomingBuffer,
    repair: Option<Repair>,
    last_activity_ms: u64,
    last_sent_ms: u64,
    sync_jitter_ms: u64,
}

impl Channel<InMemoryStorage> {
    pub fn new(config: Config) -> Result<Self, Error> {
        Self::with_storage(config, InMemoryStorage::default())
    }
}

impl<S: Storage> Channel<S> {
    pub fn with_storage(config: Config, storage: S) -> Result<Self, Error> {
        config.validate()?;
        let repair = config
            .repair
            .clone()
            .map(|repair_config| Repair::new(repair_config, config.participant_id.clone()));
        Ok(Self {
            bloom: RollingBloomFilter::new(config.bloom_capacity, config.bloom_error_rate),
            bloom_hash_count: hash_count_for(config.bloom_capacity, config.bloom_error_rate),
            config,
            storage,
            lamport_timestamp: 0,
            outgoing: OutgoingBuffer::default(),
            incoming: IncomingBuffer::default(),
            repair,
            last_activity_ms: 0,
            last_sent_ms: 0,
            sync_jitter_ms: 0,
        })
    }

    /// Message IDs in the local log, in delivery order.
    pub fn history(&self) -> Vec<String> {
        let entries = self.storage.latest(self.storage.message_count());
        entries.into_iter().map(|entry| entry.message_id).collect()
    }

    /// Returns the packet to broadcast.
    pub fn send(&mut self, message: OutgoingMessage, now_ms: u64) -> Result<Packet, Error> {
        self.check_outgoing_content(&message.content)?;
        let OutgoingMessage {
            message_id,
            content,
            retrieval_hint,
        } = message;
        let packet = Packet {
            header: self
                .build_message(message_id.clone(), now_ms)
                .encode_to_vec(),
            content: Some(content),
        };
        self.record_in_log(LogEntry {
            message_id: message_id.clone(),
            lamport_timestamp: self.lamport_timestamp,
            sender_id: self.config.participant_id.clone(),
            retrieval_hint,
            packet: self.repair.is_some().then(|| packet.clone()),
        });
        self.outgoing.add(message_id, packet.clone(), now_ms);
        self.mark_sent(now_ms);
        Ok(packet)
    }

    /// Returns the packet of a message sent without ordering or delivery guarantees.
    pub fn send_ephemeral(&self, message_id: String, content: Vec<u8>) -> Result<Packet, Error> {
        self.check_outgoing_content(&content)?;
        let header = Message {
            sender_id: self.config.participant_id.clone(),
            message_id,
            channel_id: self.config.channel_id.clone(),
            ..Default::default()
        };
        Ok(Packet {
            header: header.encode_to_vec(),
            content: Some(content),
        })
    }

    pub fn receive(
        &mut self,
        packet: Packet,
        retrieval_hint: Option<Vec<u8>>,
        now_ms: u64,
    ) -> Result<Vec<Event>, Error> {
        let mut message = Message::decode(packet.header.as_slice())?;
        message.content = packet.content.clone();
        self.check_incoming(&message)?;
        if let Some(repair) = &mut self.repair {
            repair.on_message_received(&message.message_id);
        }
        if message.sender_id == self.config.participant_id {
            return Ok(Vec::new());
        }
        let Some(lamport_timestamp) = message.lamport_timestamp else {
            return Ok(vec![ready_event(message)]);
        };

        self.last_activity_ms = now_ms;
        let mut events = self.review_acks(&message);
        // Resends carry their original, now stale, repair requests.
        if self.storage.contains(&message.message_id) || self.incoming.contains(&message.message_id)
        {
            return Ok(events);
        }
        if let Some(repair) = &mut self.repair {
            repair.on_repair_requests(&message.repair_request, &self.storage, now_ms);
        }
        if !has_content(&message) {
            let missing = self.incoming.missing_dependencies(&message, &self.storage);
            self.request_repairs(&missing, now_ms);
            return Ok(events);
        }

        self.bloom.insert(&message.message_id);
        let delivery = PendingDelivery {
            message,
            lamport_timestamp,
            packet,
            retrieval_hint,
            received_at_ms: now_ms,
        };
        events.extend(self.deliver_or_buffer(delivery, now_ms));
        Ok(events)
    }

    /// Runs periodic work. Call regularly, e.g. every few seconds.
    pub fn tick(&mut self, now_ms: u64, rng: &mut impl Rng) -> Vec<Event> {
        let mut events = self.outgoing.sweep(now_ms, &self.config);
        events.extend(self.sweep_incoming(now_ms));
        events.extend(self.sweep_repair_responses(now_ms));
        if self.is_sync_due(now_ms) {
            events.push(self.send_sync(now_ms, rng));
        }
        events
    }

    fn build_message(&mut self, message_id: String, now_ms: u64) -> Message {
        self.lamport_timestamp = now_ms.max(self.lamport_timestamp + 1);
        let repair_request = match &mut self.repair {
            Some(repair) => repair.take_due_requests(now_ms),
            None => Vec::new(),
        };
        Message {
            sender_id: self.config.participant_id.clone(),
            message_id,
            channel_id: self.config.channel_id.clone(),
            lamport_timestamp: Some(self.lamport_timestamp),
            causal_history: self.causal_history(),
            bloom_filter: Some(self.bloom.as_bytes().to_vec()),
            repair_request,
            content: None,
        }
    }

    fn causal_history(&self) -> Vec<HistoryEntry> {
        let is_repair_enabled = self.repair.is_some();
        self.storage
            .latest(self.config.max_causal_history)
            .into_iter()
            .map(|entry| HistoryEntry {
                message_id: entry.message_id,
                retrieval_hint: entry.retrieval_hint,
                sender_id: is_repair_enabled.then_some(entry.sender_id),
            })
            .collect()
    }

    fn review_acks(&mut self, message: &Message) -> Vec<Event> {
        self.outgoing
            .review_acks(
                message,
                self.bloom_hash_count,
                self.config.bloom_acks_required,
            )
            .into_iter()
            .map(|message_id| Event::MessageAcknowledged { message_id })
            .collect()
    }

    fn deliver_or_buffer(&mut self, delivery: PendingDelivery, now_ms: u64) -> Vec<Event> {
        if has_dependencies(&delivery.message, &self.storage) {
            return self.deliver_with_dependents(delivery);
        }
        let mut events = Vec::new();
        let missing = self
            .incoming
            .missing_dependencies(&delivery.message, &self.storage);
        if !missing.is_empty() {
            self.request_repairs(&missing, now_ms);
            let message_id = delivery.message.message_id.clone();
            events.push(Event::MissingDependencies {
                message_id,
                missing,
            });
        }
        if let Some(message_id) = self.incoming.add(delivery, self.config.max_incoming_buffer) {
            events.push(Event::Lost { message_id });
        }
        events
    }

    fn deliver_with_dependents(&mut self, delivery: PendingDelivery) -> Vec<Event> {
        let mut events = vec![self.deliver(delivery)];
        while let Some(ready) = self.incoming.take_ready(&self.storage) {
            events.push(self.deliver(ready));
        }
        events
    }

    fn deliver(&mut self, delivery: PendingDelivery) -> Event {
        let Message {
            sender_id,
            message_id,
            content,
            ..
        } = delivery.message;
        self.lamport_timestamp = self.lamport_timestamp.max(delivery.lamport_timestamp);
        let is_responder = self
            .repair
            .as_ref()
            .is_some_and(|repair| repair.is_responder(&sender_id, &message_id));
        self.record_in_log(LogEntry {
            message_id: message_id.clone(),
            lamport_timestamp: delivery.lamport_timestamp,
            sender_id: sender_id.clone(),
            retrieval_hint: delivery.retrieval_hint,
            packet: is_responder.then_some(delivery.packet),
        });
        Event::MessageReady {
            message_id,
            sender_id,
            content: content.unwrap_or_default(),
        }
    }

    fn record_in_log(&mut self, entry: LogEntry) {
        self.storage.insert(entry);
        while self.storage.message_count() > self.config.max_history {
            self.storage.remove_oldest();
        }
    }

    fn request_repairs(&mut self, missing: &[HistoryEntry], now_ms: u64) {
        if let Some(repair) = &mut self.repair {
            repair.request_missing(missing, now_ms);
        }
    }

    fn sweep_incoming(&mut self, now_ms: u64) -> Vec<Event> {
        let missing = self.incoming.all_missing_dependencies(&self.storage);
        self.request_repairs(&missing, now_ms);
        self.incoming
            .expire(now_ms, self.config.dependency_timeout_ms)
            .into_iter()
            .map(|message_id| Event::Lost { message_id })
            .collect()
    }

    fn sweep_repair_responses(&mut self, now_ms: u64) -> Vec<Event> {
        let Some(repair) = &mut self.repair else {
            return Vec::new();
        };
        repair
            .take_due_responses(now_ms)
            .iter()
            .filter_map(|message_id| self.storage.get(message_id)?.packet)
            .map(Event::Repair)
            .collect()
    }

    // Requests that are due skip the quiet-channel backoff so repairs are not delayed.
    fn is_sync_due(&self, now_ms: u64) -> bool {
        let quiet_until_ms =
            self.last_activity_ms + self.config.sync_interval_ms + self.sync_jitter_ms;
        let has_due_requests = self
            .repair
            .as_ref()
            .is_some_and(|repair| repair.has_due_requests(now_ms));
        let can_resync = now_ms >= self.last_sent_ms + self.config.sync_interval_ms;
        now_ms >= quiet_until_ms || (has_due_requests && can_resync)
    }

    fn send_sync(&mut self, now_ms: u64, rng: &mut impl Rng) -> Event {
        let mut message = self.build_message(String::new(), now_ms);
        message.message_id = format!(
            "sync-{}-{}",
            self.config.participant_id, self.lamport_timestamp
        );
        self.mark_sent(now_ms);
        // Random backoff so a whole group does not sync in the same instant.
        self.sync_jitter_ms = rng.next_u64() % (self.config.sync_interval_ms / 2 + 1);
        Event::Sync(Packet {
            header: message.encode_to_vec(),
            content: None,
        })
    }

    fn mark_sent(&mut self, now_ms: u64) {
        self.last_sent_ms = now_ms;
        self.last_activity_ms = now_ms;
    }

    fn check_outgoing_content(&self, content: &[u8]) -> Result<(), Error> {
        if content.is_empty() {
            return Err(Error::EmptyContent);
        }
        self.check_content_size(content)
    }

    fn check_incoming(&self, message: &Message) -> Result<(), Error> {
        if message.channel_id != self.config.channel_id {
            return Err(Error::WrongChannel {
                expected: self.config.channel_id.clone(),
                actual: message.channel_id.clone(),
            });
        }
        self.check_content_size(message.content.as_deref().unwrap_or_default())
    }

    fn check_content_size(&self, content: &[u8]) -> Result<(), Error> {
        if content.len() > self.config.max_content_size {
            return Err(Error::ContentTooLarge {
                size: content.len(),
                max: self.config.max_content_size,
            });
        }
        Ok(())
    }
}

fn has_content(message: &Message) -> bool {
    message
        .content
        .as_ref()
        .is_some_and(|content| !content.is_empty())
}

fn ready_event(message: Message) -> Event {
    Event::MessageReady {
        message_id: message.message_id,
        sender_id: message.sender_id,
        content: message.content.unwrap_or_default(),
    }
}
