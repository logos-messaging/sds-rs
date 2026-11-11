use rand_core::Rng;

use crate::bloom::{hash_count_for, RollingBloomFilter};
use crate::config::Config;
use crate::error::Error;
use crate::event::Event;
use crate::header::{Header, HistoryEntry};
use crate::incoming::{has_dependencies, IncomingBuffer, PendingDelivery};
use crate::outgoing::OutgoingBuffer;
use crate::record::SDSRecord;
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
        let lamport_timestamp = config
            .start
            .as_ref()
            .map_or(0, |start| start.lamport_timestamp);
        Ok(Self {
            bloom: RollingBloomFilter::new(config.bloom_capacity, config.bloom_error_rate),
            bloom_hash_count: hash_count_for(config.bloom_capacity, config.bloom_error_rate),
            config,
            storage,
            lamport_timestamp,
            outgoing: OutgoingBuffer::default(),
            incoming: IncomingBuffer::default(),
            repair,
            last_activity_ms: 0,
            last_sent_ms: 0,
            sync_jitter_ms: 0,
        })
    }

    /// Message IDs in the local log, oldest first.
    pub fn history(&self) -> Vec<String> {
        let entries = self.storage.latest(self.storage.message_count());
        entries.into_iter().map(|entry| entry.message_id).collect()
    }

    /// Returns the record to broadcast.
    pub fn send(&mut self, message: OutgoingMessage, now_ms: u64) -> Result<SDSRecord, Error> {
        self.check_outgoing_content(&message.content)?;
        let OutgoingMessage {
            message_id,
            content,
            retrieval_hint,
        } = message;
        let record = SDSRecord {
            header: self.build_header(message_id.clone(), now_ms),
            content: Some(content),
        };
        self.record_in_log(LogEntry {
            message_id: message_id.clone(),
            lamport_timestamp: self.lamport_timestamp,
            sender_id: self.config.participant_id.clone(),
            retrieval_hint,
            record: self.repair.is_some().then(|| record.clone()),
        });
        self.outgoing.add(message_id, record.clone(), now_ms);
        self.mark_sent(now_ms);
        Ok(record)
    }

    /// Returns the record of a message sent without ordering or delivery guarantees.
    pub fn send_ephemeral(&self, message_id: String, content: Vec<u8>) -> Result<SDSRecord, Error> {
        self.check_outgoing_content(&content)?;
        let header = Header {
            sender_id: self.config.participant_id.clone(),
            message_id,
            channel_id: self.config.channel_id.clone(),
            ..Default::default()
        };
        Ok(SDSRecord {
            header,
            content: Some(content),
        })
    }

    pub fn receive(
        &mut self,
        record: SDSRecord,
        retrieval_hint: Option<Vec<u8>>,
        now_ms: u64,
    ) -> Result<Vec<Event>, Error> {
        self.check_incoming(&record)?;
        let mut header = record.header.clone();
        if let Some(repair) = &mut self.repair {
            repair.on_message_received(&header.message_id);
        }
        if header.sender_id == self.config.participant_id {
            return Ok(Vec::new());
        }
        let Some(lamport_timestamp) = header.lamport_timestamp else {
            return Ok(vec![ready_event(record)]);
        };

        self.last_activity_ms = now_ms;
        let mut events = self.review_acks(&header);
        // Resends carry their original, now stale, repair requests.
        if self.storage.contains(&header.message_id) || self.incoming.contains(&header.message_id) {
            return Ok(events);
        }
        if let Some(repair) = &mut self.repair {
            repair.on_repair_requests(&header.repair_request, &self.storage, now_ms);
        }
        if !self.is_after_start(lamport_timestamp) {
            events.extend(self.log_before_start(record, lamport_timestamp, retrieval_hint));
            return Ok(events);
        }
        self.skip_history_before_start(&mut header);
        if !has_content(&record) {
            let missing = self.incoming.missing_dependencies(&header, &self.storage);
            self.request_repairs(&missing, now_ms);
            return Ok(events);
        }

        self.bloom.insert(&header.message_id);
        let delivery = PendingDelivery {
            header,
            lamport_timestamp,
            record,
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

    fn build_header(&mut self, message_id: String, now_ms: u64) -> Header {
        self.lamport_timestamp = now_ms.max(self.lamport_timestamp + 1);
        let repair_request = match &mut self.repair {
            Some(repair) => repair.take_due_requests(now_ms),
            None => Vec::new(),
        };
        Header {
            sender_id: self.config.participant_id.clone(),
            message_id,
            channel_id: self.config.channel_id.clone(),
            lamport_timestamp: Some(self.lamport_timestamp),
            causal_history: self.causal_history(),
            bloom_filter: Some(self.bloom.as_bytes().to_vec()),
            repair_request,
        }
    }

    // Nothing stamped at or before the start was delivered here, and naming it would hold back
    // this participant's messages wherever it is missing.
    fn causal_history(&self) -> Vec<HistoryEntry> {
        let is_repair_enabled = self.repair.is_some();
        self.storage
            .latest(self.config.max_causal_history)
            .into_iter()
            .filter(|entry| self.is_after_start(entry.lamport_timestamp))
            .map(|entry| HistoryEntry {
                message_id: entry.message_id,
                retrieval_hint: entry.retrieval_hint,
                sender_id: is_repair_enabled.then_some(entry.sender_id),
            })
            .collect()
    }

    fn review_acks(&mut self, header: &Header) -> Vec<Event> {
        self.outgoing
            .review_acks(
                header,
                self.bloom_hash_count,
                self.config.bloom_acks_required,
            )
            .into_iter()
            .map(|message_id| Event::MessageAcknowledged { message_id })
            .collect()
    }

    // A participant that has delivered the start stamps its sends after it.
    fn is_after_start(&self, lamport_timestamp: u64) -> bool {
        self.config
            .start
            .as_ref()
            .is_none_or(|start| lamport_timestamp > start.lamport_timestamp)
    }

    // Logged at once rather than held, as its history is from before the start too. It keeps no
    // record, so this participant answers no repair with it.
    fn log_before_start(
        &mut self,
        record: SDSRecord,
        lamport_timestamp: u64,
        retrieval_hint: Option<Vec<u8>>,
    ) -> Vec<Event> {
        if !has_content(&record) {
            return Vec::new();
        }
        self.bloom.insert(&record.header.message_id);
        self.record_in_log(LogEntry {
            message_id: record.header.message_id,
            lamport_timestamp,
            sender_id: record.header.sender_id,
            retrieval_hint,
            record: None,
        });
        self.deliver_ready()
    }

    // A history lists IDs oldest first, so the start and those it lists before it are all met.
    fn skip_history_before_start(&self, header: &mut Header) {
        let Some(start) = &self.config.start else {
            return;
        };
        let Some(start_index) = header
            .causal_history
            .iter()
            .position(|entry| entry.message_id == start.message_id)
        else {
            return;
        };
        header.causal_history.drain(..=start_index);
    }

    fn deliver_or_buffer(&mut self, delivery: PendingDelivery, now_ms: u64) -> Vec<Event> {
        if has_dependencies(&delivery.header, &self.storage) {
            return self.deliver_with_dependents(delivery);
        }
        let mut events = Vec::new();
        let missing = self
            .incoming
            .missing_dependencies(&delivery.header, &self.storage);
        if !missing.is_empty() {
            self.request_repairs(&missing, now_ms);
            let message_id = delivery.header.message_id.clone();
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
        events.extend(self.deliver_ready());
        events
    }

    fn deliver_ready(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        while let Some(ready) = self.incoming.take_ready(&self.storage) {
            events.push(self.deliver(ready));
        }
        events
    }

    fn deliver(&mut self, delivery: PendingDelivery) -> Event {
        let Header {
            sender_id,
            message_id,
            ..
        } = delivery.header;
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
            record: is_responder.then(|| delivery.record.clone()),
        });
        Event::MessageReady {
            message_id,
            sender_id,
            content: delivery.record.content.unwrap_or_default(),
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
            .filter_map(|message_id| self.storage.get(message_id)?.record)
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
        let mut header = self.build_header(String::new(), now_ms);
        header.message_id = format!(
            "sync-{}-{}",
            self.config.participant_id, self.lamport_timestamp
        );
        self.mark_sent(now_ms);
        // Random backoff so a whole group does not sync in the same instant.
        self.sync_jitter_ms = rng.next_u64() % (self.config.sync_interval_ms / 2 + 1);
        Event::Sync(SDSRecord {
            header,
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

    fn check_incoming(&self, record: &SDSRecord) -> Result<(), Error> {
        if record.header.channel_id != self.config.channel_id {
            return Err(Error::WrongChannel {
                expected: self.config.channel_id.clone(),
                actual: record.header.channel_id.clone(),
            });
        }
        self.check_content_size(record.content.as_deref().unwrap_or_default())
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

fn has_content(record: &SDSRecord) -> bool {
    record
        .content
        .as_ref()
        .is_some_and(|content| !content.is_empty())
}

fn ready_event(record: SDSRecord) -> Event {
    Event::MessageReady {
        message_id: record.header.message_id,
        sender_id: record.header.sender_id,
        content: record.content.unwrap_or_default(),
    }
}
