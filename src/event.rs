use crate::header::{Header, HistoryEntry};
use crate::record::SDSRecord;

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    MessageReady {
        message_id: String,
        sender_id: String,
        content: Vec<u8>,
    },
    MessageAcknowledged {
        message_id: String,
    },
    /// The message waits in the incoming buffer for its causal history to be fulfilled.
    MessageBuffered {
        message_id: String,
        /// Unseen messages.
        missing_dependencies: Vec<HistoryEntry>,
        /// Arrived, not yet released.
        buffered_dependencies: Vec<HistoryEntry>,
    },
    /// A received message that is neither buffered nor released.
    MessageIgnored {
        message_id: String,
        reason: IgnoreReason,
    },
    SendFailed {
        message_id: String,
    },
    /// A buffered message was dropped undelivered.
    Lost {
        message_id: String,
        reason: LossReason,
    },
    Resend(SDSRecord),
    /// Header only: a sync has no content.
    Sync(Header),
    Repair(SDSRecord),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LossReason {
    /// Its causal history did not arrive within the dependency timeout.
    Expired,
    /// The incoming buffer was full, and it was the oldest message there.
    BufferFull,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IgnoreReason {
    /// This participant sent it.
    Own,
    /// It is already delivered or buffered.
    Duplicate,
    /// It is stamped at or before the start, so it is logged without release.
    BeforeStart,
}
