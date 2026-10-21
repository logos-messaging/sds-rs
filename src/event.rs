use crate::proto::HistoryEntry;

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    MessageReady {
        message_id: String,
        content: Vec<u8>,
    },
    MessageAcknowledged {
        message_id: String,
    },
    MissingDependencies {
        message_id: String,
        missing: Vec<HistoryEntry>,
    },
    SendFailed {
        message_id: String,
    },
    Lost {
        message_id: String,
    },
    Resend(Vec<u8>),
    Sync(Vec<u8>),
    Repair(Vec<u8>),
}
