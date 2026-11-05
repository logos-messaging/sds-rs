use crate::proto::HistoryEntry;
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
    Resend(SDSRecord),
    Sync(SDSRecord),
    Repair(SDSRecord),
}
