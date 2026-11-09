use prost::Message as _;

use crate::error::Error;
use crate::proto::Message;

/// SDS's record of a message: the encoded header, with the content beside it.
#[derive(Clone, Debug, PartialEq)]
pub struct SDSRecord {
    /// The encoded `proto::Message`, its content unset.
    pub header: Vec<u8>,
    /// `None` for a sync message.
    pub content: Option<Vec<u8>>,
}

impl SDSRecord {
    /// Encodes the spec's single `Message`, content inside.
    pub fn to_bytes(&self) -> Vec<u8> {
        let content = Message {
            content: self.content.clone(),
            ..Default::default()
        };
        // Protobuf decodes concatenated messages as one, and content is the last field.
        [self.header.as_slice(), &content.encode_to_vec()].concat()
    }

    /// Splits the spec's single `Message` into header and content.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut message = Message::decode(bytes)?;
        let content = message.content.take();
        Ok(Self {
            header: message.encode_to_vec(),
            content,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(content: Option<Vec<u8>>) -> Message {
        Message {
            sender_id: "alice".into(),
            message_id: "m1".into(),
            channel_id: "chan".into(),
            lamport_timestamp: Some(1),
            content,
            ..Default::default()
        }
    }

    fn record(content: Option<Vec<u8>>) -> SDSRecord {
        SDSRecord {
            header: message(None).encode_to_vec(),
            content,
        }
    }

    #[test]
    fn to_bytes_matches_the_single_message_encoding() {
        let content = Some(b"hi".to_vec());
        assert_eq!(
            record(content.clone()).to_bytes(),
            message(content).encode_to_vec()
        );
        assert_eq!(record(None).to_bytes(), message(None).encode_to_vec());
    }

    #[test]
    fn from_bytes_takes_the_content_out() {
        let content = Some(b"hi".to_vec());
        let bytes = message(content.clone()).encode_to_vec();
        assert_eq!(SDSRecord::from_bytes(&bytes).unwrap(), record(content));
    }
}
