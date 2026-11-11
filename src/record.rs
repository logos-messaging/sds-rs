use crate::header::Header;

/// SDS's record of a message: the header, with the content beside it.
#[derive(Clone, Debug, PartialEq)]
pub struct SDSRecord {
    pub header: Header,
    /// `None` for a sync message.
    pub content: Option<Vec<u8>>,
}
