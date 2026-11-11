use std::fmt;

#[derive(Debug)]
pub enum Error {
    EmptyContent,
    ContentTooLarge { size: usize, max: usize },
    WrongChannel { expected: String, actual: String },
    InvalidConfig(&'static str),
    UnknownMessage(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::EmptyContent => write!(f, "content must not be empty"),
            Error::ContentTooLarge { size, max } => {
                write!(f, "content is {size} bytes, max is {max}")
            }
            Error::WrongChannel { expected, actual } => {
                write!(f, "message for channel {actual}, expected {expected}")
            }
            Error::InvalidConfig(reason) => write!(f, "invalid config: {reason}"),
            Error::UnknownMessage(message_id) => {
                write!(f, "message {message_id} is not in the log")
            }
        }
    }
}

impl std::error::Error for Error {}
