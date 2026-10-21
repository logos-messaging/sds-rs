use std::fmt;

#[derive(Debug)]
pub enum Error {
    Decode(prost::DecodeError),
    EmptyContent,
    ContentTooLarge { size: usize, max: usize },
    WrongChannel { expected: String, actual: String },
    InvalidConfig(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Decode(error) => write!(f, "failed to decode message: {error}"),
            Error::EmptyContent => write!(f, "content must not be empty"),
            Error::ContentTooLarge { size, max } => {
                write!(f, "content is {size} bytes, max is {max}")
            }
            Error::WrongChannel { expected, actual } => {
                write!(f, "message for channel {actual}, expected {expected}")
            }
            Error::InvalidConfig(reason) => write!(f, "invalid config: {reason}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<prost::DecodeError> for Error {
    fn from(error: prost::DecodeError) -> Self {
        Error::Decode(error)
    }
}
