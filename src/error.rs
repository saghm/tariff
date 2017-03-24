use std::fmt;

#[derive(Debug)]
pub enum ErrorType {
    ConversionError,
}

#[derive(Debug)]
pub struct Error {
    message: &'static str,
    err_type: ErrorType,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.message)
    }
}

impl Error {
    pub fn coversion_error(message: &'static str) -> Self {
        Error {
            message: message,
            err_type: ErrorType::ConversionError,
        }
    }
}

impl ::std::error::Error for Error {
    fn description(&self) -> &str {
        self.message
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        None
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

