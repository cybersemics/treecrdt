use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("invalid operation: {0}")]
    InvalidOperation(String),
    #[error("inconsistent state: {0}")]
    InconsistentState(String),
    #[error("missing dependency: {0}")]
    MissingDependency(String),
}
