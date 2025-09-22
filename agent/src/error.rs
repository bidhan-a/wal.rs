use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("Log error: {0}")]
    LogError(#[from] log::error::LogError),
    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Generic error: {0}")]
    GenericError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub type AgentResult<T> = Result<T, AgentError>;
