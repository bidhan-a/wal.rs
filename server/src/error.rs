use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Record is required")]
    RecordRequired,
    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),
    #[error("Log operation failed: {0}")]
    LogError(#[from] log::error::LogError),
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<ServiceError> for tonic::Status {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::RecordRequired => tonic::Status::invalid_argument("Record is required"),
            ServiceError::InvalidOffset(_) => tonic::Status::invalid_argument(err.to_string()),
            ServiceError::LogError(_) => tonic::Status::internal("Log operation failed"),
            ServiceError::Internal(_) => tonic::Status::internal("Internal server error"),
        }
    }
}
