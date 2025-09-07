use log::log::Log;
use std::sync::Arc;
use tokio::sync::Mutex;

use api::log_service_server::LogService;

mod error;
use error::ServiceError;

pub struct GrpcLogService {
    log: Arc<Mutex<Log>>,
}

impl GrpcLogService {
    pub fn new(log: Log) -> Self {
        Self {
            log: Arc::new(Mutex::new(log)),
        }
    }
}

#[tonic::async_trait]
impl LogService for GrpcLogService {
    async fn produce(
        &self,
        request: tonic::Request<api::ProduceRequest>,
    ) -> Result<tonic::Response<api::ProduceResponse>, tonic::Status> {
        let record = request
            .into_inner()
            .record
            .ok_or(ServiceError::RecordRequired)?;
        let offset = self
            .log
            .lock()
            .await
            .append(&record)
            .map_err(ServiceError::from)?;
        let response = api::ProduceResponse { offset };
        Ok(tonic::Response::new(response))
    }

    async fn consume(
        &self,
        request: tonic::Request<api::ConsumeRequest>,
    ) -> Result<tonic::Response<api::ConsumeResponse>, tonic::Status> {
        let offset = request.into_inner().offset;
        let record = self
            .log
            .lock()
            .await
            .read(offset)
            .map_err(ServiceError::from)?;
        let response = api::ConsumeResponse {
            record: Some(record),
        };
        Ok(tonic::Response::new(response))
    }
}
