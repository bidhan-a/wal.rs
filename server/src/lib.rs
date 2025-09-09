use futures::Stream;
use log::log::Log;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

use api::log_service_server::LogService;

mod error;
use error::ServiceError;

pub struct GrpcLogService {
    log: Arc<Log>,
}

impl GrpcLogService {
    pub fn new(log: Log) -> Self {
        Self { log: Arc::new(log) }
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
        let offset = self.log.append(&record).map_err(ServiceError::from)?;
        let response = api::ProduceResponse { offset };
        Ok(tonic::Response::new(response))
    }

    async fn consume(
        &self,
        request: tonic::Request<api::ConsumeRequest>,
    ) -> Result<tonic::Response<api::ConsumeResponse>, tonic::Status> {
        let offset = request.into_inner().offset;
        let record = self.log.read(offset).map_err(ServiceError::from)?;
        let response = api::ConsumeResponse {
            record: Some(record),
        };
        Ok(tonic::Response::new(response))
    }

    type ProduceStreamStream =
        Pin<Box<dyn Stream<Item = Result<api::ProduceResponse, tonic::Status>> + Send>>;

    async fn produce_stream(
        &self,
        request: tonic::Request<tonic::Streaming<api::ProduceRequest>>,
    ) -> Result<tonic::Response<Self::ProduceStreamStream>, tonic::Status> {
        let mut stream = request.into_inner();
        let log = self.log.clone();

        let (tx, rx) = mpsc::channel(1000); // Buffer up to 1000 responses

        // Spawn a task to handle the stream processing.
        tokio::spawn(async move {
            while let Some(request) = stream.next().await {
                match request {
                    Ok(req) => {
                        if let Some(record) = req.record {
                            match log.append(&record) {
                                Ok(offset) => {
                                    let response = api::ProduceResponse { offset };
                                    if tx.send(Ok(response)).await.is_err() {
                                        break; // Client disconnected
                                    }
                                }
                                Err(e) => {
                                    let status = ServiceError::from(e).into();
                                    let _ = tx.send(Err(status)).await;
                                    break;
                                }
                            }
                        } else {
                            let status = ServiceError::RecordRequired.into();
                            let _ = tx.send(Err(status)).await;
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(output_stream)))
    }

    type ConsumeStreamStream =
        Pin<Box<dyn Stream<Item = Result<api::ConsumeResponse, tonic::Status>> + Send>>;

    async fn consume_stream(
        &self,
        request: tonic::Request<api::ConsumeRequest>,
    ) -> Result<tonic::Response<Self::ConsumeStreamStream>, tonic::Status> {
        let start_offset = request.into_inner().offset;
        let log = self.log.clone();

        let (tx, rx) = mpsc::channel(1000); // Buffer up to 1000 responses

        // Spawn a task to handle streaming reads.
        tokio::spawn(async move {
            let mut current_offset = start_offset;

            loop {
                match log.read(current_offset) {
                    Ok(record) => {
                        let response = api::ConsumeResponse {
                            record: Some(record),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break; // Client disconnected
                        }
                        current_offset += 1;
                    }
                    Err(e) => {
                        let status = ServiceError::from(e).into();
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(output_stream)))
    }
}
