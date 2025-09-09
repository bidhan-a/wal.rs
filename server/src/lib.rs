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

#[cfg(test)]
mod tests {
    use super::*;
    use api::{ConsumeRequest, ProduceRequest, Record};
    use cfg::{Config, DurabilityPolicy, LogConfig, SegmentConfig};
    use futures::StreamExt;
    use log::log::Log;
    use tempfile::TempDir;
    use tonic::Request;

    fn create_test_log() -> (Log, TempDir) {
        let config = Config {
            log: LogConfig {
                segment: SegmentConfig {
                    max_store_size: 1024,
                    max_index_size: 1024,
                    initial_offset: 0,
                },
                durability: DurabilityPolicy::Never,
            },
        };
        let temp_dir = tempfile::tempdir().unwrap();
        let log = Log::new(temp_dir.path(), config).unwrap();
        (log, temp_dir)
    }

    #[tokio::test]
    async fn test_produce() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let record = Record {
            offset: None,
            value: b"test record".to_vec(),
        };
        let request = Request::new(ProduceRequest {
            record: Some(record),
        });

        let response = service.produce(request).await.unwrap();
        assert_eq!(response.into_inner().offset, 0);
    }

    #[tokio::test]
    async fn test_produce_multiple_records() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        // Produce first record.
        let record1 = Record {
            offset: None,
            value: b"first record".to_vec(),
        };
        let request1 = Request::new(ProduceRequest {
            record: Some(record1),
        });
        let response1 = service.produce(request1).await.unwrap();
        assert_eq!(response1.into_inner().offset, 0);

        // Produce second record.
        let record2 = Record {
            offset: None,
            value: b"second record".to_vec(),
        };
        let request2 = Request::new(ProduceRequest {
            record: Some(record2),
        });
        let response2 = service.produce(request2).await.unwrap();
        assert_eq!(response2.into_inner().offset, 1);
    }

    #[tokio::test]
    async fn test_produce_missing_record() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let request = Request::new(ProduceRequest { record: None });

        let result = service.produce(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_consume() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        // First produce a record.
        let record = Record {
            offset: None,
            value: b"test record".to_vec(),
        };
        let produce_request = Request::new(ProduceRequest {
            record: Some(record),
        });
        service.produce(produce_request).await.unwrap();

        // Now consume it.
        let consume_request = Request::new(ConsumeRequest { offset: 0 });
        let response = service.consume(consume_request).await.unwrap();
        let consumed_record = response.into_inner().record.unwrap();
        assert_eq!(consumed_record.value, b"test record");
    }

    #[tokio::test]
    async fn test_consume_invalid_offset() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let request = Request::new(ConsumeRequest { offset: 999 });
        let result = service.consume(request).await;
        assert!(result.is_err());
    }

    // TODO: Testing produce_stream is a bit tricky since it requires a stream request.

    #[tokio::test]
    async fn test_consume_stream() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        // First produce some records.
        let records = vec![b"record 1", b"record 2", b"record 3"];
        for record_data in &records {
            let record = Record {
                offset: None,
                value: record_data.to_vec(),
            };
            let request = Request::new(ProduceRequest {
                record: Some(record),
            });
            service.produce(request).await.unwrap();
        }

        // Now consume them via stream.
        let request = Request::new(ConsumeRequest { offset: 0 });
        let response = service.consume_stream(request).await.unwrap();
        let mut output_stream = response.into_inner();

        // Collect responses.
        let mut consumed_records = Vec::new();
        for _ in 0..3 {
            let result = output_stream.next().await.unwrap();
            let record = result.unwrap().record.unwrap();
            consumed_records.push(record.value);
        }

        // Verify consumed records.
        assert_eq!(consumed_records.len(), 3);
        assert_eq!(consumed_records[0], b"record 1");
        assert_eq!(consumed_records[1], b"record 2");
        assert_eq!(consumed_records[2], b"record 3");

        // Next read should fail (since there are no more records)
        let result = output_stream.next().await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_consume_stream_invalid_offset() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let request = Request::new(ConsumeRequest { offset: 999 });
        let response = service.consume_stream(request).await.unwrap();
        let mut output_stream = response.into_inner();

        // Should immediately get an error.
        let result = output_stream.next().await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_end_to_end_workflow() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        // 1. Produce some records via single calls.
        for i in 0..5 {
            let record = Record {
                offset: None,
                value: format!("single record {}", i).into_bytes(),
            };
            let request = Request::new(ProduceRequest {
                record: Some(record),
            });
            let response = service.produce(request).await.unwrap();
            assert_eq!(response.into_inner().offset, i);
        }

        // 2. Consume individual records.
        for i in 0..5 {
            let request = Request::new(ConsumeRequest { offset: i });
            let response = service.consume(request).await.unwrap();
            let record = response.into_inner().record.unwrap();
            assert!(!record.value.is_empty());
        }

        // 3. Consume via stream from offset 3.
        let request = Request::new(ConsumeRequest { offset: 3 });
        let response = service.consume_stream(request).await.unwrap();
        let mut output_stream = response.into_inner();

        // Should get records 3, 4, then error on 5.
        for _expected_offset in 3..5 {
            let result = output_stream.next().await.unwrap();
            let record = result.unwrap().record.unwrap();
            assert!(!record.value.is_empty());
        }

        // Should get error on non-existent offset 5.
        let result = output_stream.next().await.unwrap();
        assert!(result.is_err());
    }
}
