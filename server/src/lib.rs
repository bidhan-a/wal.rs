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
    use api::{log_service_client::LogServiceClient, log_service_server::LogServiceServer};
    use cfg::{Config, DurabilityPolicy, LogConfig, SegmentConfig};
    use log::log::Log;
    use tokio::sync::oneshot;
    use tonic::transport::Server;

    fn create_test_log() -> (Log, tempfile::TempDir) {
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

    async fn start_server(
        service: GrpcLogService,
    ) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
        // Bind to an ephemeral port on localhost.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let svc = LogServiceServer::new(service);

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        (format!("http://{}", addr), shutdown_tx, handle)
    }

    #[tokio::test]
    async fn test_produce() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let (endpoint, shutdown_tx, handle) = start_server(service).await;
        let mut client = LogServiceClient::connect(endpoint).await.unwrap();

        let record = Record {
            offset: None,
            value: b"p1".to_vec(),
        };
        let resp = client
            .produce(tonic::Request::new(ProduceRequest {
                record: Some(record),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.offset, 0);

        let _ = shutdown_tx.send(());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_consume() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let (endpoint, shutdown_tx, handle) = start_server(service).await;
        let mut client = LogServiceClient::connect(endpoint).await.unwrap();

        // Produce a record.
        let record = Record {
            offset: None,
            value: b"c1".to_vec(),
        };
        client
            .produce(tonic::Request::new(ProduceRequest {
                record: Some(record),
            }))
            .await
            .unwrap();

        // Consume the record.
        let got = client
            .consume(tonic::Request::new(ConsumeRequest { offset: 0 }))
            .await
            .unwrap()
            .into_inner()
            .record
            .unwrap();
        assert_eq!(got.value, b"c1");

        let _ = shutdown_tx.send(());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_produce_stream() {
        use tokio::sync::mpsc;
        use tokio_stream::wrappers::ReceiverStream;

        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let (endpoint, shutdown_tx, handle) = start_server(service).await;
        let mut client = LogServiceClient::connect(endpoint).await.unwrap();

        // Build a channel-backed request stream.
        let (tx, rx) = mpsc::channel(8);
        let req_stream = ReceiverStream::new(rx);

        let mut resp_stream = client
            .produce_stream(tonic::Request::new(req_stream))
            .await
            .unwrap()
            .into_inner();

        // Send three records then close input.
        for (i, data) in [b"s1", b"s2", b"s3"].into_iter().enumerate() {
            let req = ProduceRequest {
                record: Some(Record {
                    offset: None,
                    value: data.to_vec(),
                }),
            };
            tx.send(req).await.unwrap();
            // Expect ack with increasing offsets.
            let ack = resp_stream.message().await.unwrap().unwrap();
            assert_eq!(ack.offset as usize, i);
        }
        drop(tx);

        // No more acks expected.
        let done = resp_stream.message().await.unwrap_or(None);
        assert!(done.is_none());

        let _ = shutdown_tx.send(());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_consume_stream() {
        let (log, _temp_dir) = create_test_log();
        let service = GrpcLogService::new(log);

        let (endpoint, shutdown_tx, handle) = start_server(service).await;
        let mut client = LogServiceClient::connect(endpoint).await.unwrap();

        // Produce three records.
        for data in [b"r1", b"r2", b"r3"] {
            let record = Record {
                offset: None,
                value: data.to_vec(),
            };
            client
                .produce(tonic::Request::new(ProduceRequest {
                    record: Some(record),
                }))
                .await
                .unwrap();
        }

        let mut stream = client
            .consume_stream(tonic::Request::new(ConsumeRequest { offset: 0 }))
            .await
            .unwrap()
            .into_inner();

        // Expect three messages with those values.
        for expected in [b"r1", b"r2", b"r3"] {
            let msg = stream.message().await.unwrap().unwrap();
            let rec = msg.record.unwrap();
            assert_eq!(rec.value, expected);
        }

        // Next should be end or error; service returns error when no more records.
        // Here we accept either end-of-stream or a final error converted to end.
        let maybe_next = stream.message().await;
        // If we got Ok(None) it's fine, Err(_) is also acceptable.
        // Only panic on unexpected Ok(Some(_)).
        if let Ok(Some(_)) = maybe_next {
            panic!("unexpected extra message in consume_stream");
        }

        let _ = shutdown_tx.send(());
        handle.await.unwrap();
    }
}
