use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use api::log_service_client::LogServiceClient;
use api::{ConsumeRequest, ProduceRequest};

use crate::error::{LogError, LogResult};

pub struct Replicator {
    local_server: LogServiceClient<Channel>,
    servers: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<()>>>>,
    closed: Arc<AtomicBool>,
    close_tx: Arc<Mutex<Option<mpsc::UnboundedSender<()>>>>,
    tasks: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl Replicator {
    pub fn new(local_server: LogServiceClient<Channel>) -> Self {
        Self {
            local_server,
            servers: Arc::new(Mutex::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            close_tx: Arc::new(Mutex::new(None)),
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn join(&self, name: &str, addr: &str) -> LogResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Ok(());
        }

        let mut servers = self.servers.lock().await;
        if servers.contains_key(name) {
            // Already replicating so skip.
            return Ok(());
        }

        let (leave_tx, mut leave_rx) = mpsc::unbounded_channel();
        servers.insert(name.to_string(), leave_tx);

        // Start replication task.
        let local_server = self.local_server.clone();
        let close_tx = self.close_tx.clone();
        let closed = self.closed.clone();
        let tasks = self.tasks.clone();
        let servers_clone = self.servers.clone();
        let name_owned = name.to_string();
        let addr_owned = addr.to_string();

        let task = tokio::spawn(async move {
            Self::replicate(addr_owned, local_server, close_tx, closed, &mut leave_rx).await;

            // Clean up task when done.
            let mut tasks = tasks.lock().await;
            tasks.remove(&name_owned);

            let mut servers = servers_clone.lock().await;
            servers.remove(&name_owned);
        });

        let mut tasks = self.tasks.lock().await;
        tasks.insert(name.to_string(), task);

        Ok(())
    }

    async fn replicate(
        addr: String,
        mut local_server: LogServiceClient<Channel>,
        close_tx: Arc<Mutex<Option<mpsc::UnboundedSender<()>>>>,
        closed: Arc<AtomicBool>,
        leave_rx: &mut mpsc::UnboundedReceiver<()>,
    ) {
        // Connect to remote server.
        let mut client = match LogServiceClient::connect(addr.clone()).await {
            Ok(c) => c,
            Err(e) => {
                Self::log_error(&LogError::TransportError(e), "failed to dial", &addr);
                return;
            }
        };

        // Start consume stream.
        let request = ConsumeRequest { offset: 0 };
        let mut stream = match client.consume_stream(request).await {
            Ok(s) => s.into_inner(),
            Err(e) => {
                Self::log_error(&LogError::GrpcError(e), "failed to consume", &addr);
                return;
            }
        };

        // Process records.
        loop {
            tokio::select! {
                _ = leave_rx.recv() => {
                    return;
                }
                _ = async {
                    if closed.load(Ordering::Relaxed) {
                        return;
                    }
                    let close_tx = close_tx.lock().await;
                    if let Some(ref tx) = *close_tx {
                        let _ = tx.send(());
                    }
                } => {
                    return;
                }
                result = stream.message() => {
                    match result {
                        Ok(Some(response)) => {
                            // Produce record to local server.
                            let record = response.record.unwrap_or_default();
                            let request = ProduceRequest {
                                record: Some(record),
                            };

                            if let Err(e) = local_server.produce(request).await {
                                Self::log_error(&LogError::GrpcError(e), "failed to produce", &addr);
                                return;
                            }
                        }
                        Ok(None) => {
                            // Stream ended.
                            return;
                        }
                        Err(e) => {
                            Self::log_error(&LogError::GrpcError(e), "failed to receive", &addr);
                            return;
                        }
                    }
                }
            }
        }
    }

    pub async fn leave(&self, name: &str) -> LogResult<()> {
        let mut servers = self.servers.lock().await;
        if let Some(leave_tx) = servers.remove(name) {
            let _ = leave_tx.send(());
        }
        Ok(())
    }

    pub async fn close(&self) -> LogResult<()> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }

        // Signal all tasks to close.
        let (close_tx, _) = mpsc::unbounded_channel();
        {
            let mut tx_guard = self.close_tx.lock().await;
            *tx_guard = Some(close_tx);
        }

        // Wait for all tasks to complete.
        let mut tasks = self.tasks.lock().await;
        for (_, task) in tasks.drain() {
            let _ = task.await;
        }

        Ok(())
    }

    fn log_error(err: &LogError, msg: &str, addr: &str) {
        tracing::error!("{}: addr={}, error={}", msg, addr, err);
    }
}
