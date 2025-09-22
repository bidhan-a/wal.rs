use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Server};

use api::log_service_client::LogServiceClient;
use discovery::membership::{Config as DiscoveryConfig, Handler, Membership};
use log::log::Log;
use log::replicator::Replicator;
use server::GrpcLogService;

use crate::error::{AgentError, AgentResult};

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind_addr: SocketAddr,
    pub rpc_port: u16,
    pub node_name: String,
    pub start_join_addrs: Vec<SocketAddr>,
}

impl Config {
    pub fn rpc_addr(&self) -> AgentResult<SocketAddr> {
        let rpc_addr = SocketAddr::new(self.bind_addr.ip(), self.rpc_port);
        Ok(rpc_addr)
    }
}

pub struct Agent {
    config: Config,
    log: Arc<Log>,
    server: Option<JoinHandle<()>>,
    membership: Arc<Membership>,
    replicator: Arc<Replicator>,
    shutdown: Arc<AtomicBool>,
}

impl Agent {
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn new(config: Config) -> AgentResult<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));

        // Setup log.
        let log = Self::setup_log(&config).await?;

        // Setup server.
        let server = Self::setup_server(&config, log.clone()).await?;

        // Give the server a moment to start up.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Setup membership and replicator.
        let (membership, replicator) = Self::setup_membership(&config, log.clone()).await?;

        Ok(Self {
            config,
            log,
            server: Some(server),
            membership,
            replicator,
            shutdown,
        })
    }

    async fn setup_log(config: &Config) -> AgentResult<Arc<Log>> {
        let cfg = cfg::Config {
            log: cfg::LogConfig {
                segment: cfg::SegmentConfig {
                    max_store_size: 1024,
                    max_index_size: 1024,
                    initial_offset: 0,
                },
                durability: cfg::DurabilityPolicy::Never,
            },
        };
        let log = Log::new(&config.data_dir, cfg)?;
        Ok(Arc::new(log))
    }

    async fn setup_server(config: &Config, log: Arc<Log>) -> AgentResult<JoinHandle<()>> {
        let rpc_addr = config.rpc_addr()?;

        // Create log service.
        let log_service = GrpcLogService::new((*log).clone());

        // Start gRPC server.
        let server_task = tokio::spawn(async move {
            let server = Server::builder()
                .add_service(api::log_service_server::LogServiceServer::new(log_service))
                .serve(rpc_addr);

            if let Err(e) = server.await {
                tracing::error!("gRPC server error: {}", e);
            }
        });

        Ok(server_task)
    }

    async fn setup_membership(
        config: &Config,
        _log: Arc<Log>,
    ) -> AgentResult<(Arc<Membership>, Arc<Replicator>)> {
        let rpc_addr = config.rpc_addr()?;

        // Create gRPC client for replicator.
        let channel = Channel::from_shared(format!("http://{}", rpc_addr))
            .map_err(|e| AgentError::ConfigError(format!("Invalid RPC address: {}", e)))?
            .connect()
            .await?;
        let client = LogServiceClient::new(channel);

        // Create replicator.
        let replicator = Arc::new(Replicator::new(client));

        // Create membership handler.
        let handler = Arc::new(MembershipHandler {
            replicator: replicator.clone(),
        });

        // Create discovery config.
        let mut tags = HashMap::new();
        tags.insert("rpc_addr".to_string(), rpc_addr.to_string());

        let discovery_config = DiscoveryConfig {
            node_name: config.node_name.clone().into(),
            bind_addr: config.bind_addr,
            rpc_addr,
            tags,
            start_join_addrs: config.start_join_addrs.clone(),
        };

        // Create membership.
        let membership = Arc::new(Membership::with_config(handler, discovery_config).await?);

        Ok((membership, replicator))
    }

    pub async fn shutdown(&self) -> AgentResult<()> {
        if self.shutdown.swap(true, Ordering::Relaxed) {
            return Ok(());
        }

        // Leave membership.
        self.membership.leave().await?;

        // Close replicator.
        self.replicator.close().await?;

        // Shutdown server.
        if let Some(server_task) = &self.server {
            server_task.abort();
        }

        // Close log.
        self.log.close()?;

        Ok(())
    }
}

// Handler for membership events.
struct MembershipHandler {
    replicator: Arc<Replicator>,
}

impl Handler for MembershipHandler {
    fn join(
        &self,
        name: &str,
        addr: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let replicator = self.replicator.clone();
        let name = name.to_string();
        let addr = addr.to_string();

        tokio::spawn(async move {
            if let Err(e) = replicator.join(&name, &addr).await {
                tracing::error!("Failed to join replica {}: {}", name, e);
            }
        });

        Ok(())
    }

    fn leave(
        &self,
        name: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let replicator = self.replicator.clone();
        let name = name.to_string();

        tokio::spawn(async move {
            if let Err(e) = replicator.leave(&name).await {
                tracing::error!("Failed to leave replica {}: {}", name, e);
            }
        });

        Ok(())
    }
}
