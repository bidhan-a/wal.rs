use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bincode::{deserialize, serialize};
use crossbeam_skiplist::SkipMap;
use serf::{
    Options,
    delegate::CompositeDelegate,
    event::{Event as SerfEvent, EventProducer, EventSubscriber},
    net::{NetTransportOptions, Node, NodeId, TokioNetTransport},
    tokio::{TokioSocketAddrResolver, TokioTcp, TokioTcpSerf},
    types::{MaybeResolvedAddress, SmolStr},
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

type MembershipDelegate = CompositeDelegate<NodeId, SocketAddr>;

enum Event {
    Register {
        name: SmolStr,
        addr: SocketAddr,
        tx: oneshot::Sender<Result<()>>,
    },
    List {
        tx: oneshot::Sender<Result<Vec<Service>>>,
    },
    Join {
        addr: SocketAddr,
        id: NodeId,
        tx: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Service {
    name: SmolStr,
    addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberStatus {
    Alive,
    Left,
}

#[derive(Clone, Debug)]
pub struct Member {
    pub name: SmolStr,
    pub addr: SocketAddr,
    pub status: MemberStatus,
}

#[derive(Clone)]
pub struct Config {
    pub node_name: SmolStr,
    pub bind_addr: SocketAddr,
    pub rpc_addr: SocketAddr,
    pub tags: HashMap<String, String>,
    pub start_join_addrs: Vec<SocketAddr>,
}

pub trait Handler: Send + Sync + 'static {
    fn join(&self, name: &str, addr: &str) -> Result<()>;
    fn leave(&self, name: &str) -> Result<()>;
}

struct NoopHandler;
impl Handler for NoopHandler {
    fn join(&self, _name: &str, _addr: &str) -> Result<()> {
        Ok(())
    }
    fn leave(&self, _name: &str) -> Result<()> {
        Ok(())
    }
}

struct Inner {
    serf: TokioTcpSerf<NodeId, TokioSocketAddrResolver, MembershipDelegate>,
    services: SkipMap<SmolStr, Service>,
    tx: UnboundedSender<Event>,
    config: Config,
    handler: Arc<dyn Handler>,
    members: SkipMap<SmolStr, Member>,
}

#[derive(Clone)]
pub struct Membership {
    inner: Arc<Inner>,
}

impl Membership {
    pub async fn new(
        opts: Options,
        net_opts: NetTransportOptions<NodeId, TokioSocketAddrResolver, TokioTcp>,
    ) -> Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (producer, subscriber) = EventProducer::unbounded();
        let serf =
            TokioTcpSerf::with_event_producer(net_opts, opts.with_event_buffer_size(256), producer)
                .await?;

        let default_config = Config {
            node_name: SmolStr::from("node"),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            rpc_addr: "127.0.0.1:0".parse().unwrap(),
            tags: HashMap::new(),
            start_join_addrs: Vec::new(),
        };
        let noop_handler: Arc<dyn Handler> = Arc::new(NoopHandler);

        let this = Self {
            inner: Inner {
                serf,
                services: SkipMap::new(),
                tx,
                config: default_config,
                handler: noop_handler,
                members: SkipMap::new(),
            }
            .into(),
        };

        this.clone().handle_serf_events(subscriber);
        this.clone().handle_internal_events(rx);

        Ok(this)
    }

    pub async fn with_config(handler: Arc<dyn Handler>, config: Config) -> Result<Self> {
        let node_id = NodeId::new(config.bind_addr.to_string())?;
        let net_opts = NetTransportOptions::new(node_id)
            .with_bind_addresses([config.bind_addr].into_iter().collect());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (producer, subscriber) = EventProducer::unbounded();
        let serf = TokioTcpSerf::with_event_producer(
            net_opts,
            Options::new().with_event_buffer_size(256),
            producer,
        )
        .await?;

        let this = Self {
            inner: Inner {
                serf,
                services: SkipMap::new(),
                tx,
                config: config.clone(),
                handler,
                members: SkipMap::new(),
            }
            .into(),
        };

        this.clone().handle_serf_events(subscriber);
        this.clone().handle_internal_events(rx);

        // announce ourselves.
        this.register(config.node_name.clone(), config.rpc_addr)
            .await?;
        // start joins.
        for addr in &this.inner.config.start_join_addrs {
            let id = NodeId::new(addr.to_string())?;
            let _ = this.join(id, *addr).await;
        }
        Ok(this)
    }

    pub async fn register(&self, name: SmolStr, addr: SocketAddr) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.inner.tx.send(Event::Register { name, addr, tx }) {
            return Err(Box::new(e));
        }
        rx.await??;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<Service>> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.inner.tx.send(Event::List { tx }) {
            return Err(Box::new(e));
        }
        rx.await?
    }

    pub async fn join(&self, id: NodeId, addr: SocketAddr) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.inner.tx.send(Event::Join { addr, id, tx }) {
            return Err(Box::new(e));
        }
        rx.await??;
        Ok(())
    }

    pub fn is_local(&self, name: &str) -> bool {
        self.inner.config.node_name.as_str() == name
    }

    pub fn members(&self) -> Vec<Member> {
        self.inner
            .members
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    pub async fn leave(&self) -> Result<()> {
        let payload = serialize(&self.inner.config.rpc_addr)?;
        let _ = self
            .inner
            .serf
            .user_event("deregister", payload, false)
            .await?;
        Ok(())
    }

    fn handle_internal_events(self, mut rx: UnboundedReceiver<Event>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("membership: shutting down internal event loop");
                        break;
                    }
                    ev = rx.recv() => {
                        match ev {
                            Some(Event::Register { name, addr, tx }) => {
                                let service = Service { name: name.clone(), addr };
                                match serialize(&service) {
                                    Ok(data) => {
                                        match self.inner.serf.user_event("register", data, false).await {
                                            Ok(_) => { let _ = tx.send(Ok(())); }
                                            Err(e) => {
                                                tracing::error!(err=%e, "membership: failed to send register event");
                                                let _ = tx.send(Err(e.into()));
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(err=%e, "membership: failed to encode register event");
                                        let _ = tx.send(Err(e.into()));
                                    }
                                }
                                // update local maps immediately.
                                self.inner.services.insert(service.name.clone(), service);
                                self.inner.members.insert(name.clone(), Member { name, addr, status: MemberStatus::Alive });
                            }
                            Some(Event::List { tx }) => {
                                let services = self.inner.services.iter().map(|ent| ent.value().clone()).collect();
                                let _ = tx.send(Ok(services));
                            }
                            Some(Event::Join { id, addr, tx }) => {
                                let res = self.inner.serf.join(Node::new(id, MaybeResolvedAddress::Resolved(addr)), false).await;
                                let _ = tx.send(res.map_err(Into::into).map(|_| ()));
                            }
                            None => { break; }
                        }
                    }
                }
            }
        });
    }

    fn handle_serf_events(
        self,
        subscriber: EventSubscriber<
            TokioNetTransport<NodeId, TokioSocketAddrResolver, TokioTcp>,
            MembershipDelegate,
        >,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("membership: shutting down serf event listener");
                        break;
                    }
                    ev = subscriber.recv() => {
                        match ev {
                            Ok(SerfEvent::User(ev)) => {
                                match ev.name().as_str() {
                                    "register" => {
                                        let payload = ev.payload();
                                        let service: Service = match deserialize(payload) {
                                            Ok(service) => service,
                                            Err(e) => {
                                                tracing::error!(err=%e, "membership: failed to decode register event");
                                                continue;
                                            }
                                        };
                                        if service.name != self.inner.config.node_name {
                                            let _ = self.inner.handler.join(&service.name, &service.addr.to_string());
                                        }
                                        self.inner.services.insert(service.name.clone(), service.clone());
                                        self.inner.members.insert(service.name.clone(), Member { name: service.name, addr: service.addr, status: MemberStatus::Alive });
                                    }
                                    "deregister" => {
                                        if let Ok(addr) = deserialize::<SocketAddr>(ev.payload()) {
                                            // mark members with this addr as Left.
                                            let keys: Vec<SmolStr> = self.inner.members.iter().filter(|e| e.value().addr == addr).map(|e| e.key().clone()).collect();
                                            for k in keys {
                                                if let Some(mut entry) = self.inner.members.get(&k) {
                                                    let m = entry.value().clone();
                                                    self.inner.members.insert(k.clone(), Member { status: MemberStatus::Left, ..m });
                                                }
                                            }
                                            Self::remove_services_by_addr(&self, addr);
                                        }
                                    }
                                    other => {
                                        tracing::warn!("membership: unknown user event {}", other);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        });
    }

    fn remove_services_by_addr(&self, addr: SocketAddr) {
        let keys: Vec<SmolStr> = self
            .inner
            .services
            .iter()
            .filter(|ent| ent.value().addr == addr)
            .map(|ent| ent.key().clone())
            .collect();
        for key in keys {
            let _ = self.inner.services.remove(&key);
            let _ = self.inner.handler.leave(key.as_str());
        }
    }
}
