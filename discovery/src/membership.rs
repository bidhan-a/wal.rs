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
        config: Config,
        tx: oneshot::Sender<Result<()>>,
    },
    List {
        tx: oneshot::Sender<Result<Vec<Config>>>,
    },
    Join {
        addr: SocketAddr,
        id: NodeId,
        tx: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    members: SkipMap<SmolStr, Config>,
    tx: UnboundedSender<Event>,
    config: Config,
    handler: Arc<dyn Handler>,
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
                members: SkipMap::new(),
                tx,
                config: default_config,
                handler: noop_handler,
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
                members: SkipMap::new(),
                tx,
                config: config.clone(),
                handler,
            }
            .into(),
        };

        this.clone().handle_serf_events(subscriber);
        this.clone().handle_internal_events(rx);

        // announce ourselves.
        this.register(config.clone()).await?;
        // start joins.
        for addr in &this.inner.config.start_join_addrs {
            let id = NodeId::new(addr.to_string())?;
            let _ = this.join(id, *addr).await;
        }
        Ok(this)
    }

    pub async fn register(&self, config: Config) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.inner.tx.send(Event::Register { config, tx }) {
            return Err(Box::new(e));
        }
        rx.await??;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<Config>> {
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

    pub fn members(&self) -> Vec<Config> {
        self.inner
            .members
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    pub fn member_count(&self) -> usize {
        self.inner.members.len()
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
                            Some(Event::Register { config, tx }) => {
                                match serialize(&config) {
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
                                self.inner.members.insert(config.node_name.clone(), config);
                            }
                            Some(Event::List { tx }) => {
                                let members = self.inner.members.iter().map(|ent| ent.value().clone()).collect();
                                let _ = tx.send(Ok(members));
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
                                        let config: Config = match deserialize(payload) {
                                            Ok(config) => config,
                                            Err(e) => {
                                                tracing::error!(err=%e, "membership: failed to decode register event");
                                                continue;
                                            }
                                        };
                                        if config.node_name != self.inner.config.node_name {
                                            let _ = self.inner.handler.join(&config.node_name, &config.rpc_addr.to_string());
                                        }
                                        self.inner.members.insert(config.node_name.clone(), config.clone());
                                    }
                                    "deregister" => {
                                        if let Ok(addr) = deserialize::<SocketAddr>(ev.payload()) {
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
            .members
            .iter()
            .filter(|ent| ent.value().rpc_addr == addr)
            .map(|ent| ent.key().clone())
            .collect();
        for key in keys {
            let _ = self.inner.members.remove(&key);
            let _ = self.inner.handler.leave(key.as_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Builder;
    use tokio::time::{Duration, Instant, sleep};

    struct TestHandler {
        joins: tokio::sync::mpsc::UnboundedSender<(String, String)>,
        leaves: tokio::sync::mpsc::UnboundedSender<String>,
    }

    impl Handler for TestHandler {
        fn join(&self, name: &str, addr: &str) -> Result<()> {
            let _ = self.joins.send((name.to_string(), addr.to_string()));
            Ok(())
        }
        fn leave(&self, name: &str) -> Result<()> {
            let _ = self.leaves.send(name.to_string());
            Ok(())
        }
    }

    async fn start_member_with(
        bind: SocketAddr,
        start_join: Vec<SocketAddr>,
        joins_tx: tokio::sync::mpsc::UnboundedSender<(String, String)>,
        leaves_tx: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Membership {
        let handler = Arc::new(TestHandler {
            joins: joins_tx,
            leaves: leaves_tx,
        });
        let cfg = Config {
            node_name: SmolStr::from(format!("{}", bind)),
            bind_addr: bind,
            rpc_addr: bind,
            tags: HashMap::new(),
            start_join_addrs: start_join,
        };
        Membership::with_config(handler, cfg).await.unwrap()
    }

    #[test]
    fn test_membership() {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let (jt, jr) = tokio::sync::mpsc::unbounded_channel();
            let (lt, mut lr) = tokio::sync::mpsc::unbounded_channel();

            let ports = [53131, 53132, 53133];
            let addrs: Vec<SocketAddr> = ports
                .iter()
                .map(|p| format!("127.0.0.1:{}", p).parse().unwrap())
                .collect();

            let m0 = start_member_with(addrs[0], vec![], jt.clone(), lt.clone()).await;
            let _m1 = start_member_with(addrs[1], vec![addrs[0]], jt.clone(), lt.clone()).await;
            let m2 = start_member_with(addrs[2], vec![addrs[0]], jt.clone(), lt.clone()).await;

            let start = Instant::now();
            loop {
                let joins_ok = jr.len() >= 2;
                let mem_ok = m0.member_count() == 3;
                let leaves_ok = lr.len() == 0;
                if joins_ok && mem_ok && leaves_ok {
                    break;
                }
                if start.elapsed() > Duration::from_secs(3) {
                    panic!("initial membership not reached");
                }
                sleep(Duration::from_millis(250)).await;
            }

            m2.leave().await.unwrap();

            // Give some time for the leave event to propagate.
            sleep(Duration::from_millis(100)).await;

            let start = Instant::now();
            loop {
                let joins_ok = jr.len() >= 2;
                let mem_ok = m0.member_count() == 3;
                let leaves_ok = lr.len() >= 1;
                if joins_ok && mem_ok && leaves_ok {
                    break;
                }
                if start.elapsed() > Duration::from_secs(5) {
                    panic!("leave not observed in time");
                }
                sleep(Duration::from_millis(250)).await;
            }

            let left_name = lr.recv().await.unwrap();
            assert_eq!(left_name, format!("{}", addrs[2]));
        });
    }
}
