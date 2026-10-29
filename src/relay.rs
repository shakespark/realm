//! Running relays, which can be updated without restart.

use std::io::Result;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;

use log::Level;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use futures::future::join_all;

use realm_core::endpoint::Endpoint;
use realm_core::tcp::{bind_tcp, run_tcp_with};
use realm_core::udp::{bind_udp, run_udp_with};

use crate::conf::{EndpointConf, EndpointInfo};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Proto {
    Tcp,
    Udp,
}

impl Display for Proto {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Proto::Tcp => write!(f, "tcp"),
            Proto::Udp => write!(f, "udp"),
        }
    }
}

type Key = (SocketAddr, Proto);

enum Listener {
    Tcp(TcpListener),
    Udp(UdpSocket),
}

impl Listener {
    fn bind(proto: Proto, endpoint: &Endpoint) -> std::result::Result<Self, String> {
        let res = match proto {
            Proto::Tcp => bind_tcp(endpoint).map(Listener::Tcp),
            Proto::Udp => bind_udp(endpoint).map(Listener::Udp),
        };
        res.map_err(|e| format!("[{}]failed to bind {}: {}", proto, endpoint.laddr, e))
    }
}

struct Relay {
    conf: EndpointConf,
    endpoint: watch::Sender<Arc<Endpoint>>,
    task: JoinHandle<Result<()>>,
}

impl Relay {
    fn spawn(lis: Listener, conf: EndpointConf, endpoint: Arc<Endpoint>) -> Self {
        let (endpoint, rx) = watch::channel(endpoint);
        let task = match lis {
            Listener::Tcp(lis) => tokio::spawn(run_tcp_with(lis, rx)),
            Listener::Udp(lis) => tokio::spawn(run_udp_with(lis, rx)),
        };
        Relay { conf, endpoint, task }
    }

    // wait until the listener is released
    async fn stop(self) {
        let Relay { endpoint, task, .. } = self;
        drop(endpoint);
        // the handle of a finished task may have been polled to completion by `Relays::join`
        if !task.is_finished() {
            let _ = task.await;
        }
    }
}

/// Print to stdout/stderr when the logger does not take it, so that it is always visible.
pub fn report(level: Level, msg: std::fmt::Arguments) {
    use std::io::{self, Write};
    if log::log_enabled!(level) {
        log::log!(level, "{}", msg);
        return;
    }
    // ignore error, e.g. the terminal is gone
    let _ = match level {
        Level::Error | Level::Warn => writeln!(io::stderr(), "{}", msg),
        _ => writeln!(io::stdout(), "{}", msg),
    };
}

type Wanted = HashMap<Key, (EndpointConf, Arc<Endpoint>)>;

fn wanted(endpoints: Vec<(EndpointConf, EndpointInfo)>) -> std::result::Result<Wanted, String> {
    let mut wanted = HashMap::new();
    for (conf, info) in endpoints {
        let EndpointInfo {
            no_tcp,
            use_udp,
            endpoint,
        } = info;
        let endpoint = Arc::new(endpoint);
        let protos = [(!no_tcp, Proto::Tcp), (use_udp, Proto::Udp)];
        for (_, proto) in protos.into_iter().filter(|(on, _)| *on) {
            let key = (endpoint.laddr, proto);
            if wanted.insert(key, (conf.clone(), endpoint.clone())).is_some() {
                return Err(format!("duplicated listen address: [{}]{}", proto, endpoint.laddr));
            }
        }
    }
    Ok(wanted)
}

/// Relays keyed by local address and protocol.
#[derive(Default)]
pub struct Relays(HashMap<Key, Relay>);

impl Relays {
    /// Start endpoints, those failed to bind are reported and skipped.
    pub fn start(&mut self, endpoints: Vec<(EndpointConf, EndpointInfo)>) -> std::result::Result<(), String> {
        for (key, (conf, endpoint)) in wanted(endpoints)? {
            match Listener::bind(key.1, &endpoint) {
                Ok(lis) => {
                    self.0.insert(key, Relay::spawn(lis, conf, endpoint));
                }
                Err(e) => report(Level::Error, format_args!("{}", e)),
            }
        }
        Ok(())
    }

    /// Apply endpoints. If they are invalid or a new listener fails to bind, nothing is changed.
    ///
    /// - removed endpoints are stopped, established tcp connections are kept.
    /// - endpoints with changed bind options are stopped and started again,
    ///   if that fails, the old one is restored.
    /// - endpoints with other changes are updated in place, without rebinding.
    /// - new endpoints, and those exited, are started.
    pub async fn apply(&mut self, endpoints: Vec<(EndpointConf, EndpointInfo)>) -> std::result::Result<(), String> {
        let wanted = wanted(endpoints)?;

        // bind new listeners before anything is changed
        let mut fresh = HashMap::new();
        for (key, (_, endpoint)) in wanted.iter() {
            if !self.0.get(key).is_some_and(|relay| !relay.task.is_finished()) {
                fresh.insert(*key, Listener::bind(key.1, endpoint)?);
            }
        }

        // stop relays that are removed or have exited
        let stale: Vec<Key> = self
            .0
            .iter()
            .filter(|(key, relay)| !wanted.contains_key(key) || relay.task.is_finished())
            .map(|(key, _)| *key)
            .collect();
        for key in stale {
            self.0.remove(&key).unwrap().stop().await;
            if !wanted.contains_key(&key) {
                report(Level::Info, format_args!("stopped: [{}]{}", key.1, key.0));
            }
        }

        for (key, (conf, endpoint)) in wanted {
            let proto = key.1;
            if let Some(lis) = fresh.remove(&key) {
                report(Level::Info, format_args!("started: [{}]{}", proto, endpoint));
                self.0.insert(key, Relay::spawn(lis, conf, endpoint));
                continue;
            }

            let relay = self.0.get_mut(&key).unwrap();
            if relay.conf == conf {
                continue;
            }
            if relay.endpoint.borrow().bind_opts == endpoint.bind_opts {
                report(Level::Info, format_args!("updated: [{}]{}", proto, endpoint));
                relay.conf = conf;
                relay.endpoint.send_replace(endpoint);
                continue;
            }

            // rebind, the old listener must be closed first
            let old = self.0.remove(&key).unwrap();
            let (old_conf, old_endpoint) = (old.conf.clone(), old.endpoint.borrow().clone());
            old.stop().await;
            report(Level::Info, format_args!("stopped: [{}]{}", proto, key.0));
            let (conf, endpoint, lis) = match Listener::bind(proto, &endpoint) {
                Ok(lis) => (conf, endpoint, lis),
                Err(e) => {
                    report(Level::Error, format_args!("{}, restore the old one", e));
                    match Listener::bind(proto, &old_endpoint) {
                        Ok(lis) => (old_conf, old_endpoint, lis),
                        Err(e) => {
                            report(Level::Error, format_args!("{}", e));
                            continue;
                        }
                    }
                }
            };
            report(Level::Info, format_args!("started: [{}]{}", proto, endpoint));
            self.0.insert(key, Relay::spawn(lis, conf, endpoint));
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Wait until all relays exit.
    pub async fn join(&mut self) {
        join_all(self.0.values_mut().map(|relay| &mut relay.task)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;
    use crate::conf::{Config, FullConf};

    fn endpoints(conf: &str) -> Vec<(EndpointConf, EndpointInfo)> {
        let mut conf = FullConf::from_conf_str(conf).unwrap();
        conf.apply_global_opts();
        conf.endpoints.into_iter().map(|c| (c.clone(), c.build())).collect()
    }

    fn task_id(relays: &Relays, laddr: &str, proto: Proto) -> Option<tokio::task::Id> {
        relays.0.get(&(laddr.parse().unwrap(), proto)).map(|r| r.task.id())
    }

    // connect through the relay, return the remote that accepts it
    async fn route(laddr: &str, remotes: &[&TcpListener]) -> usize {
        let _local = TcpStream::connect(laddr).await.unwrap();
        let accepts = remotes.iter().map(|r| Box::pin(r.accept()));
        let (res, idx, _) = timeout(Duration::from_secs(3), futures::future::select_all(accepts))
            .await
            .unwrap();
        res.unwrap();
        idx
    }

    #[tokio::test]
    async fn update_rebind_and_stop() {
        let laddr = "127.0.0.1:17000";
        let r0 = TcpListener::bind("127.0.0.1:27000").await.unwrap();
        let r1 = TcpListener::bind("127.0.0.1:27001").await.unwrap();
        let mut relays = Relays::default();

        let conf = |remote: &str, extra: &str| {
            format!(
                "[[endpoints]]\nlisten = \"{}\"\nremote = \"{}\"\n{}",
                laddr, remote, extra
            )
        };

        relays.apply(endpoints(&conf("127.0.0.1:27000", ""))).await.unwrap();
        let id = task_id(&relays, laddr, Proto::Tcp).unwrap();
        assert_eq!(route(laddr, &[&r0, &r1]).await, 0);

        // same config, nothing changes
        relays.apply(endpoints(&conf("127.0.0.1:27000", ""))).await.unwrap();
        assert_eq!(task_id(&relays, laddr, Proto::Tcp), Some(id));

        // remote changes, updated in place
        relays.apply(endpoints(&conf("127.0.0.1:27001", ""))).await.unwrap();
        assert_eq!(task_id(&relays, laddr, Proto::Tcp), Some(id));
        assert_eq!(route(laddr, &[&r0, &r1]).await, 1);

        // bind options change, rebind
        let ipv6_only = "network = { ipv6_only = true }";
        relays
            .apply(endpoints(&conf("127.0.0.1:27001", ipv6_only)))
            .await
            .unwrap();
        let id2 = task_id(&relays, laddr, Proto::Tcp).unwrap();
        assert_ne!(id2, id);
        assert_eq!(route(laddr, &[&r0, &r1]).await, 1);

        // udp enabled, tcp kept
        let udp = "network = { ipv6_only = true, use_udp = true }";
        relays.apply(endpoints(&conf("127.0.0.1:27001", udp))).await.unwrap();
        assert!(task_id(&relays, laddr, Proto::Udp).is_some());
        assert_eq!(task_id(&relays, laddr, Proto::Tcp), Some(id2));

        // removed, port released
        relays.apply(Vec::new()).await.unwrap();
        assert!(relays.0.is_empty());
        TcpListener::bind(laddr).await.unwrap();
        tokio::net::UdpSocket::bind(laddr).await.unwrap();
    }

    #[tokio::test]
    async fn invalid_endpoints() {
        let mut relays = Relays::default();
        let one = "[[endpoints]]\nlisten = \"127.0.0.1:17010\"\nremote = \"127.0.0.1:27010\"\n";
        relays.apply(endpoints(one)).await.unwrap();
        let id = task_id(&relays, "127.0.0.1:17010", Proto::Tcp);

        // duplicated listen address, nothing changes
        let dup = format!(
            "{0}{0}[[endpoints]]\nlisten = \"127.0.0.1:17011\"\nremote = \"127.0.0.1:27010\"\n",
            one
        );
        assert!(relays.apply(endpoints(&dup)).await.is_err());
        assert_eq!(relays.0.len(), 1);
        assert_eq!(task_id(&relays, "127.0.0.1:17010", Proto::Tcp), id);

        // a port in use, nothing changes
        let _occupied = std::net::TcpListener::bind("127.0.0.1:17012").unwrap();
        let busy = "[[endpoints]]\nlisten = \"127.0.0.1:17012\"\nremote = \"127.0.0.1:27010\"\n[[endpoints]]\nlisten = \"127.0.0.1:17013\"\nremote = \"127.0.0.1:27010\"\n";
        assert!(relays.apply(endpoints(busy)).await.is_err());
        assert_eq!(relays.0.len(), 1);
        assert_eq!(task_id(&relays, "127.0.0.1:17010", Proto::Tcp), id);
        assert!(task_id(&relays, "127.0.0.1:17013", Proto::Tcp).is_none());

        // on start, a port in use does not block others
        let mut relays = Relays::default();
        relays.start(endpoints(busy)).unwrap();
        assert!(task_id(&relays, "127.0.0.1:17012", Proto::Tcp).is_none());
        assert!(task_id(&relays, "127.0.0.1:17013", Proto::Tcp).is_some());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn rebind_failure_restores() {
        let laddr = "127.0.0.1:17030";
        let remote = TcpListener::bind("127.0.0.1:27030").await.unwrap();
        let conf = |extra: &str| {
            format!(
                "[[endpoints]]\nlisten = \"{}\"\nremote = \"127.0.0.1:27030\"\n{}",
                laddr, extra
            )
        };
        let mut relays = Relays::default();
        relays.start(endpoints(&conf(""))).unwrap();

        // no such interface
        relays
            .apply(endpoints(&conf("listen_interface = \"nonexistent0\"")))
            .await
            .unwrap();
        let relay = &relays.0[&(laddr.parse().unwrap(), Proto::Tcp)];
        assert_eq!(relay.conf.listen_interface, None);
        assert_eq!(route(laddr, &[&remote]).await, 0);
    }

    #[tokio::test]
    async fn restart_exited() {
        let laddr = "127.0.0.1:17020";
        let remote = TcpListener::bind("127.0.0.1:27020").await.unwrap();
        let conf = format!("[[endpoints]]\nlisten = \"{}\"\nremote = \"127.0.0.1:27020\"\n", laddr);
        let mut relays = Relays::default();
        relays.start(endpoints(&conf)).unwrap();

        // the relay exits on its own, e.g. a fatal accept error
        let key = (laddr.parse().unwrap(), Proto::Tcp);
        let relay = relays.0.get_mut(&key).unwrap();
        relay.task.abort();
        while !relay.task.is_finished() {
            tokio::task::yield_now().await;
        }

        relays.apply(endpoints(&conf)).await.unwrap();
        assert!(!relays.0[&key].task.is_finished());
        assert_eq!(route(laddr, &[&remote]).await, 0);
    }
}
