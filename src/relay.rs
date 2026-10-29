//! Running relays, which can be updated without restart.

use std::io::Result;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;

use log::Level;
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

struct Relay {
    conf: EndpointConf,
    endpoint: watch::Sender<Arc<Endpoint>>,
    task: JoinHandle<Result<()>>,
}

impl Relay {
    fn start(proto: Proto, conf: EndpointConf, endpoint: Arc<Endpoint>) -> Result<Self> {
        let (endpoint, task) = match proto {
            Proto::Tcp => {
                let lis = bind_tcp(&endpoint)?;
                let (tx, rx) = watch::channel(endpoint);
                (tx, tokio::spawn(run_tcp_with(lis, rx)))
            }
            Proto::Udp => {
                let lis = bind_udp(&endpoint)?;
                let (tx, rx) = watch::channel(endpoint);
                (tx, tokio::spawn(run_udp_with(lis, rx)))
            }
        };
        Ok(Relay { conf, endpoint, task })
    }

    // wait until the listener is released
    async fn stop(self) {
        let Relay { endpoint, task, .. } = self;
        drop(endpoint);
        let _ = task.await;
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

/// Relays keyed by local address and protocol.
#[derive(Default)]
pub struct Relays(HashMap<Key, Relay>);

impl Relays {
    /// Apply endpoints, nothing is changed if they are invalid.
    ///
    /// - removed endpoints are stopped, established tcp connections are kept.
    /// - endpoints with changed bind options are stopped and started again.
    /// - endpoints with other changes are updated in place, without rebinding.
    /// - new endpoints are started.
    pub async fn apply(&mut self, endpoints: Vec<(EndpointConf, EndpointInfo)>) -> std::result::Result<(), String> {
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

        // stop relays that are removed, need to rebind, or have exited
        let stale: Vec<Key> = self
            .0
            .iter()
            .filter(|(key, relay)| match wanted.get(key) {
                None => true,
                Some((_, endpoint)) => {
                    relay.endpoint.borrow().bind_opts != endpoint.bind_opts || relay.task.is_finished()
                }
            })
            .map(|(key, _)| *key)
            .collect();
        for key in stale {
            let relay = self.0.remove(&key).unwrap();
            relay.stop().await;
            report(Level::Info, format_args!("stopped: [{}]{}", key.1, key.0));
        }

        // update or start relays
        for (key, (conf, endpoint)) in wanted {
            let proto = key.1;
            if let Some(relay) = self.0.get_mut(&key) {
                if relay.conf != conf {
                    report(Level::Info, format_args!("updated: [{}]{}", proto, endpoint));
                    relay.conf = conf;
                    relay.endpoint.send_replace(endpoint);
                }
                continue;
            }

            match Relay::start(proto, conf, endpoint.clone()) {
                Ok(relay) => {
                    report(Level::Info, format_args!("started: [{}]{}", proto, endpoint));
                    self.0.insert(key, relay);
                }
                Err(e) => report(Level::Error, format_args!("[{}]failed to bind {}: {}", proto, key.0, e)),
            }
        }

        Ok(())
    }

    /// Wait until all relays exit.
    pub async fn join(&mut self) {
        join_all(self.0.values_mut().map(|relay| &mut relay.task)).await;
    }
}
