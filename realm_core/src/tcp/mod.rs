//! TCP relay entrance.

mod socket;
mod middle;
mod plain;

#[cfg(feature = "hook")]
mod hook;

#[cfg(feature = "proxy")]
mod proxy;

#[cfg(feature = "transport")]
mod transport;

use std::io::{ErrorKind, Result};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::endpoint::Endpoint;

use middle::connect_and_relay;

/// Launch a tcp relay.
pub async fn run_tcp(endpoint: Endpoint) -> Result<()> {
    let lis = bind_tcp(&endpoint).unwrap_or_else(|e| panic!("[tcp]failed to bind {}: {}", endpoint.laddr, e));

    // keep the sender alive, so that the relay never stops
    let (_tx, rx) = watch::channel(Arc::new(endpoint));
    run_tcp_with(lis, rx).await
}

/// Bind a tcp listener on the local address of an endpoint.
pub fn bind_tcp(endpoint: &Endpoint) -> Result<TcpListener> {
    socket::bind(&endpoint.laddr, endpoint.bind_opts.clone())
}

/// Launch a tcp relay on a bound listener, whose endpoint can be updated at runtime.
///
/// New connections always use the latest endpoint, while established connections keep the old one.
/// Local address and bind options of an updated endpoint are ignored.
///
/// The relay stops accepting new connections once the sender is dropped.
pub async fn run_tcp_with(lis: TcpListener, mut endpoint: watch::Receiver<Arc<Endpoint>>) -> Result<()> {
    let mut ep = endpoint.borrow_and_update().clone();
    let mut keepalive = socket::keepalive::build(&ep.conn_opts);

    loop {
        let accepted = tokio::select! {
            biased;
            res = endpoint.changed() => {
                if res.is_err() {
                    break;
                }
                ep = endpoint.borrow_and_update().clone();
                keepalive = socket::keepalive::build(&ep.conn_opts);
                continue;
            }
            res = lis.accept() => res,
        };

        let (local, addr) = match accepted {
            Ok(x) => x,
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                log::warn!("[tcp]failed to accept: {}", e);
                continue;
            }
            Err(e) => {
                log::error!("[tcp]failed to accept: {}", e);
                break;
            }
        };

        // ignore error
        let _ = local.set_nodelay(true);
        // set tcp_keepalive
        if let Some(kpa) = &keepalive {
            use socket::keepalive::SockRef;
            if let Err(e) = SockRef::from(&local).set_tcp_keepalive(kpa) {
                log::warn!("[tcp]{} failed to set keepalive: {}", addr, e);
            }
        }

        let ep = ep.clone();
        tokio::spawn(async move {
            match connect_and_relay(local, &ep).await {
                Ok(..) => log::debug!("[tcp]{} => {}, finish", addr, ep.raddr),
                Err(e) => log::error!("[tcp]{} => {}, error: {}", addr, ep.raddr, e),
            }
        });
    }

    Ok(())
}
