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

use std::future::{Future, pending};
use std::io::{ErrorKind, Result};
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};

use crate::endpoint::Endpoint;

use middle::connect_and_relay;
use socket::keepalive::{SockRef, TcpKeepalive};

/// Launch a tcp relay.
pub async fn run_tcp(endpoint: Endpoint) -> Result<()> {
    let lis = bind_tcp(&endpoint).unwrap_or_else(|e| panic!("[tcp]failed to bind {}: {}", endpoint.laddr, e));
    run_tcp_until(lis, endpoint, pending()).await
}

/// Bind a tcp listener on the local address of an endpoint.
pub fn bind_tcp(endpoint: &Endpoint) -> Result<TcpListener> {
    socket::bind(&endpoint.laddr, endpoint.bind_opts.clone())
}

/// Launch a tcp relay on a bound listener, until `shutdown` completes.
///
/// The listener is closed on return, established connections are not affected.
pub async fn run_tcp_until(lis: TcpListener, endpoint: Endpoint, shutdown: impl Future<Output = ()>) -> Result<()> {
    let endpoint = Arc::new(endpoint);
    let keepalive = socket::keepalive::build(&endpoint.conn_opts);
    let mut shutdown = pin!(shutdown);

    loop {
        let accepted = tokio::select! {
            biased;
            _ = &mut shutdown => {
                break;
            }
            res = lis.accept() => res,
        };

        match accepted {
            Ok((local, addr)) => relay(local, addr, &endpoint, &keepalive),
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                log::warn!("[tcp]failed to accept: {}", e);
            }
            Err(e) => {
                log::error!("[tcp]failed to accept: {}", e);
                break;
            }
        };
    }

    Ok(())
}

fn relay(local: TcpStream, addr: SocketAddr, endpoint: &Arc<Endpoint>, keepalive: &Option<TcpKeepalive>) {
    // ignore error
    let _ = local.set_nodelay(true);
    // set tcp_keepalive
    if let Some(kpa) = keepalive {
        if let Err(e) = SockRef::from(&local).set_tcp_keepalive(kpa) {
            log::warn!("[tcp]{} failed to set keepalive: {}", addr, e);
        }
    }

    let ep = endpoint.clone();
    tokio::spawn(async move {
        match connect_and_relay(local, &ep).await {
            Ok(..) => log::debug!("[tcp]{} => {}, finish", addr, ep.raddr),
            Err(e) => log::error!("[tcp]{} => {}, error: {}", addr, ep.raddr, e),
        }
    });
}
