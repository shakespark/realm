//! UDP relay entrance.

mod socket;
mod sockmap;
mod middle;
mod batched;

use std::future::{Future, pending};
use std::io::Result;
use std::pin::pin;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::task::JoinSet;

use crate::endpoint::Endpoint;

use sockmap::SockMap;
use middle::associate_and_relay;

/// Launch a udp relay.
pub async fn run_udp(endpoint: Endpoint) -> Result<()> {
    let lis = bind_udp(&endpoint).unwrap_or_else(|e| panic!("[udp]failed to bind {}: {}", endpoint.laddr, e));
    run_udp_until(lis, endpoint, pending()).await
}

/// Bind a udp socket on the local address of an endpoint.
pub fn bind_udp(endpoint: &Endpoint) -> Result<UdpSocket> {
    socket::bind(&endpoint.laddr, endpoint.bind_opts.clone())
}

/// Launch a udp relay on a bound socket, until `shutdown` completes.
///
/// All associations are closed and the socket is released on return.
pub async fn run_udp_until(lis: UdpSocket, endpoint: Endpoint, shutdown: impl Future<Output = ()>) -> Result<()> {
    let Endpoint { raddr, conn_opts, .. } = endpoint;

    let lis = Arc::new(lis);
    let sockmap = Arc::new(SockMap::new());
    let mut associations = JoinSet::new();
    let mut shutdown = pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            res = associate_and_relay(&lis, &raddr, &conn_opts, &sockmap, &mut associations) => {
                if let Err(e) = res {
                    log::error!("[udp]error: {}", e);
                }
            }
        }
    }

    associations.shutdown().await;
    Ok(())
}
