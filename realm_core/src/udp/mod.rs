//! UDP relay entrance.

mod socket;
mod sockmap;
mod middle;
mod batched;

use std::io::Result;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::endpoint::Endpoint;

use sockmap::SockMap;
use middle::associate_and_relay;

/// Launch a udp relay.
pub async fn run_udp(endpoint: Endpoint) -> Result<()> {
    let lis = bind_udp(&endpoint).unwrap_or_else(|e| panic!("[udp]failed to bind {}: {}", endpoint.laddr, e));

    // keep the sender alive, so that the relay never stops
    let (_tx, rx) = watch::channel(Arc::new(endpoint));
    run_udp_with(lis, rx).await
}

/// Bind a udp socket on the local address of an endpoint.
pub fn bind_udp(endpoint: &Endpoint) -> Result<UdpSocket> {
    socket::bind(&endpoint.laddr, endpoint.bind_opts.clone())
}

/// Launch a udp relay on a bound socket, whose endpoint can be updated at runtime.
///
/// New associations always use the latest endpoint, while established associations keep the old one.
/// Local address and bind options of an updated endpoint are ignored.
///
/// The relay stops once the sender is dropped, all associations are closed as well.
pub async fn run_udp_with(lis: UdpSocket, mut endpoint: watch::Receiver<Arc<Endpoint>>) -> Result<()> {
    let lis = Arc::new(lis);
    let sockmap = Arc::new(SockMap::new());
    let mut associations = JoinSet::new();

    loop {
        match associate_and_relay(&lis, &mut endpoint, &sockmap, &mut associations).await {
            Ok(()) => break,
            Err(e) => log::error!("[udp]error: {}", e),
        }
    }

    // release the socket
    associations.shutdown().await;
    Ok(())
}
