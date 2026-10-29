use std::sync::Arc;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpStream, TcpListener, UdpSocket};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use realm_core::tcp::{bind_tcp, run_tcp_with};
use realm_core::udp::{bind_udp, run_udp_with};
use realm_core::endpoint::{Endpoint, RemoteAddr};

fn endpoint(laddr: &str, raddr: &str) -> Endpoint {
    Endpoint {
        laddr: laddr.parse().unwrap(),
        raddr: raddr.parse::<SocketAddr>().map(RemoteAddr::SocketAddr).unwrap(),
        conn_opts: Default::default(),
        bind_opts: Default::default(),
        extra_raddrs: Vec::new(),
    }
}

// connect through the relay, return the stream and the peer accepted by remote
async fn connect_via(laddr: &str, remote: &TcpListener) -> (TcpStream, TcpStream) {
    let local = TcpStream::connect(laddr).await.unwrap();
    let (peer, _) = timeout(Duration::from_secs(3), remote.accept()).await.unwrap().unwrap();
    (local, peer)
}

async fn ping(a: &mut TcpStream, b: &mut TcpStream) {
    let mut buf = [0u8; 4];
    a.write_all(b"ping").await.unwrap();
    b.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
}

#[tokio::test]
async fn tcp_reload() {
    let laddr = "127.0.0.1:10100";
    let remote1 = TcpListener::bind("127.0.0.1:20100").await.unwrap();
    let remote2 = TcpListener::bind("127.0.0.1:20101").await.unwrap();

    let ep = endpoint(laddr, "127.0.0.1:20100");
    let lis = bind_tcp(&ep).unwrap();
    let (tx, rx) = watch::channel(Arc::new(ep));
    let relay = tokio::spawn(run_tcp_with(lis, rx));

    let (mut old_local, mut old_peer) = connect_via(laddr, &remote1).await;
    ping(&mut old_local, &mut old_peer).await;

    // switch remote, new connections go to remote2
    tx.send(Arc::new(endpoint(laddr, "127.0.0.1:20101"))).unwrap();
    tokio::task::yield_now().await;
    let (mut new_local, mut new_peer) = connect_via(laddr, &remote2).await;
    ping(&mut new_local, &mut new_peer).await;

    // established connection is not affected
    ping(&mut old_local, &mut old_peer).await;

    // stop, listener is released while established connections stay alive
    drop(tx);
    timeout(Duration::from_secs(3), relay).await.unwrap().unwrap().unwrap();
    ping(&mut old_local, &mut old_peer).await;
    ping(&mut new_peer, &mut new_local).await;
    TcpListener::bind(laddr).await.unwrap();
}

#[tokio::test]
async fn udp_reload() {
    let laddr = "127.0.0.1:10101";
    let remote1 = UdpSocket::bind("127.0.0.1:20102").await.unwrap();
    let remote2 = UdpSocket::bind("127.0.0.1:20103").await.unwrap();

    let ep = endpoint(laddr, "127.0.0.1:20102");
    let lis = bind_udp(&ep).unwrap();
    let (tx, rx) = watch::channel(Arc::new(ep));
    let relay = tokio::spawn(run_udp_with(lis, rx));

    let mut buf = [0u8; 16];
    let client1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client1.send_to(b"ping", laddr).await.unwrap();
    let (n, _) = timeout(Duration::from_secs(3), remote1.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"ping");

    // switch remote, new associations go to remote2
    tx.send(Arc::new(endpoint(laddr, "127.0.0.1:20103"))).unwrap();
    let client2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client2.send_to(b"pong", laddr).await.unwrap();
    let (n, _) = timeout(Duration::from_secs(3), remote2.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"pong");

    // stop, socket and associations are released
    drop(tx);
    timeout(Duration::from_secs(3), relay).await.unwrap().unwrap().unwrap();
    UdpSocket::bind(laddr).await.unwrap();
}
