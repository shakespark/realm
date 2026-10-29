use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpStream, TcpListener, UdpSocket};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use realm_core::tcp::{bind_tcp, run_tcp_until};
use realm_core::udp::{bind_udp, run_udp_until};
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

async fn ping(a: &mut TcpStream, b: &mut TcpStream) {
    let mut buf = [0u8; 4];
    a.write_all(b"ping").await.unwrap();
    b.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
}

#[tokio::test]
async fn tcp_shutdown() {
    let laddr = "127.0.0.1:10100";
    let remote = TcpListener::bind("127.0.0.1:20100").await.unwrap();

    let ep = endpoint(laddr, "127.0.0.1:20100");
    let lis = bind_tcp(&ep).unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let relay = tokio::spawn(run_tcp_until(lis, ep, async {
        let _ = rx.await;
    }));

    let mut local = TcpStream::connect(laddr).await.unwrap();
    let (mut peer, _) = timeout(Duration::from_secs(3), remote.accept()).await.unwrap().unwrap();
    ping(&mut local, &mut peer).await;

    // listener is closed, established connections stay alive
    drop(tx);
    timeout(Duration::from_secs(3), relay).await.unwrap().unwrap().unwrap();
    ping(&mut local, &mut peer).await;
    ping(&mut peer, &mut local).await;
    TcpListener::bind(laddr).await.unwrap();
}

#[tokio::test]
async fn udp_shutdown() {
    let laddr = "127.0.0.1:10101";
    let remote = UdpSocket::bind("127.0.0.1:20101").await.unwrap();

    let ep = endpoint(laddr, "127.0.0.1:20101");
    let lis = bind_udp(&ep).unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let relay = tokio::spawn(run_udp_until(lis, ep, async {
        let _ = rx.await;
    }));

    let mut buf = [0u8; 16];
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.send_to(b"ping", laddr).await.unwrap();
    let (n, assoc) = timeout(Duration::from_secs(3), remote.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"ping");

    // socket and associations are closed
    drop(tx);
    timeout(Duration::from_secs(3), relay).await.unwrap().unwrap().unwrap();
    UdpSocket::bind(laddr).await.unwrap();
    UdpSocket::bind(assoc).await.unwrap();
}
