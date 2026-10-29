use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinSet;

use super::SockMap;
use super::{socket, batched};

use crate::time::timeoutfut;
use crate::dns::resolve_addr;
use crate::endpoint::Endpoint;

use batched::{Packet, SockAddrStore};
use registry::Registry;
mod registry {
    use super::*;
    type Range = std::ops::Range<u16>;

    pub struct Registry {
        pkts: Box<[Packet]>,
        groups: Vec<Range>,
        cursor: u16,
    }

    impl Registry {
        pub fn new(npkts: usize) -> Self {
            debug_assert!(npkts <= batched::MAX_PACKETS);
            Self {
                pkts: vec![Packet::new(); npkts].into_boxed_slice(),
                groups: Vec::with_capacity(npkts),
                cursor: 0u16,
            }
        }

        pub async fn batched_recv_on(&mut self, sock: &UdpSocket) -> Result<()> {
            let n = batched::recv_some(sock, &mut self.pkts).await?;
            self.cursor = n as u16;
            Ok(())
        }

        pub fn group_by_addr(&mut self) {
            let n = self.cursor as usize;
            self.groups.clear();
            group_by_inner(&mut self.pkts[..n], &mut self.groups, |a, b| a.addr == b.addr);
        }

        pub fn group_iter(&self) -> GroupIter<'_> {
            GroupIter {
                pkts: &self.pkts,
                ranges: self.groups.iter(),
            }
        }

        pub fn iter(&self) -> std::slice::Iter<'_, Packet> {
            self.pkts[..self.cursor as usize].iter()
        }

        pub const fn count(&self) -> usize {
            self.cursor as usize
        }
    }

    use std::slice::Iter;
    use std::iter::Iterator;
    pub struct GroupIter<'a> {
        pkts: &'a [Packet],
        ranges: Iter<'a, Range>,
    }

    impl<'a> Iterator for GroupIter<'a> {
        type Item = &'a [Packet];

        fn next(&mut self) -> Option<Self::Item> {
            self.ranges
                .next()
                .map(|Range { start, end }| &self.pkts[*start as usize..*end as usize])
        }
    }

    fn group_by_inner<T, F>(data: &mut [T], groups: &mut Vec<Range>, eq: F)
    where
        F: Fn(&T, &T) -> bool,
    {
        let maxn = data.len();
        let (mut beg, mut end) = (0, 1);
        while end < maxn {
            // go ahead if addr is same
            if eq(&data[end], &data[beg]) {
                end += 1;
                continue;
            }
            // pick packets afterwards
            let mut probe = end + 1;
            while probe < maxn {
                if eq(&data[probe], &data[beg]) {
                    data.swap(probe, end);
                    end += 1;
                }
                probe += 1;
            }
            groups.push(beg as _..end as _);
            (beg, end) = (end, end + 1);
        }
        groups.push(beg as _..end as _);
    }
}

/// Relay until the sender of endpoint is dropped.
pub async fn associate_and_relay(
    lis: &Arc<UdpSocket>,
    endpoint: &mut watch::Receiver<Arc<Endpoint>>,
    sockmap: &Arc<SockMap>,
    associations: &mut JoinSet<()>,
) -> Result<()> {
    let mut registry = Registry::new(batched::MAX_PACKETS);

    loop {
        tokio::select! {
            biased;
            res = endpoint.changed() => match res {
                Ok(()) => continue,
                Err(_) => return Ok(()),
            },
            res = registry.batched_recv_on(lis) => res?,
        }
        log::debug!("[udp]entry batched recvfrom[{}]", registry.count());

        // clean up closed associations
        while associations.try_join_next().is_some() {}

        let ep = endpoint.borrow().clone();
        let Endpoint {
            raddr: rname,
            conn_opts,
            ..
        } = ep.as_ref();

        // an association keeps its remote address, resolve only for new associations
        let mut resolved = None;

        registry.group_by_addr();
        for pkts in registry.group_iter() {
            let laddr = pkts[0].addr.clone().into();
            let (rsock, raddr) = match sockmap.find(&laddr) {
                Some(x) => x,
                None => {
                    let raddr = match resolved {
                        Some(x) => x,
                        None => {
                            let x = resolve_addr(rname).await?.iter().next().unwrap();
                            log::debug!("[udp]{} resolved as {}", rname, x);
                            *resolved.insert(x)
                        }
                    };
                    let s = Arc::new(socket::associate(&raddr, conn_opts)?);
                    sockmap.insert(laddr, s.clone(), raddr);
                    associations.spawn(send_back(
                        lis.clone(),
                        laddr,
                        s.clone(),
                        conn_opts.associate_timeout,
                        sockmap.clone(),
                    ));
                    log::info!("[udp]new association {} => {} as {}", laddr, rname, raddr);
                    (s, raddr)
                }
            };
            let raddr: SockAddrStore = raddr.into();
            batched::send_all(&rsock, pkts.iter().map(|x| x.ref_with_addr(&raddr))).await?;
        }
    }
}

async fn send_back(
    lsock: Arc<UdpSocket>,
    laddr: SocketAddr,
    rsock: Arc<UdpSocket>,
    timeout: usize,
    sockmap: Arc<SockMap>,
) {
    let mut registry = Registry::new(batched::MAX_PACKETS);
    let laddr_s: SockAddrStore = laddr.into();

    loop {
        match timeoutfut(registry.batched_recv_on(&rsock), timeout).await {
            Err(_) => {
                log::debug!("[udp]rear recvfrom timeout");
                break;
            }
            Ok(Err(e)) => {
                log::error!("[udp]rear recvfrom failed: {}", e);
                break;
            }
            Ok(Ok(())) => {
                log::debug!("[udp]rear batched recvfrom[{}]", registry.count())
            }
        };

        let pkts = registry.iter().map(|pkt| pkt.ref_with_addr(&laddr_s));
        if let Err(e) = batched::send_all(&lsock, pkts).await {
            log::error!("[udp]failed to sendto client{}: {}", laddr, e);
            break;
        }
    }

    sockmap.remove(&laddr);
    log::debug!("[udp]remove association for {}", laddr);
}
