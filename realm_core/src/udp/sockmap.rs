use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use tokio::net::UdpSocket;

use crate::endpoint::Endpoint;

/// Associations, client address => (socket, endpoint when it is established).
pub struct SockMap(RwLock<HashMap<SocketAddr, Association>>);

type Association = (Arc<UdpSocket>, Arc<Endpoint>);

impl SockMap {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    #[inline]
    pub fn find(&self, addr: &SocketAddr) -> Option<Association> {
        // fetch the lock

        let sockmap = self.0.read().unwrap();

        sockmap.get(addr).cloned()

        // drop the lock
    }

    #[inline]
    pub fn insert(&self, addr: SocketAddr, association: Association) {
        // fetch the lock
        let mut sockmap = self.0.write().unwrap();

        let _ = sockmap.insert(addr, association);

        // drop the lock
    }

    #[inline]
    pub fn find_or_insert<E, F>(&self, addr: &SocketAddr, f: F) -> Result<Association, E>
    where
        F: FnOnce() -> Result<Association, E>,
    {
        match self.find(addr) {
            Some(x) => Ok(x),
            None => {
                let association = f()?;
                self.insert(*addr, association.clone());
                Ok(association)
            }
        }
    }

    #[inline]
    pub fn remove(&self, addr: &SocketAddr) {
        // fetch the lock
        let mut sockmap = self.0.write().unwrap();

        let _ = sockmap.remove(addr);

        // drop the lock
    }
}
