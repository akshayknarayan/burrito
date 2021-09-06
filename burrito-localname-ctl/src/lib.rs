#![warn(clippy::all)]
#![allow(clippy::type_complexity)]

pub const CONTROLLER_ADDRESS: &str = "localname-ctl";

pub mod client;
pub mod proto;

#[cfg(feature = "ctl")]
pub mod ctl;

mod runtime_fastpath;
pub use runtime_fastpath::*;

use bertha::{
    either::Either, Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, Negotiate,
};
use color_eyre::eyre::{Report, WrapErr};
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tracing::{debug, instrument, trace};

#[instrument(level = "trace", skip(cl, addr_cache, rev_addr_map))]
async fn query_ctl<A>(
    addr: A,
    cl: Arc<Mutex<client::LocalNameClient>>,
    addr_cache: Arc<StdMutex<HashMap<SocketAddr, LocalAddrCacheEntry<PathBuf>>>>,
    rev_addr_map: Arc<StdMutex<HashMap<PathBuf, A>>>,
) where
    A: GetSockAddr + Clone + Debug + Send + 'static,
{
    let skaddr = addr.as_sk_addr();
    let mut cl_g = cl.lock().await;
    let res = cl_g.query(skaddr).await;
    std::mem::drop(cl_g);
    trace!(?res, "queried localname-ctl");

    fn insert_antihit(
        skaddr: SocketAddr,
        addr_cache: Arc<StdMutex<HashMap<SocketAddr, LocalAddrCacheEntry<PathBuf>>>>,
    ) {
        let mut c = addr_cache.lock().unwrap();
        c.insert(
            skaddr,
            LocalAddrCacheEntry::AntiHit {
                expiry: Some(std::time::Instant::now() + std::time::Duration::from_millis(100)),
            },
        );
    }

    match res {
        Ok(Some(laddr)) => {
            let mut c = addr_cache.lock().unwrap();
            c.insert(
                skaddr,
                LocalAddrCacheEntry::Hit {
                    laddr: laddr.clone(),
                    expiry: std::time::Instant::now() + std::time::Duration::from_millis(100),
                },
            );

            rev_addr_map.lock().unwrap().insert(laddr.clone(), addr);
        }
        Ok(None) => insert_antihit(skaddr, addr_cache),
        Err(e) => {
            debug!(err = %format!("{:#}", e), ?addr, "LocalNameClient query failed");
            insert_antihit(skaddr, addr_cache);
        }
    }
}
