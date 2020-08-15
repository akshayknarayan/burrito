//! Helper traits to avoid re-writing common Chunnel implementation code for simple Chunnels.

use crate::{ChunnelConnection, Serve};
use futures_util::future::{ready, Ready};
use futures_util::stream::Stream;
use futures_util::stream::TryStreamExt;
use std::pin::Pin;

/// A simpler trait to implement for traits with simpler (or no) Chunnel setup.
pub trait InheritChunnel<CxConn: ChunnelConnection> {
    type Connection: ChunnelConnection;
    type Config: Clone + Send + Sync + 'static;

    fn get_config(&mut self) -> Self::Config;
    fn make_connection(cx: CxConn, cfg: Self::Config) -> Self::Connection;
}

impl<C, InS, InC, InE> Serve<InS> for C
where
    InS: Stream<Item = Result<InC, InE>> + Send + 'static,
    InC: ChunnelConnection + Send + Sync + 'static,
    InE: Send + Sync + 'static,
    C: InheritChunnel<InC>,
{
    type Future = Ready<Result<Self::Stream, Self::Error>>;
    type Connection = C::Connection;
    type Error = InE;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: InS) -> Self::Future {
        let cfg = self.get_config();
        ready(Ok(
            Box::pin(inner.map_ok(|cn| C::make_connection(cn, cfg.clone()))) as _,
        ))
    }
}
