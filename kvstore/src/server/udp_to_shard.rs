use bertha::{util::ProjectLeft, ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{eyre, Report};
use futures_util::stream::{Stream, StreamExt};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::{future::Future, pin::Pin};
use tracing::debug;

/// Shim address semantics.
///
/// `ShardCanonicalServer` wants a `impl ChunnelConnector<Addr = A, Connection = impl
/// ChunnelConnection<Data = (A, Vec<u8>)>>`. But, the `A` in the connection is "fake": it's
/// meant to be passed to the shard, which will echo it, so we know which address to send the
/// response to.
///
/// So, we `ProjectLeft` a shard address onto the connection, and put the client address into
/// the data type.
#[derive(Debug, Clone, Default)]
pub struct UdpToShard<I>(pub I);

impl<I> UdpToShard<I> {
    pub fn new(inner: I) -> Self {
        UdpToShard(inner)
    }
}

impl<I, E> ChunnelConnector for UdpToShard<I>
where
    I: ChunnelConnector<Addr = SocketAddr, Error = E> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
    UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelConnector>::Connection>>:
        ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
{
    type Addr = SocketAddr;
    type Connection = UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelConnector>::Connection>>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let ctr_fut = self.0.connect(a);
        Box::pin(async move {
            Ok(UdpToShardCn(ProjectLeft::new(
                a,
                ctr_fut.await.map_err(Into::into)?,
            )))
        })
    }
}

impl<I, E> ChunnelListener for UdpToShard<I>
where
    I: ChunnelListener<Addr = SocketAddr, Error = E> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
    UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelListener>::Connection>>:
        ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
{
    type Addr = SocketAddr;
    type Connection = UdpToShardCn<ProjectLeft<SocketAddr, <I as ChunnelListener>::Connection>>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let lis_fut = self.0.listen(a);
        Box::pin(async move {
            let l = lis_fut.await.map_err(Into::into)?;
            // ProjectLeft a is a dummy, the UdpConn will ignore it and replace with the
            // req-connection source addr.
            Ok(Box::pin(
                l.map(move |cn| Ok(UdpToShardCn(ProjectLeft::new(a, cn.map_err(Into::into)?)))),
            ) as _)
        })
    }
}

/// UdpToShardCn encodes a [`SocketAddr`] in the payload.
///
/// Wire format (little endian):
/// ```text,no_run
/// 0   | 1 2  | 3 .. 7 (.. 19) | ...
/// len | port | ipv4   (v6)    | payload
/// ```
#[derive(Debug, Clone)]
pub struct UdpToShardCn<C>(C);

impl<C> ChunnelConnection for UdpToShardCn<C>
where
    C: ChunnelConnection<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Data = (SocketAddr, Vec<u8>);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.0.send(burst.into_iter().map(prepend_addr))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let mut slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
            let ms = self.0.recv(&mut slots).await?;
            let mut slot_idx = 0;
            for m in ms.iter_mut().map_while(Option::take) {
                msgs_buf[slot_idx] = Some(peel_addr(m)?);
                slot_idx += 1;
            }

            Ok(&mut msgs_buf[..slot_idx])
        })
    }
}

fn peel_addr(mut data: Vec<u8>) -> Result<(SocketAddr, Vec<u8>), Report> {
    if data.len() < 7 {
        tracing::warn!("bad payload");
        return Err(eyre!("Bad payload, no address"));
    }

    let mut p = [0u8; 2];
    p.copy_from_slice(&data[1..3]);
    let port = u16::from_be_bytes(p);
    match data[0] {
        4 => {
            let mut a = [0u8; 4];
            a.copy_from_slice(&data[3..7]);
            let addr = Ipv4Addr::from(a);
            let sa = SocketAddr::new(IpAddr::V4(addr), port);
            debug!(?sa, "peeled return address from payload");
            data.splice(0..7, std::iter::empty());
            Ok((sa, data))
        }
        16 => {
            if data.len() < 19 {
                return Err(eyre!("Bad payload, no address"));
            }

            let mut a = [0u8; 16];
            a.copy_from_slice(&data[3..19]);
            let sa = SocketAddr::new(IpAddr::V6(Ipv6Addr::from(a)), port);
            data.splice(0..19, std::iter::empty());
            Ok((sa, data))
        }
        _ => Err(eyre!("Bad payload, no address")),
    }
}

fn prepend_addr((addr, mut data): (SocketAddr, Vec<u8>)) -> Vec<u8> {
    // stick the addr in the front of data.
    let ip = addr.ip();
    let port = addr.port();
    let p = port.to_be_bytes();
    match ip {
        IpAddr::V4(v4) => {
            let addr_bytes = v4.octets();
            let addr_bytes_len = addr_bytes.len() as u8; // either 8 or 16 will fit.
            let i = std::iter::once(addr_bytes_len)
                .chain(p.iter().copied())
                .chain(addr_bytes.iter().copied());
            debug!(?addr, "prepended address bytes to payload");
            data.splice(0..0, i);
        }
        IpAddr::V6(v6) => {
            let addr_bytes = v6.octets();
            let addr_bytes_len = addr_bytes.len() as u8; // either 8 or 16 will fit.
            let i = std::iter::once(addr_bytes_len)
                .chain(p.iter().copied())
                .chain(addr_bytes.iter().copied());
            data.splice(0..0, i);
        }
    };

    data
}
