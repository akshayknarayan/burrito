//! A message-oriented TCP Chunnel.
//!
//! Unlike [`bertha::udp`], TCP chunnels are always connection-oriented, so there is no equivalent
//! of [`bertha::udp::UdpSkChunnel`].

#![feature(iter_array_chunks)]

use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{bail, ensure, eyre, Report, WrapErr};
use futures_util::stream::{Stream, StreamExt, TryStreamExt};
use std::cmp::Ordering;
use std::net::SocketAddr;
use std::{future::Future, pin::Pin};
use tokio::net::TcpStream;
use tokio_stream::wrappers::TcpListenerStream;

/// TCP Chunnel connector.
///
/// Wraps [`tokio::net::TcpSocket`]. Carries no state.
///
/// If `WITHADDR` == true, wrap the returned connection in an [`IgnoreAddr`].
#[derive(Default, Clone, Debug)]
pub struct TcpChunnel<const WITHADDR: bool = false>;

impl ChunnelListener for TcpChunnel<false> {
    type Addr = SocketAddr;
    type Connection = TcpCn;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = tokio::net::TcpListener::bind(a)
                .await
                .wrap_err_with(|| eyre!("Could not bind TcpListener to {:?}", a))?;
            Ok(Box::pin(
                TcpListenerStream::new(sk)
                    .map_ok(TcpCn::new)
                    .map_err(Into::into),
            ) as _)
        })
    }
}

impl ChunnelListener for TcpChunnel<true> {
    type Addr = SocketAddr;
    type Connection = IgnoreAddr<SocketAddr, TcpCn>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = tokio::net::TcpListener::bind(a)
                .await
                .wrap_err_with(|| eyre!("Could not bind TcpListener to {:?}", a))?;
            Ok(Box::pin(TcpListenerStream::new(sk).map(|cn| {
                let cn = cn?;
                let addr = cn.peer_addr()?;
                Ok(IgnoreAddr(addr, TcpCn::new(cn)))
            })) as _)
        })
    }
}

impl ChunnelConnector for TcpChunnel<false> {
    type Addr = SocketAddr;
    type Connection = TcpCn;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = TcpStream::connect(a).await?;
            Ok(TcpCn::new(sk))
        })
    }
}

impl ChunnelConnector for TcpChunnel<true> {
    type Addr = SocketAddr;
    type Connection = IgnoreAddr<SocketAddr, TcpCn>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        Box::pin(async move {
            let sk = TcpStream::connect(a).await?;
            Ok(IgnoreAddr(a, TcpCn::new(sk)))
        })
    }
}

#[derive(Debug, Clone)]
pub struct IgnoreAddr<A, C>(pub A, pub C);

impl<A, C, D> ChunnelConnection for IgnoreAddr<A, C>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
    A: Clone + PartialEq + std::fmt::Debug + Send + Sync + 'static,
    D: Send,
{
    type Data = (A, D);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.1.send(burst.into_iter().map(|(_, d)| d))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let mut with_addr_slots: Vec<_> = (0..msgs_buf.len()).map(|_| None).collect();
            let ms = self.1.recv(&mut with_addr_slots[..]).await?;
            let mut ret_len = 0;
            for (msg, slot) in ms
                .iter_mut()
                .map_while(Option::take)
                .zip(msgs_buf.iter_mut())
            {
                *slot = Some((self.0.clone(), msg));
                ret_len += 1;
            }

            Ok(&mut msgs_buf[..ret_len])
        })
    }
}

pub struct TcpCn {
    inner: TcpStream,
}

impl TcpCn {
    pub fn new(inner: TcpStream) -> Self {
        TcpCn { inner }
    }
}

impl ChunnelConnection for TcpCn {
    type Data = Vec<u8>;

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(async move {
            let mut batches = burst
                .into_iter()
                .map(|msg| (msg.len() as u32, msg))
                .array_chunks::<8>();

            use std::io::IoSlice;
            // we need the while let ... because a for loop takes ownership of `batches`, but we
            // need to call `::into_remainder` afterward.
            #[allow(clippy::while_let_on_iterator)]
            while let Some(
                [(h0, m0), (h1, m1), (h2, m2), (h3, m3), (h4, m4), (h5, m5), (h6, m6), (h7, m7)],
            ) = batches.next()
            {
                let h0_buf = h0.to_be_bytes();
                let h1_buf = h1.to_be_bytes();
                let h2_buf = h2.to_be_bytes();
                let h3_buf = h3.to_be_bytes();
                let h4_buf = h4.to_be_bytes();
                let h5_buf = h5.to_be_bytes();
                let h6_buf = h6.to_be_bytes();
                let h7_buf = h7.to_be_bytes();
                let batch = [
                    IoSlice::new(&h0_buf[..]),
                    IoSlice::new(&m0[..]),
                    IoSlice::new(&h1_buf[..]),
                    IoSlice::new(&m1[..]),
                    IoSlice::new(&h2_buf[..]),
                    IoSlice::new(&m2[..]),
                    IoSlice::new(&h3_buf[..]),
                    IoSlice::new(&m3[..]),
                    IoSlice::new(&h4_buf[..]),
                    IoSlice::new(&m4[..]),
                    IoSlice::new(&h5_buf[..]),
                    IoSlice::new(&m5[..]),
                    IoSlice::new(&h6_buf[..]),
                    IoSlice::new(&m6[..]),
                    IoSlice::new(&h7_buf[..]),
                    IoSlice::new(&m7[..]),
                ];

                loop {
                    self.inner.writable().await?;
                    match self.inner.try_write_vectored(&batch) {
                        Ok(_) => break,
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            continue;
                        }
                        Err(e) => {
                            bail!(eyre!(e).wrap_err("try_write_vectored error"));
                        }
                    }
                }
            }

            if let Some(rem) = batches.into_remainder() {
                // rem is an iterator with at least 1 and at most 7 elements.
                let rem_buf = rem.as_slice();
                if rem_buf.is_empty() {
                    bail!("remainder is empty (?)");
                }

                const NULL: [u8; 0] = [];
                let mut final_batch_nums = [
                    [0u8; 4], [0u8; 4], [0u8; 4], [0u8; 4], [0u8; 4], [0u8; 4], [0u8; 4],
                ];
                let mut final_batch = [
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                    IoSlice::new(&NULL[..]),
                ];

                // `final_batch_nums` is a place to store our message length header buffers. It's
                // not needed in the previous loop since that one contains an exact number of
                // messages, so it can use variables. Since that's not the case here - we could
                // have between 1 and 7 elements - we need an array instead.
                //
                // We need two loops because we are assigning to `final_batch_nums[i]` and also
                // borrowing it to make the IoSlice. We could move the contents of the first loop
                // into the second like this:
                // ```rust
                // let x: *mut [u8; 4] = (&final_batch_nums[i]) as *const _ as *mut _;
                // unsafe { *x = data[i].0.to_be_bytes(); }
                // ```
                // but after checking the resulting assembly the compiler mostly does this for us
                // anyway, so we will keep it like this.
                for i in 0..rem_buf.len() {
                    final_batch_nums[i] = rem_buf[i].0.to_be_bytes();
                }
                for i in 0..rem_buf.len() {
                    final_batch[i * 2] = IoSlice::new(&final_batch_nums[i][..]);
                    final_batch[(i * 2) + 1] = IoSlice::new(&(rem_buf[i].1)[..]);
                }

                loop {
                    self.inner.writable().await?;
                    match self
                        .inner
                        .try_write_vectored(&final_batch[..rem_buf.len() * 2])
                    {
                        Ok(_) => break,
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            continue;
                        }
                        Err(e) => {
                            bail!(eyre!(e).wrap_err(eyre!(
                                "try_write_vectored remainder error: {:?}",
                                rem_buf.len() * 2
                            )));
                        }
                    }
                }
            }

            Ok(())
        })
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let mut partial_header_len = 0;
            let mut curr_expected_len = None;
            let mut tot_msgs = 0;
            'readable: loop {
                self.inner.readable().await?;
                // TODO would be more efficient to do bigger reads into buf, and do message
                // assembly from a buffer here.

                let mut len_buf = [0; 4];
                let mut buf = [0; 2048];

                'msg: loop {
                    if let Some((mut so_far, tot_expect)) = curr_expected_len.take() {
                        assert!(so_far < tot_expect, "so_far must be < tot_expect");
                        match self.inner.try_read(&mut buf[so_far..tot_expect]) {
                            Ok(0) => {
                                unreachable!();
                            }
                            Ok(n) => {
                                so_far += n;
                                match so_far.cmp(&tot_expect) {
                                    Ordering::Equal => {
                                        if let Some(ref mut dst_buf) = &mut msgs_buf[tot_msgs] {
                                            dst_buf.clear();
                                            dst_buf.extend_from_slice(&buf[..tot_expect]);
                                        } else {
                                            msgs_buf[tot_msgs] = Some(buf[..tot_expect].to_vec());
                                        }

                                        tot_msgs += 1;
                                        if tot_msgs == msgs_buf.len() {
                                            return Ok(&mut msgs_buf[..]);
                                        }

                                        continue 'msg;
                                    }
                                    Ordering::Less => {
                                        curr_expected_len = Some((so_far, tot_expect));
                                        continue 'readable;
                                    }
                                    Ordering::Greater => unreachable!(),
                                }
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                // put this back and try again. we cannot return here, having
                                // received a header but not its message.
                                //
                                // if we end up in a situation where we receive a full message and
                                // then the header of a second message, the first message will be
                                // delayed while we wait to get the body of the second message.
                                curr_expected_len = Some((so_far, tot_expect));
                                continue 'readable;
                            }
                            Err(e) => {
                                bail!(eyre!(e).wrap_err(eyre!(
                                    "Error reading message body of length {:?}",
                                    tot_expect
                                )));
                            }
                        }
                    } else {
                        ensure!(
                            partial_header_len < len_buf.len(),
                            "partial_header_len is wrong"
                        );
                        match self.inner.try_read(&mut len_buf[partial_header_len..]) {
                            Ok(0) => {
                                unreachable!()
                            }
                            Ok(n) => {
                                partial_header_len += n;
                                match partial_header_len.cmp(&4) {
                                    Ordering::Less => continue 'readable,
                                    Ordering::Greater => unreachable!(),
                                    Ordering::Equal => (),
                                }
                            }
                            Err(ref e)
                                if e.kind() == std::io::ErrorKind::WouldBlock && tot_msgs > 0 =>
                            {
                                return Ok(&mut msgs_buf[..tot_msgs]);
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                continue 'readable;
                            }
                            Err(e) => {
                                bail!(eyre!(e).wrap_err(eyre!("Error reading message header")));
                            }
                        };

                        curr_expected_len = Some((0, u32::from_be_bytes(len_buf) as usize));
                        ensure!(
                            curr_expected_len.as_ref().unwrap().1 > 0,
                            "invalid message length"
                        );
                        partial_header_len = 0;
                        continue 'msg;
                    }
                }
            }
        })
    }
}

pub trait Connected {
    fn local_addr(&self) -> SocketAddr;
    fn peer_addr(&self) -> Option<SocketAddr> {
        None
    }
}

impl Connected for TcpCn {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr().unwrap()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.inner.peer_addr().ok()
    }
}

impl Connected for bertha::udp::UdpSk<SocketAddr> {
    fn local_addr(&self) -> SocketAddr {
        self.local_addr().unwrap()
    }
}

impl Connected for bertha::udp::UdpConn {
    fn local_addr(&self) -> SocketAddr {
        self.inner().local_addr().unwrap()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        Some(self.peer())
    }
}

impl<C: Connected> Connected for bertha::util::ProjectLeft<SocketAddr, C> {
    fn local_addr(&self) -> SocketAddr {
        self.conn().local_addr()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        Some(*self.addr())
    }
}

impl<C: Connected, D> Connected for bertha::negotiate::InjectWithChannel<C, D> {
    fn local_addr(&self) -> SocketAddr {
        self.conn().local_addr()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.conn().peer_addr()
    }
}

pub struct Connect<C>(SocketAddr, C);

impl<C> Connect<C> {
    pub fn new(addr: SocketAddr, inner: C) -> Self {
        Self(addr, inner)
    }
}

impl<C, D> ChunnelConnection for Connect<C>
where
    C: ChunnelConnection<Data = (SocketAddr, D)> + Send + Sync + 'static,
    D: Send + 'static,
{
    type Data = (SocketAddr, D);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        Box::pin(
            self.1
                .send(burst.into_iter().map(|(_, data)| (self.0, data))),
        ) as _
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let msgs = self.1.recv(&mut msgs_buf[..]).await?;
            for (a, _) in msgs.iter_mut().map_while(Option::as_mut) {
                *a = self.0;
            }

            Ok(&mut msgs[..])
        })
    }
}

impl<C> Connected for Connect<C>
where
    C: Connected,
{
    fn local_addr(&self) -> SocketAddr {
        self.1.local_addr()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        Some(self.0)
    }
}

#[cfg(test)]
mod t {
    use super::TcpChunnel;
    use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
    use color_eyre::eyre::{ensure, Report, WrapErr};
    use futures_util::TryStreamExt;
    use std::sync::Arc;
    use std::sync::Once;
    use tokio::sync::Barrier;
    use tracing::{debug, info};
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    pub static COLOR_EYRE: Once = Once::new();

    #[test]
    fn tcp_chunnel() {
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let _guard = subscriber.set_default();
        COLOR_EYRE.call_once(|| color_eyre::install().unwrap_or(()));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();

        async fn server(start: Arc<Barrier>) -> Result<(), Report> {
            let mut ch: TcpChunnel = TcpChunnel;
            let st = ch.listen("127.0.0.1:58281".parse().unwrap()).await?;
            start.wait().await;
            st.try_for_each_concurrent(None, |cn| async move {
                info!("starting connection");
                let mut msgs: Vec<_> = (0..8).map(|_| Some(Vec::with_capacity(2048))).collect();
                loop {
                    let ms = cn.recv(&mut msgs[..]).await?;
                    debug!(?ms, "received from client");
                    cn.send(ms.iter_mut().map_while(Option::take)).await?;
                }
            })
            .await?;
            Ok(())
        }

        //client
        rt.block_on(async move {
            let start = Arc::new(tokio::sync::Barrier::new(2));
            let srv_start = start.clone();
            let jh = tokio::spawn(async move {
                if let Err(err) = server(srv_start).await {
                    tracing::error!(?err, "server errored");
                    return Err(err);
                }

                unreachable!()
            });

            let mut ch: TcpChunnel = TcpChunnel;
            start.wait().await;
            let cn = ch
                .connect("127.0.0.1:58281".parse().unwrap())
                .await
                .wrap_err("could not connect")?;

            let mut msgs: Vec<_> = (0..8).map(|_| Some(Vec::with_capacity(8))).collect();
            for i in 0..10 {
                debug!(?i, "send");
                cn.send((0..4).map(|i| u64::to_le_bytes(i).to_vec()))
                    .await?;

                let ms = cn.recv(&mut msgs[..]).await?;
                debug!(?i, ?ms, "got echo");
                for (i, m) in ms.iter_mut().map_while(Option::take).enumerate() {
                    ensure!(
                        i == u64::from_le_bytes(m[0..8].try_into().unwrap()) as usize,
                        "wrong message"
                    );
                }
            }

            if jh.is_finished() {
                return jh.await.unwrap();
            }

            Ok(())
        })
        .unwrap();
    }
}
