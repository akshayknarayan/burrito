use crate::Client;
use bertha::{Chunnel, ChunnelConnection, ChunnelConnector, ChunnelListener, CxList};
use color_eyre::eyre::{bail, Report, WrapErr};
use futures_util::{
    stream::{FuturesUnordered, TryStreamExt},
    Stream,
};
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{
    future::{ready, Future, Ready},
    pin::Pin,
};
use tokio::sync::Barrier;
use tracing::{debug, debug_span, info, trace, warn};
use tracing_futures::Instrument;

/// we want a Chunnel implementation that reads the data at runtime, but does nothing else. this
/// represents the overhead of having a chunnel there at all.
#[derive(Debug, Clone, Default)]
struct NoOpChunnel<const UNIQ: u64>;

impl<const UNIQ: u64> bertha::negotiate::Negotiate for NoOpChunnel<UNIQ> {
    type Capability = ();
    fn guid() -> u64 {
        0x8c7bda3445c93fcb + UNIQ
    }
}

impl<const UNIQ: u64, D, InC> Chunnel<InC> for NoOpChunnel<UNIQ>
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Read + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = NoOpChunnelCn<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Self::Future {
        ready(Ok(NoOpChunnelCn(cn)))
    }
}

trait Read {
    fn read(&self) -> &[u8];
}

impl Read for Vec<u8> {
    fn read(&self) -> &[u8] {
        &self[..]
    }
}
impl<T> Read for (T, Vec<u8>) {
    fn read(&self) -> &[u8] {
        &self.1[..]
    }
}

#[derive(Debug, Clone)]
struct NoOpChunnelCn<C>(C);

impl<D, C> ChunnelConnection for NoOpChunnelCn<C>
where
    C: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Read + Send + Sync + 'static,
{
    type Data = D;

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.0.send(burst.into_iter().map(|data| {
            let arr = data.read();
            if !arr.is_empty() {
                // just read the first byte
                std::hint::black_box(arr[0]);
            }

            data
        }))
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let msgs = self.0.recv(msgs_buf).await?;
            for ms in msgs.iter() {
                if let Some(ref m) = ms {
                    std::hint::black_box(m.read()[0]);
                }
            }

            Ok(msgs)
        })
    }
}

pub(crate) fn run_clients<C, Cn, E>(
    ctr: C,
    c: Client,
    port: u16,
    num_threads: usize,
    stack_depth: usize,
) -> Result<(usize, usize, Duration), Report>
where
    C: ChunnelConnector<Addr = SocketAddr, Connection = Cn, Error = E>
        + Clone
        + Send
        + Sync
        + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    let addr = SocketAddr::from(SocketAddrV4::new(c.addr, port));
    let start_barrier = Arc::new(Barrier::new(c.num_clients));

    let client_threads: Vec<_> =
        if c.num_clients > num_threads {
            let clients_per_thread = c.num_clients / num_threads;
            let mut remainder = c.num_clients - (num_threads * clients_per_thread);
            (0..num_threads)
                .map(|thread| {
                    let ctr = ctr.clone();
                    let start_barrier = start_barrier.clone();
                    let num_thread_clients = if remainder > 0 {
                        remainder -= 1;
                        clients_per_thread + 1
                    } else {
                        clients_per_thread
                    };
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?;
                        let ctr = ctr.clone();
                        let start_barrier = start_barrier.clone();
                        rt.block_on(async move {
                            let futs: FuturesUnordered<_> =
                                (0..num_thread_clients)
                                    .map(|tclient| {
                                        let start_barrier = start_barrier.clone();
                                        run_client(
                                            ctr.clone(),
                                            stack_depth,
                                            addr,
                                            c.download_size,
                                            c.packet_size,
                                            start_barrier,
                                        )
                                        .instrument(debug_span!("client", ?thread, ?tclient))
                                    })
                                    .collect();
                            futs.try_collect().await
                        })
                    })
                })
                .collect()
        } else {
            (0..c.num_clients)
                .map(|thread| {
                    let ctr = ctr.clone();
                    let start_barrier = start_barrier.clone();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?;
                        rt.block_on(async move {
                            let res = run_client(
                                ctr,
                                stack_depth,
                                addr,
                                c.download_size,
                                c.packet_size,
                                start_barrier,
                            )
                            .instrument(debug_span!("client", ?thread))
                            .await?;
                            Ok(vec![res])
                        })
                    })
                })
                .collect()
        };

    let joined: Vec<Result<Vec<(usize, usize, Duration)>, Report>> = client_threads
        .into_iter()
        .map(|jh| jh.join().expect("thread paniced"))
        .collect();
    let (tot_bytes, tot_pkts, elapsed) = joined
        .into_iter()
        .collect::<Result<Vec<Vec<(usize, usize, Duration)>>, _>>()
        .wrap_err("failed running one or more clients")?
        .into_iter()
        .flatten()
        .reduce(|(b, p, d), (bi, pi, di)| (b + bi, p + pi, std::cmp::max(d, di)))
        .expect("There should be at least one client thread");
    info!(?tot_bytes, ?tot_pkts, ?elapsed, "all clients done");
    Ok((tot_bytes, tot_pkts, elapsed))
}

async fn run_client<C, Cn, E>(
    mut ctr: C,
    stack_depth: usize,
    addr: SocketAddr,
    download_size: usize,
    packet_size: usize,
    start_barrier: Arc<Barrier>,
) -> Result<(usize, usize, Duration), Report>
where
    C: ChunnelConnector<Addr = SocketAddr, Connection = Cn, Error = E> + Send + Sync + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    macro_rules! client_with_stack {
        ($stack: expr) => {{
            info!(?addr, ?download_size, ?packet_size, "starting client");
            // 1. connect
            let cn = ctr
                .connect(addr)
                .await
                .map_err(Into::into)
                .wrap_err("connector failed")?;
            let cn = bertha::negotiate_client($stack, cn, addr)
                .await
                .wrap_err("negotiation failed")?;
            trace!("got connection");

            let addr4 = match addr {
                SocketAddr::V4(a) => a,
                _ => bail!("wrong addr"),
            };

            start_barrier.wait().await;
            run_client_inner(cn, addr4, download_size, packet_size).await
        }};
    }

    match stack_depth {
        0 => client_with_stack!(bertha::util::Nothing::<()>::default()),
        1 => client_with_stack!(NoOpChunnel::<0>),
        2 => client_with_stack!(CxList::from(NoOpChunnel::<0>).wrap(NoOpChunnel::<1>)),
        3 => {
            client_with_stack!(CxList::from(NoOpChunnel::<0>)
                .wrap(NoOpChunnel::<1>)
                .wrap(NoOpChunnel::<2>))
        }
        4 => {
            client_with_stack!(CxList::from(NoOpChunnel::<0>)
                .wrap(NoOpChunnel::<1>)
                .wrap(NoOpChunnel::<2>)
                .wrap(NoOpChunnel::<3>))
        }
        5 => {
            client_with_stack!(CxList::from(NoOpChunnel::<0>)
                .wrap(NoOpChunnel::<1>)
                .wrap(NoOpChunnel::<2>)
                .wrap(NoOpChunnel::<3>)
                .wrap(NoOpChunnel::<4>))
        }
        _ => bail!("stack depth {} not supported", stack_depth),
    }
}

pub fn run_server<L, Cn, E>(
    listener: L,
    port: u16,
    threads: usize,
    stack_depth: usize,
) -> Result<(), Report>
where
    L: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = E> + Clone + Send + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    fn server_thread<L, Cn, E>(mut listener: L, port: u16, stack_depth: usize) -> Result<(), Report>
    where
        L: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = E>,
        Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        E: Into<Report> + Send + Sync + 'static,
    {
        macro_rules! server_with_stack {
            ($stack: expr) => {{
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async move {
                    let st = listener
                        .listen(SocketAddr::from(SocketAddrV4::new(
                            std::net::Ipv4Addr::UNSPECIFIED,
                            port,
                        )))
                        .await
                        .map_err(Into::into)?;
                    let st = bertha::negotiate::negotiate_server($stack, st)
                        .instrument(debug_span!("negotiate_server"))
                        .await
                        .wrap_err("negotiate_server")?;

                    tokio::pin!(st);
                    server_thread_inner(st).await
                })
            }};
        }

        match stack_depth {
            0 => server_with_stack!(bertha::util::Nothing::<()>::default()),
            1 => server_with_stack!(NoOpChunnel::<0>),
            2 => server_with_stack!(CxList::from(NoOpChunnel::<0>).wrap(NoOpChunnel::<1>)),
            3 => {
                server_with_stack!(CxList::from(NoOpChunnel::<0>)
                    .wrap(NoOpChunnel::<1>)
                    .wrap(NoOpChunnel::<2>))
            }
            4 => {
                server_with_stack!(CxList::from(NoOpChunnel::<0>)
                    .wrap(NoOpChunnel::<1>)
                    .wrap(NoOpChunnel::<2>)
                    .wrap(NoOpChunnel::<3>))
            }
            5 => {
                server_with_stack!(CxList::from(NoOpChunnel::<0>)
                    .wrap(NoOpChunnel::<1>)
                    .wrap(NoOpChunnel::<2>)
                    .wrap(NoOpChunnel::<3>)
                    .wrap(NoOpChunnel::<4>))
            }
            _ => bail!("stack depth {} not supported", stack_depth),
        }
    }

    info!(?port, ?threads, "starting server");
    for _ in 1..threads {
        let listener = listener.clone();
        std::thread::spawn(move || server_thread(listener, port, stack_depth));
    }

    server_thread(listener, port, stack_depth)
}

pub(crate) fn run_clients_no_bertha<C, Cn>(
    ch: C,
    c: Client,
    port: u16,
    num_threads: usize,
) -> Result<(usize, usize, Duration), Report>
where
    C: ChunnelConnector<Addr = SocketAddr, Connection = Cn, Error = Report>
        + Clone
        + Send
        + Sync
        + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
{
    let addr4 = SocketAddrV4::new(c.addr, port);
    let addr = SocketAddr::V4(addr4);

    let client_threads: Vec<_> = if c.num_clients > num_threads {
        let clients_per_thread = c.num_clients / num_threads;
        let mut remainder = c.num_clients - (num_threads * clients_per_thread);
        (0..num_threads)
            .map(|thread| {
                let ch = ch.clone();
                let num_thread_clients = if remainder > 0 {
                    remainder -= 1;
                    clients_per_thread + 1
                } else {
                    clients_per_thread
                };
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    let ch = ch.clone();
                    rt.block_on(async move {
                        let futs: FuturesUnordered<_> = (0..num_thread_clients)
                            .map(|tclient| {
                                let mut ch = ch.clone();
                                async move {
                                    let cn = ch.connect(addr).await?;
                                    run_client_inner(cn, addr4, c.download_size, c.packet_size)
                                        .instrument(debug_span!("client", ?thread, ?tclient))
                                        .await
                                }
                            })
                            .collect();
                        Ok::<_, Report>(futs.try_collect().await?)
                    })
                })
            })
            .collect()
    } else {
        (0..c.num_clients)
            .map(|thread| {
                let mut ch = ch.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    let res = rt.block_on(async move {
                        let cn = ch.connect(addr).await?;
                        run_client_inner(cn, addr4, c.download_size, c.packet_size)
                            .instrument(debug_span!("client", ?thread))
                            .await
                    })?;
                    Ok(vec![res])
                })
            })
            .collect()
    };

    let joined: Vec<Result<Vec<(usize, usize, Duration)>, Report>> = client_threads
        .into_iter()
        .map(|jh| jh.join().expect("thread paniced"))
        .collect();
    let (tot_bytes, tot_pkts, elapsed) = joined
        .into_iter()
        .collect::<Result<Vec<Vec<(usize, usize, Duration)>>, _>>()
        .wrap_err("failed running one or more clients")?
        .into_iter()
        .flatten()
        .reduce(|(b, p, d), (bi, pi, di)| (b + bi, p + pi, std::cmp::max(d, di)))
        .expect("There should be at least one client thread");
    info!(?tot_bytes, ?tot_pkts, ?elapsed, "all clients done");
    Ok((tot_bytes, tot_pkts, elapsed))
}

async fn run_client_inner<C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)>>(
    cn: C,
    addr: SocketAddrV4,
    download_size: usize,
    packet_size: usize,
) -> Result<(usize, usize, Duration), Report> {
    info!(?addr, ?download_size, ?packet_size, "starting client");

    let mut tot_bytes = 0;
    let mut tot_pkts = 0;
    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
    req.extend((download_size as u64).to_le_bytes());
    req.extend((packet_size as u64).to_le_bytes());
    cn.send(std::iter::once((SocketAddr::V4(addr), req.clone())))
        .await?;
    let start = Instant::now();
    let mut last_recv_time = None;
    // pre-allocate the receive buffer.
    let mut slots: Vec<_> = (0..16)
        .map(|_| {
            Some((
                SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0),
                Vec::with_capacity(packet_size),
            ))
        })
        .collect();
    'cn: loop {
        let ms = if last_recv_time.is_none() {
            match tokio::time::timeout(Duration::from_millis(100), cn.recv(&mut slots)).await {
                Ok(r) => r?,
                Err(_) => {
                    debug!("retransmitting request");
                    cn.send(std::iter::once((SocketAddr::V4(addr), req.clone())))
                        .await?;
                    continue;
                }
            }
        } else {
            cn.recv(&mut slots).await?
        };

        last_recv_time = Some(Instant::now());
        // don't uselessly throw away our hard-allocated Vec with an `Option::take`
        for (_, r) in ms.iter().map_while(Option::as_ref) {
            tot_bytes += r.len();
            tot_pkts += 1;
            trace!(?tot_bytes, ?tot_pkts, "received part");
            if r[0] == 1 {
                cn.send(std::iter::once((SocketAddr::V4(addr), r.clone())))
                    .await?;
                break 'cn;
            }
        }
    }

    let elapsed = last_recv_time.unwrap() - start;
    info!(?tot_bytes, ?tot_pkts, ?elapsed, "done");
    Ok((tot_bytes, tot_pkts, elapsed))
}

pub fn run_server_no_bertha<L, Cn>(ch: L, port: u16, num_threads: usize) -> Result<(), Report>
where
    L: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = Report> + Clone + Send + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
{
    fn server_thread<L, Cn>(mut ch: L, port: u16) -> Result<(), Report>
    where
        L: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = Report>
            + Clone
            + Send
            + 'static,
        Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async move {
            let st = ch
                .listen(SocketAddr::from(SocketAddrV4::new(
                    std::net::Ipv4Addr::UNSPECIFIED,
                    port,
                )))
                .await?;
            tokio::pin!(st);
            server_thread_inner(st).await?;
            unreachable!() // negotiate_server never returns None
        })
    }

    info!(?port, ?num_threads, "starting server");
    for _ in 1..num_threads {
        let ch = ch.clone();
        std::thread::spawn(move || server_thread(ch, port));
    }

    server_thread(ch, port)
}

async fn server_thread_inner<
    S: Stream<Item = Result<C, Report>> + Unpin,
    C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)>,
>(
    st: S,
) -> Result<(), Report> {
    st.try_for_each_concurrent(None, |cn| async move {
        let mut slots = [None; 1];
        let ms = cn.recv(&mut slots).await?;
        let (a, (mut remaining, pkt_size)) = match ms {
            [Some((a, m))] => (*a, super::validate_message(&m[..])?),
            msg => {
                warn!(?msg, "bad client request");
                bail!("bad connection: {:?}", msg);
            }
        };

        let start = Instant::now();
        info!(?remaining, ?a, "starting send");
        while remaining > 0 {
            let bufs = (0..16).map_while(|_| {
                if remaining > 0 {
                    let this_send_size = std::cmp::min(pkt_size, remaining);
                    remaining -= this_send_size;
                    Some((a, vec![0u8; this_send_size as usize]))
                } else {
                    None
                }
            });
            cn.send(bufs).await?;
            tokio::task::yield_now().await;
        }

        info!(elapsed = ?start.elapsed(), ?a, "done sending");
        // fin
        cn.send(std::iter::once((a, vec![1u8]))).await?;
        let mut f = cn.recv(&mut slots);
        loop {
            match futures_util::future::select(
                f,
                Box::pin(tokio::time::sleep(Duration::from_millis(1))),
            )
            .await
            {
                futures_util::future::Either::Left((_, _)) => break,
                futures_util::future::Either::Right((_, rem)) => {
                    cn.send(std::iter::once((a, vec![1u8]))).await?;
                    f = rem;
                }
            }
        }

        info!(elapsed = ?start.elapsed(), ?a, "exiting");
        Ok::<_, Report>(())
    })
    .await
}
