use crate::Client;
use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{bail, Report, WrapErr};
use futures_util::{
    stream::{FuturesUnordered, TryStreamExt},
    Stream,
};
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tracing::{debug, info, info_span, trace, warn};
use tracing_futures::Instrument;

pub(crate) fn run_clients<C, Cn, E>(
    ctr: C,
    c: Client,
    port: u16,
    num_threads: usize,
) -> Result<(usize, Duration), Report>
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

    let client_threads: Vec<_> = if c.num_clients > num_threads {
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
                        let futs: FuturesUnordered<_> = (0..num_thread_clients)
                            .map(|tclient| {
                                let start_barrier = start_barrier.clone();
                                run_client(
                                    ctr.clone(),
                                    addr,
                                    c.download_size,
                                    c.packet_size,
                                    start_barrier,
                                )
                                .instrument(info_span!(
                                    "client",
                                    ?thread,
                                    ?tclient
                                ))
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
                let ctr = ctr.clone();
                let start_barrier = start_barrier.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    rt.block_on(async move {
                        let res =
                            run_client(ctr, addr, c.download_size, c.packet_size, start_barrier)
                                .instrument(info_span!("client", ?thread))
                                .await?;
                        Ok(vec![res])
                    })
                })
            })
            .collect()
    };

    let joined: Vec<Result<Vec<(usize, Duration)>, Report>> = client_threads
        .into_iter()
        .map(|jh| jh.join().expect("thread paniced"))
        .collect();
    let (tot_bytes, durs): (Vec<usize>, Vec<Duration>) = joined
        .into_iter()
        .collect::<Result<Vec<Vec<(usize, Duration)>>, _>>()
        .wrap_err("failed running one or more clients")?
        .into_iter()
        .flatten()
        .unzip();
    let tot_bytes = tot_bytes.into_iter().sum();
    let elapsed = durs.into_iter().max().unwrap();
    info!(?tot_bytes, ?elapsed, "all clients done");
    Ok((tot_bytes, elapsed))
}

async fn run_client<C, Cn, E>(
    mut ctr: C,
    addr: SocketAddr,
    download_size: usize,
    packet_size: usize,
    start_barrier: Arc<Barrier>,
) -> Result<(usize, Duration), Report>
where
    C: ChunnelConnector<Addr = SocketAddr, Connection = Cn, Error = E> + Send + Sync + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    info!(?addr, ?download_size, ?packet_size, "starting client");
    let stack = bertha::util::Nothing::<()>::default();

    // 1. connect
    let cn = ctr
        .connect(addr)
        .await
        .map_err(Into::into)
        .wrap_err("connector failed")?;
    let cn = bertha::negotiate_client(stack, cn, addr)
        .await
        .wrap_err("negotiation failed")?;
    trace!("got connection");

    let addr4 = match addr {
        SocketAddr::V4(a) => a,
        _ => bail!("wrong addr"),
    };

    start_barrier.wait().await;
    run_client_inner(cn, addr4, download_size, packet_size).await
}

pub fn run_server<L, Cn, E>(listener: L, port: u16, threads: usize) -> Result<(), Report>
where
    L: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = E> + Clone + Send + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    fn server_thread<L, Cn, E>(mut listener: L, port: u16) -> Result<(), Report>
    where
        L: ChunnelListener<Addr = SocketAddr, Connection = Cn, Error = E>,
        Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
        E: Into<Report> + Send + Sync + 'static,
    {
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
            let stack = bertha::util::Nothing::<()>::default();
            let st = bertha::negotiate::negotiate_server(stack, st)
                .instrument(info_span!("negotiate_server"))
                .await
                .wrap_err("negotiate_server")?;

            tokio::pin!(st);
            server_thread_inner(st).await
        })
    }

    info!(?port, ?threads, "starting server");
    for _ in 1..threads {
        let listener = listener.clone();
        std::thread::spawn(move || server_thread(listener, port));
    }

    server_thread(listener, port)
}

pub(crate) fn run_clients_no_bertha<C, Cn>(
    ch: C,
    c: Client,
    port: u16,
    num_threads: usize,
) -> Result<(usize, Duration), Report>
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
                                        .instrument(info_span!("client", ?thread, ?tclient))
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
                            .instrument(info_span!("client", ?thread))
                            .await
                    })?;
                    Ok(vec![res])
                })
            })
            .collect()
    };

    let joined: Vec<Result<Vec<(usize, Duration)>, Report>> = client_threads
        .into_iter()
        .map(|jh| jh.join().expect("thread paniced"))
        .collect();
    let (tot_bytes, durs): (Vec<usize>, Vec<Duration>) = joined
        .into_iter()
        .collect::<Result<Vec<Vec<(usize, Duration)>>, _>>()
        .wrap_err("failed running one or more clients")?
        .into_iter()
        .flatten()
        .unzip();
    let tot_bytes = tot_bytes.into_iter().sum();
    let elapsed = durs.into_iter().max().unwrap();
    info!(?tot_bytes, ?elapsed, "all clients done");
    Ok((tot_bytes, elapsed))
}

async fn run_client_inner<C: ChunnelConnection<Data = (SocketAddr, Vec<u8>)>>(
    cn: C,
    addr: SocketAddrV4,
    download_size: usize,
    packet_size: usize,
) -> Result<(usize, Duration), Report> {
    info!(?addr, ?download_size, ?packet_size, "starting client");

    let mut tot_bytes = 0;
    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
    req.extend((download_size as u64).to_le_bytes());
    req.extend((packet_size as u64).to_le_bytes());
    cn.send(std::iter::once((SocketAddr::V4(addr), req.clone())))
        .await?;
    let start = Instant::now();
    let mut last_recv_time = None;
    let mut slots: Vec<_> = (0..16).map(|_| Default::default()).collect();
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
        for (_, r) in ms.iter_mut().map_while(Option::take) {
            tot_bytes += r.len();
            trace!(?tot_bytes, "received part");
            if r[0] == 1 {
                cn.send(std::iter::once((SocketAddr::V4(addr), r))).await?;
                break 'cn;
            }
        }
    }

    let elapsed = last_recv_time.unwrap() - start;
    info!(?tot_bytes, ?elapsed, "done");
    Ok((tot_bytes, elapsed))
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
