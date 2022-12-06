//! Achieved throughput as number of connections and file size increases.
//!
//! One connection per request, n simultaneous clients looping on establishing connections that
//! each download m bytes.

use bertha::{
    udp::{UdpReqChunnel, UdpSkChunnel},
    ChunnelConnection, ChunnelConnector, ChunnelListener,
};
use color_eyre::eyre::{bail, Report, WrapErr};
use dpdk_direct::{DpdkInlineChunnel, DpdkInlineReqChunnel, DpdkUdpReqChunnel, DpdkUdpSkChunnel};
use dpdk_wrapper::DpdkIoKernel;
use futures_util::stream::TryStreamExt;
use shenango_chunnel::{ShenangoUdpReqChunnel, ShenangoUdpSkChunnel};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{debug, info, info_span, trace, warn};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
use tracing_subscriber::prelude::*;

#[derive(Debug, Clone, StructOpt)]
struct Opt {
    #[structopt(long)]
    cfg: PathBuf,

    #[structopt(short, long)]
    port: u16,

    #[structopt(long)]
    datapath: String,

    #[structopt(long)]
    no_bertha: bool,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(Debug, Clone, StructOpt)]
struct Client {
    #[structopt(long)]
    addr: Ipv4Addr,

    #[structopt(long)]
    num_clients: usize,

    #[structopt(long)]
    download_size: usize,

    #[structopt(long)]
    out_file: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, StructOpt)]
enum Mode {
    Client(Client),
    Server,
}

fn main() -> Result<(), Report> {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    color_eyre::install()?;
    let Opt {
        cfg,
        port,
        datapath,
        no_bertha,
        mode,
    } = Opt::from_args();

    if no_bertha && datapath == "shenango" {
        shenango_nobertha(cfg, port, mode);
        unreachable!();
    }

    if datapath == "shenangort" {
        shenangort_bertha(cfg, port, mode);
        unreachable!();
    }

    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let num_clients = cl.num_clients;
        let of = cl.out_file.take();

        let (tot_bytes, elapsed) = if no_bertha {
            match datapath.as_str() {
                "dpdkthread" => {
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(4)
                        .enable_all()
                        .build()?;
                    rt.block_on(run_clients_no_bertha(cl, port, cfg))?
                }
                _ => bail!("no_bertha with non-dpdk not implemented"),
            }
        } else {
            match datapath.as_str() {
                "dpdkthread" => {
                    let ch = DpdkUdpSkChunnel::new(cfg).wrap_err("make dpdk-thread chunnel")?;
                    run_clients(ch, cl, port)?
                }
                "dpdkmulti" => {
                    let ch = DpdkInlineChunnel::new(cfg, 4).wrap_err("make dpdk-multi chunnel")?;
                    run_clients(ch, cl, port)?
                }
                "shenango" => {
                    let ch = ShenangoUdpSkChunnel::new(cfg);
                    run_clients(ch, cl, port)?
                }
                "kernel" => {
                    let ch = UdpSkChunnel;
                    run_clients(ch, cl, port)?
                }
                d => {
                    bail!("unknown datapath {:?}", d);
                }
            }
        };

        let rate = (tot_bytes as f64 * 8.) / elapsed.as_secs_f64();
        info!(?num_clients, ?download_size, rate_mbps=?(rate / 1e6), "finished");
        if let Some(of) = of {
            use std::io::Write;
            let mut f = std::fs::File::create(of)?;
            writeln!(
                &mut f,
                "num_clients,download_size,tot_bytes,elapsed_us,rate_bps"
            )?;
            writeln!(
                &mut f,
                "{:?},{:?},{:?},{:?},{:?}",
                num_clients,
                download_size,
                tot_bytes,
                elapsed.as_micros(),
                rate
            )?;
        } else {
            println!(
                "num_clients={:?},download_size={:?},tot_bytes={:?},elapsed_us={:?},rate_bps={:?}",
                num_clients,
                download_size,
                tot_bytes,
                elapsed.as_micros(),
                rate
            );
        }
    } else {
        if no_bertha {
            match datapath.as_ref() {
                "dpdkthread" => {
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(4)
                        .enable_all()
                        .build()?;
                    rt.block_on(run_server_no_bertha(port, cfg))?;
                }
                _ => bail!("non-dpdk no-bertha not implemented"),
            }
        } else {
            match datapath.as_str() {
                "dpdkthread" => {
                    let ch = DpdkUdpSkChunnel::new(cfg)?;
                    let ch = DpdkUdpReqChunnel(ch);
                    run_server(ch, port, 4)?;
                }
                "dpdkinline" => {
                    let ch = DpdkInlineChunnel::new(cfg, 4)?;
                    let ch = DpdkInlineReqChunnel::from(ch);
                    run_server(ch, port, 4)?;
                }
                "shenango" => {
                    let ch = ShenangoUdpSkChunnel::new(cfg);
                    let ch = ShenangoUdpReqChunnel(ch);
                    run_server(ch, port, 4)?;
                }
                "kernel" => {
                    let ch = UdpReqChunnel;
                    run_server(ch, port, 4)?;
                }
                d => {
                    bail!("unknown datapath {:?}", d);
                }
            }
        }
    }

    Ok(())
}

fn run_clients<C, Cn, E>(ctr: C, c: Client, port: u16) -> Result<(usize, Duration), Report>
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

    let clients: Vec<_> = (0..c.num_clients)
        .map(|i| {
            let ctr = ctr.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(
                    run_client(ctr, addr, c.download_size).instrument(info_span!("client", ?i)),
                )
            })
        })
        .collect();

    let joined: Vec<Result<(usize, Duration), Report>> = clients
        .into_iter()
        .map(|jh| jh.join().expect("thread paniced"))
        .collect();
    let (tot_bytes, durs): (Vec<usize>, Vec<Duration>) = joined
        .into_iter()
        .collect::<Result<Vec<(usize, Duration)>, _>>()
        .wrap_err("failed running one or more clients")?
        .into_iter()
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
) -> Result<(usize, Duration), Report>
where
    C: ChunnelConnector<Addr = SocketAddr, Connection = Cn, Error = E> + Send + Sync + 'static,
    Cn: ChunnelConnection<Data = (SocketAddr, Vec<u8>)> + Send + Sync + 'static,
    E: Into<Report> + Send + Sync + 'static,
{
    info!(?addr, ?download_size, "starting client");
    let stack = bertha::util::Nothing::<()>::default();
    let mut tot_bytes = 0;
    let start = Instant::now();

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

    // 2. get bytes
    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
    req.extend((download_size as u64).to_le_bytes());
    cn.send(std::iter::once((addr, req))).await?;
    let mut last_recv_time = Instant::now();
    let mut slots: Vec<_> = (0..16).map(|_| Default::default()).collect();
    'cn: loop {
        let ms = cn.recv(&mut slots).await?;
        for (_, r) in ms.iter_mut().map_while(Option::take) {
            tot_bytes += r.len();
            trace!(?tot_bytes, "received part");
            if r[0] == 1 {
                cn.send(std::iter::once((addr, r))).await?;
                break 'cn;
            } else {
                last_recv_time = Instant::now();
            }
        }
    }

    let elapsed = last_recv_time - start;
    info!(?tot_bytes, ?elapsed, "done");
    Ok((tot_bytes, elapsed))
}

fn run_server<L, Cn, E>(listener: L, port: u16, threads: usize) -> Result<(), Report>
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
            st.try_for_each_concurrent(None, |cn| async move {
                let mut slots = [None; 1];
                let ms = cn.recv(&mut slots).await?;
                let (a, msg) = match ms {
                    [Some((a, msg))] if msg[..8] == [1, 2, 3, 4, 5, 6, 7, 8] => {
                        (*a, std::mem::take(msg))
                    }
                    msg => {
                        warn!(?msg, "bad client request");
                        bail!("bad connection: {:?}", msg);
                    }
                };

                let mut remaining = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                let start = Instant::now();
                info!(?remaining, ?a, "starting send");
                while remaining > 0 {
                    let bufs = (0..16).map_while(|_| {
                        if remaining > 0 {
                            let this_send_size = std::cmp::min(1460, remaining);
                            remaining -= this_send_size;
                            Some((a, vec![0u8; this_send_size as usize]))
                        } else {
                            None
                        }
                    });
                    cn.send(bufs).await?;
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
            .await?;
            unreachable!() // negotiate_server never returns None
        })
    }

    info!(?port, ?threads, "starting server");
    for _ in 1..threads {
        let listener = listener.clone();
        std::thread::spawn(move || server_thread(listener, port));
    }

    server_thread(listener, port)
}

async fn run_clients_no_bertha(
    c: Client,
    port: u16,
    cfg: std::path::PathBuf,
) -> Result<(usize, Duration), Report> {
    let addr = SocketAddrV4::new(c.addr, port);
    info!(?addr, "starting clients");
    let (handle_s, handle_r) = flume::bounded(1);
    std::thread::spawn(move || {
        let (iokernel, handle) = DpdkIoKernel::new(cfg).unwrap();
        handle_s.send(handle).unwrap();
        iokernel.run();
    });
    let handle = handle_r.recv().wrap_err("Could not start DpdkIoKernel")?;

    let clients: futures_util::stream::FuturesUnordered<_> = (0..c.num_clients)
        .map(|i| {
            let res = handle.socket(None);
            tokio::spawn(async move {
                Ok(run_client_no_bertha(res?, addr, c.download_size)
                    .instrument(info_span!("client", ?i))
                    .await?)
            })
        })
        .collect();

    let joined: Vec<Result<(usize, Duration), Report>> = clients
        .try_collect()
        .await
        .wrap_err("failed running one or more clients")?;
    let (tot_bytes, durs): (Vec<usize>, Vec<Duration>) = joined
        .into_iter()
        .collect::<Result<Vec<(usize, Duration)>, _>>()?
        .into_iter()
        .unzip();
    let tot_bytes = tot_bytes.into_iter().sum();
    let elapsed = durs.into_iter().max().unwrap();
    info!(?tot_bytes, ?elapsed, "all clients done");
    Ok((tot_bytes, elapsed))
}

async fn run_client_no_bertha(
    cn: dpdk_wrapper::DpdkConn,
    addr: SocketAddrV4,
    download_size: usize,
) -> Result<(usize, Duration), Report> {
    info!(?addr, ?download_size, "starting client");
    let addr = SocketAddr::V4(addr);
    let mut tot_bytes = 0;
    let start = Instant::now();

    // 2. get bytes
    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
    req.extend((download_size as u64).to_le_bytes());
    cn.send_async(addr, req).await?;
    let mut last_recv_time = Instant::now();
    loop {
        let (_, r) = cn.recv_async().await?;
        tot_bytes += r.len();
        trace!(?tot_bytes, "received part");
        if r[0] == 1 {
            cn.send_async(addr, r).await?;
            break;
        } else {
            last_recv_time = Instant::now();
        }
    }

    let elapsed = last_recv_time - start;
    info!(?tot_bytes, ?elapsed, "done");
    Ok((tot_bytes, elapsed))
}

async fn run_server_no_bertha(port: u16, cfg: std::path::PathBuf) -> Result<(), Report> {
    info!(?port, "starting server");
    let (handle_s, handle_r) = flume::bounded(1);
    std::thread::spawn(move || {
        let (iokernel, handle) = DpdkIoKernel::new(cfg).unwrap();
        handle_s.send(handle).unwrap();
        iokernel.run();
    });
    let handle = handle_r.recv().wrap_err("Could not start DpdkIoKernel")?;
    let st = handle.accept(port)?;
    loop {
        let cn = st.recv_async().await?;
        tokio::spawn(async move {
            let (a, msg) = cn.recv_async().await?;
            if &msg[..8] != [1, 2, 3, 4, 5, 6, 7, 8] {
                debug!("bad client request");
                bail!("bad connection");
            }

            let mut remaining = u64::from_le_bytes(msg[8..16].try_into().unwrap());
            let start = Instant::now();
            info!(?remaining, ?a, "starting send");
            while remaining > 0 {
                let this_send_size = std::cmp::min(1460, remaining);
                let buf = vec![0u8; this_send_size as usize];
                cn.send_async(buf).await?;
                remaining -= this_send_size;
            }

            info!(elapsed = ?start.elapsed(), ?a, "done sending");
            // fin
            cn.send_async(vec![1u8]).await?;
            let mut f = Box::pin(cn.recv_async());
            loop {
                match futures_util::future::select(
                    f,
                    Box::pin(tokio::time::sleep(Duration::from_millis(1))),
                )
                .await
                {
                    futures_util::future::Either::Left((_, _)) => break,
                    futures_util::future::Either::Right((_, rem)) => {
                        cn.send_async(vec![1u8]).await?;
                        f = rem;
                    }
                }
            }

            info!(elapsed = ?start.elapsed(), ?a, "exiting");
            Ok::<_, Report>(())
        });
    }
}

fn shenangort_bertha(cfg: std::path::PathBuf, port: u16, mode: Mode) {
    use shenango::udp;
    use shenango_bertha::ChunnelConnection;
    use std::sync::atomic::{AtomicUsize, Ordering};
    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let num_clients = cl.num_clients;
        let of = cl.out_file.take();
        let addr = SocketAddrV4::new(cl.addr, port);

        shenango::runtime_init(cfg.to_str().unwrap().to_owned(), move || {
            use rand::Rng;
            let mut jhs = Vec::with_capacity(cl.num_clients);
            let wg = shenango::sync::WaitGroup::new();
            wg.add(cl.num_clients as _);
            for i in 0..cl.num_clients {
                let wg = wg.clone();
                let jh = shenango::thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    let cn = udp::UdpConnection::dial(
                        SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
                        addr,
                    )?;

                    // space these out
                    let start_wait: u64 = rng.gen_range(0..100);
                    shenango::time::sleep(Duration::from_millis(start_wait));

                    use shenango_bertha::udp::UdpChunnelConnection;
                    let stack = shenango_bertha::chunnels::Nothing::<()>::default();
                    let cn = shenango_bertha::negotiate_client(stack,UdpChunnelConnection::new(cn), addr).wrap_err("negotiation failed")?;
                    let cn = Arc::new(cn);

                    wg.done();
                    wg.wait();

                    info!(?addr, ?download_size, ?i, "starting_client");
                    let mut tot_bytes = 0;

                    // 2. get bytes
                    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
                    req.extend((download_size as u64).to_le_bytes());
                    req.extend((cl.num_clients as u32).to_le_bytes());
                    cn.send((addr, req.clone()))?;
                    let mut start = Instant::now();

                    const RECV: u32 = 1;
                    const TIME: u32 = 2;
                    let mut p = shenango::poll::PollWaiter::new();
                    let recv_trigger = p.trigger(RECV);
                    let cn2 = Arc::clone(&cn);
                    let recv_jh = shenango::thread::spawn(move || {
                        let _recv_trigger = recv_trigger;
                        let (_, m) = cn2.recv()?;
                        Ok::<_, Report>(m.len())
                    });

                    let mut cnt = 0;
                    loop {
                        let sleep_trigger = p.trigger(TIME);
                        shenango::thread::spawn(move || {
                            let _sleep_trigger = sleep_trigger;
                            shenango::time::sleep(Duration::from_millis(5));
                        });

                        if p.wait() == TIME {
                            cnt += 1;
                            if cnt > 50 {
                              info!(?i, "terminate");
                              return Ok((0, start.elapsed()));
                            }

                            debug!(elapsed = ?start.elapsed(), ?i, "retrying req");
                            start = Instant::now(); // measure from first byte received
                            cn.send((addr, req.clone()))?;
                        } else {
                            break;
                        }
                    }

                    tot_bytes += recv_jh.join().unwrap()?;
                    info!(elapsed = ?start.elapsed(), ?i, "connection_started");
                    let mut p = shenango::poll::PollWaiter::new();
                    let sleep_trigger = p.trigger(TIME);
                    shenango::thread::spawn(move || {
                        let _sleep_trigger = sleep_trigger;
                        shenango::time::sleep(Duration::from_secs(15));
                    });

                    let latest_elapsed = Arc::new(AtomicUsize::new((Instant::now() - start).as_micros() as _));
                    let tot_bytes = Arc::new(AtomicUsize::new(tot_bytes));
                    let recv_trigger = p.trigger(RECV);
                    let le = Arc::clone(&latest_elapsed);
                    let tb = Arc::clone(&tot_bytes);
                    shenango::thread::spawn(move || {
                        let _recv_trigger = recv_trigger;
                        let tot_bytes = tb;
                        let latest_elapsed = le;
                        loop {
                            let (_, m) = cn.recv()?;
                            assert!(!m.is_empty());
                            tot_bytes.fetch_add(m.len(), Ordering::Relaxed);
                            trace!(?tot_bytes, ?i, "received part");
                            if m[0] == 1 {
                                break;
                            } else {
                                let elapsed = Instant::now() - start;
                                latest_elapsed.store(elapsed.as_micros() as _, Ordering::Relaxed);
                            }
                        }

                        cn.send((addr, vec![1u8; 16]))?;
                        Ok::<_, Report>(())
                    });

                    p.wait();

                    let tot_bytes = tot_bytes.load(Ordering::SeqCst);
                    let elapsed = Duration::from_micros(latest_elapsed.load(Ordering::SeqCst) as _);
                    info!(?tot_bytes, ?elapsed, ?i, "done");
                    Ok((tot_bytes, elapsed))
                });
                jhs.push(jh);
            }

            wg.wait();
            let joined: Vec<Result<(usize, Duration), Report>> =
                jhs.into_iter().map(|jh| jh.join().unwrap()).collect();
            let (tot_bytes, durs): (Vec<usize>, Vec<Duration>) = joined
                .into_iter()
                .collect::<Result<Vec<(usize, Duration)>, _>>()
                .unwrap()
                .into_iter()
                .unzip();
            let tot_bytes: usize = tot_bytes.into_iter().sum();
            let elapsed = durs.into_iter().max().unwrap();
            info!(?tot_bytes, ?elapsed, "all clients done");

            let rate = (tot_bytes as f64 * 8.) / elapsed.as_secs_f64();
            info!(?num_clients, ?download_size, rate_mbps=?(rate / 1e6), "finished");
            if let Some(of) = of {
                use std::io::Write;
                let mut f = std::fs::File::create(of).unwrap();
                writeln!(
                    &mut f,
                    "num_clients,download_size,tot_bytes,elapsed_us,rate_bps"
                )
                .unwrap();
                writeln!(
                    &mut f,
                    "{:?},{:?},{:?},{:?},{:?}",
                    num_clients,
                    download_size,
                    tot_bytes,
                    elapsed.as_micros(),
                    rate
                )
                .unwrap();
            } else {
                println!(
                    "num_clients={:?},download_size={:?},tot_bytes={:?},elapsed_us={:?},rate_bps={:?}",
                    num_clients,
                    download_size,
                    tot_bytes,
                    elapsed.as_micros(),
                    rate
                );
            }

            std::process::exit(0);
        }).unwrap();
    } else {
        info!(?port, "starting server");
        let listen_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
        shenango::runtime_init(cfg.to_str().unwrap().to_owned(), move || {
            let activated_clients = Arc::new(AtomicUsize::default());
            let wg = shenango::sync::WaitGroup::new();
            shenango_bertha::negotiate_server(
                shenango_bertha::chunnels::Nothing::<()>::default(),
                listen_addr,
                move |cn| {
                    let activated_clients = Arc::clone(&activated_clients);
                    let wg = wg.clone();
                    let (a, msg) = match cn.recv() {
                        Ok(x) => x,
                        Err(e) => {
                            warn!(?e, "request read failed");
                            return;
                        }
                    };

                    if &msg[..8] != [1, 2, 3, 4, 5, 6, 7, 8] {
                        debug!("bad client request");
                        return;
                    }

                    let mut remaining = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                    let num_clients = u32::from_le_bytes(msg[16..20].try_into().unwrap()) as usize;

                    if num_clients == 0 {
                        debug!("bad client request");
                        return;
                    }

                    match activated_clients.compare_exchange(
                        0,
                        num_clients,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(0) => {
                            wg.add((num_clients - 1) as _);
                        }
                        Err(c) => {
                            if c != num_clients {
                                debug!(?c, ?num_clients, "unexpected number of clients");
                            }

                            wg.done();
                        }
                        Ok(_) => unreachable!(),
                    }

                    wg.wait();

                    let start = Instant::now();
                    info!(?remaining, ?a, "starting send");
                    while remaining > 0 {
                        let this_send_size = std::cmp::min(1460, remaining);
                        let buf = vec![0u8; this_send_size as usize];
                        if let Err(e) = cn.send((a, buf)) {
                            trace!(?e, "write errored");
                        } else {
                            remaining -= this_send_size;
                        }
                    }

                    info!(elapsed = ?start.elapsed(), ?a, "done sending");
                    // fin
                    let fin_buf = vec![1u8; 1];
                    if let Err(e) = cn.send((a, fin_buf)) {
                        warn!(?e, "fin write failed");
                    }

                    const RECV: u32 = 1;
                    const TIME: u32 = 2;
                    let mut p = shenango::poll::PollWaiter::new();
                    let recv_trigger = p.trigger(RECV);
                    let cn = Arc::new(cn);
                    let cn2 = Arc::clone(&cn);
                    shenango::thread::spawn(move || {
                        let _recv_trigger = recv_trigger;
                        if let Err(e) = cn.recv() {
                            warn!(?e, "recv errored");
                        }
                    });

                    loop {
                        let sleep_trigger = p.trigger(TIME);
                        shenango::thread::spawn(move || {
                            let _sleep_trigger = sleep_trigger;
                            shenango::time::sleep(Duration::from_millis(100));
                        });

                        if p.wait() == TIME {
                            debug!(elapsed = ?start.elapsed(), ?a, "retrying fin");
                            let fin_buf = vec![1u8; 1];
                            if let Err(e) = cn2.send((a, fin_buf)) {
                                warn!(?e, "fin write failed");
                            }
                        } else {
                            break;
                        }
                    }

                    let remaining_conns = activated_clients.fetch_sub(1, Ordering::SeqCst);
                    info!(?remaining_conns, elapsed = ?start.elapsed(), ?a, "exiting");
                },
            )
            .unwrap();
        })
        .unwrap();
    }
}

fn shenango_nobertha(cfg: std::path::PathBuf, port: u16, mode: Mode) {
    use shenango::udp;
    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let num_clients = cl.num_clients;
        let of = cl.out_file.take();
        let addr = SocketAddrV4::new(cl.addr, port);

        shenango::runtime_init(cfg.to_str().unwrap().to_owned(), move || {
            let wg = shenango::sync::WaitGroup::new();
            wg.add(cl.num_clients as _);
            let mut jhs = Vec::with_capacity(cl.num_clients);
            for i in 0..cl.num_clients {
                let wg = wg.clone();
                let jh = shenango::thread::spawn(move || {
                    let cn = udp::UdpConnection::dial(
                        SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
                        addr,
                    )?;
                    let cn = Arc::new(cn);

                    wg.done();
                    wg.wait();

                    info!(?addr, ?download_size, "starting client");
                    let mut tot_bytes = 0;
                    let mut start = Instant::now();

                    // 2. get bytes
                    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
                    req.extend((download_size as u64).to_le_bytes());
                    cn.write_to(&req, addr)?;

                    const RECV: u32 = 1;
                    const TIME: u32 = 2;
                    let mut p = shenango::poll::PollWaiter::new();
                    let recv_trigger = p.trigger(RECV);
                    let cn2 = Arc::clone(&cn);
                    let recv_jh = shenango::thread::spawn(move || {
                        let _recv_trigger = recv_trigger;
                        let mut buf = [0u8; 2048];
                        cn2.recv(&mut buf)
                    });

                    let mut cnt = 0;
                    loop {
                        let sleep_trigger = p.trigger(TIME);
                        shenango::thread::spawn(move || {
                            let _sleep_trigger = sleep_trigger;
                            shenango::time::sleep(Duration::from_millis(5));
                        });

                        debug!(elapsed = ?start.elapsed(), ?i, "waiting");
                        if p.wait() == TIME {
                            cnt += 1;
                            debug!(elapsed = ?start.elapsed(), ?i, ?cnt, "retrying req");
                            if cnt > 50 {
                                return Ok((0, start.elapsed()));
                            }

                            start = Instant::now();
                            cn.write_to(&req, addr)?;
                        } else {
                            break;
                        }
                    }

                    debug!(elapsed = ?start.elapsed(), ?i, "connection started");
                    tot_bytes += recv_jh.join().unwrap()?;

                    let mut buf = [0u8; 2048];
                    let mut last_recv_time = Instant::now();
                    loop {
                        let s = cn.recv(&mut buf[0..2048])?;
                        assert!(s != 0);
                        tot_bytes += s;
                        trace!(?tot_bytes, ?i, ?s, "received part");
                        if buf[0] == 1 {
                            cn.write_to(&buf[0..16], addr)?;
                            break;
                        } else {
                            last_recv_time = Instant::now();
                        }
                    }

                    let elapsed = last_recv_time - start;
                    info!(?tot_bytes, ?elapsed, "done");
                    Ok((tot_bytes, elapsed))
                });
                jhs.push(jh);
            }

            let joined: Vec<Result<(usize, Duration), Report>> =
                jhs.into_iter().map(|jh| jh.join().unwrap()).collect();
            let (tot_bytes, durs): (Vec<usize>, Vec<Duration>) = joined
                .into_iter()
                .collect::<Result<Vec<(usize, Duration)>, _>>()
                .unwrap()
                .into_iter()
                .unzip();
            let tot_bytes: usize = tot_bytes.into_iter().sum();
            let elapsed = durs.into_iter().max().unwrap();
            info!(?tot_bytes, ?elapsed, "all clients done");

            let rate = (tot_bytes as f64 * 8.) / elapsed.as_secs_f64();
            info!(?num_clients, ?download_size, rate_mbps=?(rate / 1e6), "finished");
            if let Some(of) = of {
                use std::io::Write;
                let mut f = std::fs::File::create(of).unwrap();
                writeln!(
                    &mut f,
                    "num_clients,download_size,tot_bytes,elapsed_us,rate_bps"
                )
                .unwrap();
                writeln!(
                    &mut f,
                    "{:?},{:?},{:?},{:?},{:?}",
                    num_clients,
                    download_size,
                    tot_bytes,
                    elapsed.as_micros(),
                    rate
                )
                .unwrap();
            } else {
                println!(
                    "num_clients={:?},download_size={:?},tot_bytes={:?},elapsed_us={:?},rate_bps={:?}",
                    num_clients,
                    download_size,
                    tot_bytes,
                    elapsed.as_micros(),
                    rate
                );
            }

            std::process::exit(0);
        }).unwrap();
    } else {
        info!(?port, "starting server");
        let listen_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
        shenango::runtime_init(cfg.to_str().unwrap().to_owned(), move || {
            udp::udp_accept(listen_addr, move |cn| {
                let mut buf = [0u8; 2048];
                let (sz, a) = match cn.read_from(&mut buf[..]) {
                    Ok(x) => x,
                    Err(e) => {
                        warn!(?e, "request read failed");
                        return;
                    }
                };
                let msg = &buf[..sz];
                if &msg[..8] != [1, 2, 3, 4, 5, 6, 7, 8] {
                    debug!("bad client request");
                    return;
                }

                let mut remaining = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                let start = Instant::now();
                info!(?remaining, ?a, "starting send");
                while remaining > 0 {
                    let this_send_size = std::cmp::min(1460, remaining);
                    let buf = vec![0u8; this_send_size as usize];
                    if let Err(e) = cn.write_to(&buf, a) {
                        trace!(?e, "write errored");
                    }
                    remaining -= this_send_size;
                }

                info!(elapsed = ?start.elapsed(), ?a, "done sending");
                // fin
                let fin_buf = [1u8; 1];
                if let Err(e) = cn.write_to(&fin_buf, a) {
                    warn!(?e, "fin write failed");
                }

                const RECV: u32 = 1;
                const TIME: u32 = 2;
                let mut p = shenango::poll::PollWaiter::new();
                let recv_trigger = p.trigger(RECV);
                let cn2 = cn.clone();
                shenango::thread::spawn(move || {
                    let _recv_trigger = recv_trigger;
                    let mut buf = [0u8; 1024];
                    if let Err(e) = cn.recv(&mut buf) {
                        warn!(?e, "recv errored");
                    }
                });

                loop {
                    let sleep_trigger = p.trigger(TIME);
                    shenango::thread::spawn(move || {
                        let _sleep_trigger = sleep_trigger;
                        shenango::time::sleep(Duration::from_millis(100));
                    });

                    if p.wait() == TIME {
                        debug!(elapsed = ?start.elapsed(), ?a, "retrying fin");
                        let fin_buf = [1u8; 1];
                        cn2.write_to(&fin_buf, a).unwrap();
                    } else {
                        break;
                    }
                }

                info!(elapsed = ?start.elapsed(), ?a, "exiting");
            })
            .unwrap();
        })
        .unwrap();
    }
}
