use super::{Client, Mode};
use bertha::{ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use flume::Sender;
use futures_util::{future::TryFutureExt, stream::FuturesUnordered, Stream, TryStreamExt};
use std::net::{SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::Instrument;
use tracing::{debug, debug_span, info, info_span, trace, warn};

use dpdk_direct::{DpdkInlineChunnel, DpdkInlineCn, DpdkInlineReqChunnel};
use dpdk_direct::{DpdkState, Msg, SendMsg, DPDK_STATE};

pub fn dpdk_inline_nobertha(
    cfg: std::path::PathBuf,
    port: u16,
    mode: Mode,
    num_threads: usize,
) -> Result<(), Report> {
    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let packet_size = cl.packet_size;
        let num_clients = cl.num_clients;
        let of = cl.out_file.take();
        let (tot_bytes, elapsed) = run_clients(cl, cfg, port, num_threads)?;
        super::write_results(
            of,
            tot_bytes,
            elapsed,
            num_clients,
            download_size,
            packet_size,
        )?;
        Ok(())
    } else {
        run_server(cfg, port, num_threads)
    }
}

fn run_clients(
    c: Client,
    cfg: std::path::PathBuf,
    port: u16,
    num_threads: usize,
) -> Result<(usize, Duration), Report> {
    let addr = SocketAddrV4::new(c.addr, port);
    let ctr = DpdkInlineChunnel::new(cfg, num_threads)?;

    let client_threads: Vec<_> = if c.num_clients > num_threads {
        let clients_per_thread = c.num_clients / num_threads;
        let mut remainder = c.num_clients - (num_threads * clients_per_thread);
        (0..num_threads)
            .map(|thread| {
                let ctr = ctr.clone();
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
                    rt.block_on(async move {
                        let futs: FuturesUnordered<_> = (0..num_thread_clients)
                            .map(|tclient| {
                                run_client(ctr.clone(), addr, c.download_size, c.packet_size)
                                    .instrument(info_span!("client", ?thread, ?tclient))
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
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    rt.block_on(async move {
                        let res = run_client(ctr, addr, c.download_size, c.packet_size)
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
        .flat_map(|x| x)
        .unzip();
    let tot_bytes = tot_bytes.into_iter().sum();
    let elapsed = durs.into_iter().max().unwrap();
    info!(?tot_bytes, ?elapsed, "all clients done");
    Ok((tot_bytes, elapsed))
}

async fn run_client(
    mut ctr: DpdkInlineChunnel,
    addr: SocketAddrV4,
    download_size: usize,
    packet_size: usize,
) -> Result<(usize, Duration), Report> {
    let cn: DpdkInlineCn = ctr
        .connect(SocketAddr::V4(addr))
        .await
        .wrap_err("connector failed")?;

    let local_port = cn.local_port();
    let remote_addr = addr;
    let this_lcore = dpdk_direct::get_lcore_id();
    info!(
        ?remote_addr,
        ?local_port,
        ?this_lcore,
        ?download_size,
        ?packet_size,
        "starting client"
    );

    let mut tot_bytes = 0;
    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
    req.extend((download_size as u64).to_le_bytes());
    req.extend((packet_size as u64).to_le_bytes());

    let req = SendMsg {
        src_port: local_port,
        to_addr: remote_addr,
        buf: req,
    };

    DPDK_STATE
        .try_with(|dpdk_cell| {
            let mut dpdk_opt = dpdk_cell.borrow_mut();
            let dpdk = dpdk_opt
                .as_mut()
                .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
            dpdk.send_burst(std::iter::once(req.clone()))?;
            Ok::<_, Report>(())
        })
        .map_err(Into::into)
        .and_then(|x| x)?;

    let start = Instant::now();
    let mut retx_time = Instant::now();
    let mut last_recv_time: Option<Instant> = None;
    let mut req_try = 1;
    let mut slots: [Option<Msg>; 16] = (0..16)
        .map(|_| None)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    'cn: loop {
        let num_received = 'recv: loop {
            tokio::task::yield_now().await;
            let nr = DPDK_STATE
                .try_with(|dpdk_cell| {
                    let mut dpdk_opt = dpdk_cell.borrow_mut();
                    let dpdk = dpdk_opt
                        .as_mut()
                        .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                    let ms =
                        dpdk.try_recv_burst(Some((local_port, Some(remote_addr))), None, None)?;
                    for (m, slot) in ms.iter_mut().map_while(Option::take).zip(slots.iter_mut()) {
                        *slot = Some(m);
                    }
                    Ok::<_, Report>(ms.len())
                })
                .map_err(Into::into)
                .and_then(|x| x)?;
            if nr > 0 {
                if last_recv_time.is_none() {
                    debug!(?req_try, "got first response");
                }

                last_recv_time = Some(Instant::now());
                break 'recv nr;
            } else if last_recv_time.is_none() && retx_time.elapsed() > Duration::from_millis(100) {
                debug!(?req_try, "retransmitting request");
                req_try += 1;
                retx_time = Instant::now();
                DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        dpdk.send_burst(std::iter::once(req.clone()))?;
                        Ok::<_, Report>(())
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)?;
            }
        };

        for msg in slots[..num_received].iter_mut().map_while(Option::take) {
            let r = msg.get_buf();
            tot_bytes += r.len();
            trace!(?tot_bytes, "received part");
            if r[0] == 1 {
                DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        dpdk.send_burst(std::iter::once(SendMsg {
                            src_port: local_port,
                            to_addr: remote_addr,
                            buf: vec![1u8],
                        }))?;
                        Ok::<_, Report>(())
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)?;
                break 'cn;
            }
        }
    }

    let elapsed = last_recv_time.unwrap() - start;
    info!(?tot_bytes, ?elapsed, "done");
    Ok((tot_bytes, elapsed))
}

pub fn run_server(cfg: PathBuf, port: u16, threads: usize) -> Result<(), Report> {
    let ch = DpdkInlineChunnel::new(cfg, threads)?;
    let ch = DpdkInlineReqChunnel::from(ch);

    fn server_thread(mut ch: DpdkInlineReqChunnel, port: u16, thread: usize) -> Result<(), Report> {
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
            server_thread_inner(st)
                .instrument(debug_span!("server_thread", ?thread))
                .await?;
            unreachable!()
        })
    }

    info!(?port, ?threads, "starting server");
    for i in 1..threads {
        let ch = ch.clone();
        std::thread::spawn(move || server_thread(ch, port, i));
    }

    server_thread(ch, port, 0)
}

// We are going to use the Stream for connection setup and handling, but we are then going to
// ignore the connection object and call methods directly on the thread local DpdkState it has set
// up.
//
// Because we are skipping negotiation, there is no natural synchronization point. The stream
// implementation will not try to create new connections either, since it has already returned one.
// We thus have to periodically try receiving during the send.
async fn server_thread_inner<S: Stream<Item = Result<DpdkInlineCn, Report>> + Unpin>(
    st: S,
) -> Result<(), Report> {
    st.try_for_each_concurrent(None, |cn| {
        let this_lcore = dpdk_direct::get_lcore_id();
        let local_port = cn.local_port();
        let remote_addr = cn
            .remote_addr()
            .expect("Connection was not connected to remote address");
        let new_conns: Option<Sender<_>> = cn.new_conn_signaller().cloned();
        async move {
            // move cn into future.
            let _cn = cn;
            // this loop has a recv in it.
            let (a, (mut remaining, pkt_size)) = loop {
                let ret = DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        let msgs = dpdk.try_recv_burst(
                            Some((local_port, Some(remote_addr))),
                            new_conns.as_ref(),
                            Some(1),
                        )?;

                        if let Some(msg) = msgs.iter_mut().map_while(Option::take).next() {
                            ensure!(msg.port == local_port, "Port mismatch");
                            // message validation.
                            Ok(Some((msg.addr, super::validate_message(msg.get_buf())?)))
                        } else {
                            Ok(None)
                        }
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)?;
                if let Some(x) = ret {
                    debug!(req = ?x, "got request");
                    break Ok::<_, Report>(x);
                } else {
                    tokio::task::yield_now().await;
                }
            }
            .wrap_err(eyre!("Error waiting for request"))?;

            let start = Instant::now();
            info!(?remaining, ?pkt_size, ?a, "starting send");
            while remaining > 0 {
                // this loop has no recv, so we do a stash-only one.
                DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        dpdk.try_recv_burst_stash_only(new_conns.as_ref())?;
                        Ok::<_, Report>(())
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)?;
                // we yield afterwards so that the stream future can process the potential new
                // connection.
                tokio::task::yield_now().await;
                DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        dpdk.send_burst((0..16).map_while(|_| {
                            if remaining > 0 {
                                let this_send_size = std::cmp::min(pkt_size, remaining);
                                remaining -= this_send_size;
                                Some(SendMsg {
                                    src_port: local_port,
                                    to_addr: remote_addr,
                                    buf: vec![0u8; this_send_size as usize],
                                })
                            } else {
                                None
                            }
                        }))?;
                        Ok::<_, Report>(())
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)?;
            }
            info!(elapsed = ?start.elapsed(), ?a, "done sending");

            // fin
            fn send_fin(
                dpdk: &mut DpdkState,
                local_port: u16,
                a: SocketAddrV4,
            ) -> Result<(), Report> {
                dpdk.send_burst(std::iter::once(SendMsg {
                    src_port: local_port,
                    to_addr: a,
                    buf: vec![1u8],
                }))?;
                Ok(())
            }

            fn recv_one(
                dpdk: &mut DpdkState,
                local_port: u16,
                remote_addr: SocketAddrV4,
                new_conns: Option<&Sender<SocketAddrV4>>,
            ) -> Result<Option<()>, Report> {
                let msgs =
                    dpdk.try_recv_burst(Some((local_port, Some(remote_addr))), new_conns, Some(1))?;
                if let Some(msg) = msgs.iter_mut().map_while(Option::take).next() {
                    ensure!(msg.port == local_port, "Port mismatch");
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }

            DPDK_STATE
                .try_with(|dpdk_cell| {
                    let mut dpdk_opt = dpdk_cell.borrow_mut();
                    let dpdk = dpdk_opt
                        .as_mut()
                        .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                    send_fin(dpdk, local_port, a)
                })
                .map_err(Into::into)
                .and_then(|x| x)
                .wrap_err("sending fin")?;
            debug!("waiting for fin ack");
            let start = Instant::now();
            // this loop has a recv in it.
            loop {
                tokio::task::yield_now().await;
                let res = DPDK_STATE
                    .try_with(|dpdk_cell| {
                        let mut dpdk_opt = dpdk_cell.borrow_mut();
                        let dpdk = dpdk_opt
                            .as_mut()
                            .ok_or(eyre!("dpdk not initialized on core {:?}", this_lcore))?;
                        match recv_one(dpdk, local_port, remote_addr, new_conns.as_ref()) {
                            Ok(Some(_)) => Ok(Some(())),
                            Ok(None) if start.elapsed() >= Duration::from_millis(1) => {
                                send_fin(dpdk, local_port, a).wrap_err("sending fin")?;
                                Ok(None)
                            }
                            Ok(None) => Ok(None),
                            Err(err) => {
                                warn!(?err, "Error trying to receive fin ack");
                                return Err(err).wrap_err("receiving fin ack");
                            }
                        }
                    })
                    .map_err(Into::into)
                    .and_then(|x| x)
                    .wrap_err("fin-ack wait loop")?;
                if let Some(_) = res {
                    break;
                }
            }

            info!(elapsed = ?start.elapsed(), ?a, "exiting");
            Ok::<_, Report>(())
        }
        .instrument(info_span!("client_conn", ?remote_addr))
        .map_err(move |err| {
            warn!(?err, ?remote_addr, "client conn errored");
            err
        })
    })
    .await
}
