use super::Mode;
use color_eyre::eyre::{Report, WrapErr};
use shenango::udp;
use shenango_bertha::ChunnelConnection;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};

pub fn shenangort_bertha(cfg: std::path::PathBuf, port: u16, mode: Mode) {
    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let packet_size = cl.packet_size;
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
                    let cn = shenango_bertha::negotiate_client(
                        stack,
                        UdpChunnelConnection::new(cn),
                        addr,
                    )
                    .wrap_err("negotiation failed")?;
                    let cn = Arc::new(cn);

                    wg.done();
                    wg.wait();

                    info!(?addr, ?download_size, ?i, "starting_client");
                    let mut tot_bytes = 0;

                    // 2. get bytes
                    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
                    req.extend((download_size as u64).to_le_bytes());
                    req.extend((cl.num_clients as u32).to_le_bytes());
                    req.extend((packet_size as u32).to_le_bytes());
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

                    let latest_elapsed =
                        Arc::new(AtomicUsize::new((Instant::now() - start).as_micros() as _));
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
            super::write_results(
                of,
                tot_bytes,
                elapsed,
                num_clients,
                download_size,
                packet_size,
            )
            .unwrap();

            std::process::exit(0);
        })
        .unwrap();
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
                    let pkt_size = u32::from_le_bytes(msg[20..24].try_into().unwrap()) as u64;

                    if num_clients == 0 || pkt_size < 64 || pkt_size > 1460 {
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
                    info!(?remaining, ?pkt_size, ?a, "starting send");
                    while remaining > 0 {
                        let this_send_size = std::cmp::min(pkt_size, remaining);
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