use super::Mode;
use color_eyre::eyre::Report;
use shenango::udp;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};

pub fn shenango_nobertha(cfg: std::path::PathBuf, port: u16, mode: Mode) {
    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let packet_size = cl.packet_size;
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
                    let mut tot_pkts = 0;
                    let mut start = Instant::now();

                    // 2. get bytes
                    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
                    req.extend((download_size as u64).to_le_bytes());
                    req.extend((packet_size as u64).to_le_bytes());
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
                            shenango::time::sleep(Duration::from_millis(100));
                        });

                        debug!(elapsed = ?start.elapsed(), ?i, "waiting");
                        if p.wait() == TIME {
                            cnt += 1;
                            debug!(elapsed = ?start.elapsed(), ?i, ?cnt, "retrying req");
                            if cnt > 50 {
                                return Ok((0, 0, start.elapsed()));
                            }

                            start = Instant::now();
                            cn.write_to(&req, addr)?;
                        } else {
                            break;
                        }
                    }

                    debug!(elapsed = ?start.elapsed(), ?i, "connection started");
                    tot_bytes += recv_jh.join().unwrap()?;
                    tot_pkts += 1;

                    let mut buf = [0u8; 2048];
                    let mut last_recv_time = Instant::now();
                    loop {
                        let s = cn.recv(&mut buf[0..2048])?;
                        assert!(s != 0);
                        tot_bytes += s;
                        tot_pkts += 1;
                        trace!(?tot_bytes, ?tot_pkts, ?i, ?s, "received part");
                        if buf[0] == 1 {
                            cn.write_to(&buf[0..16], addr)?;
                            break;
                        } else {
                            last_recv_time = Instant::now();
                        }
                    }

                    let elapsed = last_recv_time - start;
                    info!(?tot_bytes, ?tot_pkts, ?elapsed, "done");
                    Ok((tot_bytes, tot_pkts, elapsed))
                });
                jhs.push(jh);
            }

            let joined: Vec<Result<(usize, usize, Duration), Report>> =
                jhs.into_iter().map(|jh| jh.join().unwrap()).collect();
            let (tot_bytes, tot_pkts, elapsed) = joined
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("client thread errored")
                .into_iter()
                .reduce(|(b, p, d), (bi, pi, di)| (b + bi, p + pi, std::cmp::max(d, di)))
                .expect("There should be at least one client thread");
            info!(?tot_bytes, ?tot_pkts, ?elapsed, "all clients done");

            super::write_results(
                of,
                tot_bytes,
                tot_pkts,
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
                let pkt_size = u64::from_le_bytes(msg[16..24].try_into().unwrap());

                if pkt_size < 64 || pkt_size > 1460 {
                    debug!("bad client request");
                    return;
                }

                let start = Instant::now();
                info!(?remaining, ?a, "starting send");
                while remaining > 0 {
                    let this_send_size = std::cmp::min(pkt_size, remaining);
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
                    let mut buf = [0u8; 1500];
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
