use super::Mode;
use color_eyre::eyre::{bail, Report, WrapErr};
use shenango::udp;
use shenango_bertha::ChunnelConnection;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};

use bertha::CxList;
use shenango_bertha::{Chunnel, Negotiate};

// copied from crate::using_chunnel_connection. We need to re-implement for shenango-bertha.
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
impl<const UNIQ: u64> Negotiate for NoOpChunnel<UNIQ> {}

impl<const UNIQ: u64, D, InC> Chunnel<InC> for NoOpChunnel<UNIQ>
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Read + Send + Sync + 'static,
{
    type Connection = NoOpChunnelCn<InC>;
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, cn: InC) -> Result<Self::Connection, Self::Error> {
        Ok(NoOpChunnelCn(cn))
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

    fn send(&self, data: D) -> Result<(), Report> {
        let arr = data.read();
        if !arr.is_empty() {
            // just read the first byte
            std::hint::black_box(arr[0]);
        }

        self.0.send(data)
    }

    fn recv(&self) -> Result<Self::Data, Report> {
        let data = self.0.recv()?;
        std::hint::black_box(data.read()[0]);
        Ok(data)
    }
}

pub fn shenangort_bertha(cfg: std::path::PathBuf, port: u16, mode: Mode, stack_depth: usize) {
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
                    macro_rules! client_with_stack {
                        ($stack: expr) => {{
                            let mut rng = rand::thread_rng();
                            let cn = udp::UdpConnection::dial(
                                SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
                                addr,
                            )?;

                            // space these out
                            let start_wait: u64 = rng.gen_range(0..100);
                            shenango::time::sleep(Duration::from_millis(start_wait));

                            use shenango_bertha::udp::UdpChunnelConnection;
                            let cn = shenango_bertha::negotiate_client(
                                $stack,
                                UdpChunnelConnection::new(cn),
                                addr,
                            )
                            .wrap_err("negotiation failed")?;
                            let cn = Arc::new(cn);

                            wg.done();
                            wg.wait();

                            one_client(addr, cl.num_clients, download_size, packet_size, i, cn)
                        }};
                    }

                    match stack_depth {
                        0 => client_with_stack!(shenango_bertha::chunnels::Nothing::<()>::default()),
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
                });
                jhs.push(jh);
            }

            wg.wait();
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
            let activated_clients = Arc::new(AtomicUsize::default());
            let wg = shenango::sync::WaitGroup::new();
            macro_rules! server_with_stack {
                ($stack: expr) => {{
                    shenango_bertha::negotiate_server($stack, listen_addr, move |cn| {
                        one_server_thread(activated_clients.clone(), wg.clone(), cn)
                    })
                    .unwrap();
                }};
            }

            match stack_depth {
                0 => server_with_stack!(shenango_bertha::chunnels::Nothing::<()>::default()),
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
                _ => panic!("stack depth {} not supported", stack_depth),
            }
        })
        .unwrap();
    }
}

fn one_client(
    addr: SocketAddrV4,
    num_clients: usize,
    download_size: usize,
    packet_size: usize,
    i: usize,
    cn: Arc<impl ChunnelConnection<Data = (SocketAddrV4, Vec<u8>)> + Send + Sync + 'static>,
) -> Result<(usize, usize, Duration), Report> {
    info!(?addr, ?download_size, ?i, "starting_client");
    let mut tot_bytes = 0;
    let mut tot_pkts = 0;

    // 2. get bytes
    let mut req = vec![1, 2, 3, 4, 5, 6, 7, 8];
    req.extend((download_size as u64).to_le_bytes());
    req.extend((num_clients as u32).to_le_bytes());
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
                return Ok((0, 0, start.elapsed()));
            }

            debug!(elapsed = ?start.elapsed(), ?i, "retrying req");
            start = Instant::now(); // measure from first byte received
            cn.send((addr, req.clone()))?;
        } else {
            break;
        }
    }

    tot_bytes += recv_jh.join().unwrap()?;
    tot_pkts += 1;
    info!(elapsed = ?start.elapsed(), ?i, "connection_started");
    let mut p = shenango::poll::PollWaiter::new();
    let sleep_trigger = p.trigger(TIME);
    shenango::thread::spawn(move || {
        let _sleep_trigger = sleep_trigger;
        shenango::time::sleep(Duration::from_secs(15));
    });

    let latest_elapsed = Arc::new(AtomicUsize::new((Instant::now() - start).as_micros() as _));
    let tot_bytes = Arc::new(AtomicUsize::new(tot_bytes));
    let tot_pkts = Arc::new(AtomicUsize::new(tot_pkts));
    let recv_trigger = p.trigger(RECV);
    let le = Arc::clone(&latest_elapsed);
    let tb = Arc::clone(&tot_bytes);
    let tp = Arc::clone(&tot_pkts);
    shenango::thread::spawn(move || {
        let _recv_trigger = recv_trigger;
        let tot_bytes = tb;
        let tot_pkts = tp;
        let latest_elapsed = le;
        loop {
            let (_, m) = cn.recv()?;
            assert!(!m.is_empty());
            tot_bytes.fetch_add(m.len(), Ordering::Relaxed);
            tot_pkts.fetch_add(1, Ordering::Relaxed);
            trace!(?tot_bytes, ?tot_pkts, ?i, "received part");
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
    let tot_pkts = tot_pkts.load(Ordering::SeqCst);
    let elapsed = Duration::from_micros(latest_elapsed.load(Ordering::SeqCst) as _);
    info!(?tot_bytes, ?tot_pkts, ?elapsed, ?i, "done");
    Ok((tot_bytes, tot_pkts, elapsed))
}

fn one_server_thread(
    activated_clients: Arc<AtomicUsize>,
    wg: shenango::sync::WaitGroup,
    cn: impl ChunnelConnection<Data = (SocketAddrV4, Vec<u8>)> + Send + Sync + 'static,
) {
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

    match activated_clients.compare_exchange(0, num_clients, Ordering::SeqCst, Ordering::SeqCst) {
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
}
