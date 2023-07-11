use color_eyre::eyre::{ensure, Report};
use dpdk_direct::switcher::DpdkDatapathChoice;
use quanta::Instant;
use std::net::SocketAddr;
use std::time::Duration;
use structopt::StructOpt;
use tracing::trace;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "Switch datapaths microbenchmark")]
pub struct Opt {
    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long, default_value = "100")]
    num_msgs: usize,

    #[structopt(short, long)]
    threads: usize,

    #[structopt(short, long)]
    config: std::path::PathBuf,

    #[structopt(long)]
    datapath_choice: DpdkDatapathChoice,

    #[structopt(long)]
    use_locks: bool,

    #[structopt(long)]
    swap_delay_msgs: usize,

    #[structopt(long)]
    swap_to: DpdkDatapathChoice,

    #[structopt(long)]
    closed_loop: bool,

    /// If specified, this is a client. Otherwise it is a server.
    #[structopt(long)]
    ip_addr: Option<std::net::Ipv4Addr>,

    #[structopt(long, default_value = "1000")]
    interarrival_us: u64,

    #[structopt(long, short)]
    out_file: Option<std::path::PathBuf>,
}

fn main() -> Result<(), Report> {
    color_eyre::install()?;
    let subscriber = tracing_subscriber::registry();
    let subscriber = subscriber
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_error::ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    let mut opt = Opt::from_args();

    match &mut opt.datapath_choice {
        DpdkDatapathChoice::Inline {
            ref mut num_threads,
        } if *num_threads == 0 => {
            *num_threads = opt.threads;
        }
        _ => (),
    }

    match &mut opt.swap_to {
        DpdkDatapathChoice::Inline {
            ref mut num_threads,
        } if *num_threads == 0 => {
            *num_threads = opt.threads;
        }
        _ => (),
    }

    if opt.use_locks {
        lock::run(opt)
    } else {
        lockfree::run(opt)
    }
}

fn handle_received(
    remote_addr: SocketAddr,
    ms: &mut [Option<(SocketAddr, Vec<u8>)>],
    rs: &mut [(Duration, Option<(Duration, Duration)>)],
    start: quanta::Instant,
    clk: &quanta::Clock,
) -> Result<usize, Report> {
    let now = clk.now().duration_since(start);
    ms.iter_mut()
        .map_while(Option::take)
        .try_fold(0, |cnt, msg| {
            ensure!(
                msg.0 == remote_addr,
                "received response from unexpected address"
            );

            let msg_seq = u32::from_le_bytes(msg.1[0..4].try_into().unwrap()) as usize;
            ensure!(
                msg_seq < rs.len(),
                "received out-of-bounds seq number echo {}",
                msg_seq,
            );

            ensure!(
                rs[msg_seq].1.is_none(),
                "received duplicate echo packet {}",
                msg_seq
            );

            let elapsed = now - rs[msg_seq].0;
            trace!(?msg_seq, ?elapsed, "received");
            rs[msg_seq].1 = Some((now, elapsed));
            Ok(cnt + 1)
        })
}

macro_rules! exp_impl {
    ($datapathCn: ty, $dpdkDatapath: ty, $dpdkReqDatapath: ty) => {
use crate::Opt;
use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, Report, WrapErr};
use dpdk_direct::switcher::DpdkDatapathChoice;
use futures_util::Stream;
use futures_util::{future::Either, TryStreamExt};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tracing::{debug, debug_span, error, info, info_span, instrument, trace};
use tracing_futures::Instrument;

pub fn run(opt: Opt) -> Result<(), Report> {
    if let Some(addr) = opt.ip_addr {
        let remote_addr = SocketAddr::V4(SocketAddrV4::new(addr, opt.port));
        let interarrival = std::time::Duration::from_micros(opt.interarrival_us);
        info!(
            ?remote_addr,
            ?interarrival,
            "Dpdk Inline Chunnel Test - Client"
        );

        let mut ch = <$dpdkDatapath>::new(opt.config, opt.datapath_choice)?;

        let mut jhs = Vec::with_capacity(opt.threads);
        for thread in 1..opt.threads {
            let mut ch = ch.clone();
            let jh = std::thread::spawn(move || {
                let thread_span = debug_span!("thread", ?thread);
                let _thread_span_g = thread_span.enter();

                debug!("spawning thread");
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let res = rt.block_on(async move {
                    let cn = ch.connect(remote_addr).await?;
                    if opt.closed_loop {
                        closed_loop_client(cn, opt.num_msgs / opt.threads, remote_addr, None).await
                    } else {
                        open_loop_client(
                            cn,
                            opt.num_msgs / opt.threads,
                            interarrival,
                            remote_addr,
                            None,
                        )
                        .await
                    }
                })?;
                Ok::<_, Report>((thread, res))
            });
            jhs.push(jh);
        }

        let thread_span = debug_span!("thread", thread = 0);
        let thread_span_g = thread_span.enter();
        debug!("using main tokio thread");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let res = rt.block_on(async move {
            let cn = ch.connect(remote_addr).await?;
            if opt.closed_loop {
                closed_loop_client(
                    cn,
                    opt.num_msgs / opt.threads,
                    remote_addr,
                    Some(ClientSwapSpec {
                        swap_after_msgs_num: opt.swap_delay_msgs,
                        swap_to: opt.swap_to,
                        ch,
                    }),
                )
                .await
            } else {
                open_loop_client(
                    cn,
                    opt.num_msgs / opt.threads,
                    interarrival,
                    remote_addr,
                    Some(ClientSwapSpec {
                        swap_after_msgs_num: opt.swap_delay_msgs,
                        swap_to: opt.swap_to,
                        ch,
                    }),
                )
                .await
            }
        })?;
        std::mem::drop(thread_span_g);

        let mut all = Vec::with_capacity(opt.num_msgs);
        let t0_tot = res.len();
        let t0_done = res.iter().filter(|x| x.1.is_some()).count();
        all.extend(res.into_iter());
        let done_span = debug_span!("wait_done");
        info!(thread = 0, completed = ?t0_done, dropped = ?(t0_tot - t0_done), "client done");
        let _done_span_g = done_span.enter();
        for jh in jhs {
            match jh.join() {
                Ok(r) => {
                    let (thread, res) = r?;
                    let t_tot = res.len();
                    let t_done = res.iter().filter(|x| x.1.is_some()).count();
                    all.extend(res.into_iter());
                    info!(?thread, completed = ?t_done, dropped = ?(t_tot - t_done), "client done");
                }
                Err(e) => std::panic::resume_unwind(e),
            }
        }

        if let Some(ofp) = opt.out_file {
            all.sort_by_key(|k| k.0);
            use std::io::prelude::*;
            let mut f = std::fs::File::create(&ofp)?;
            writeln!(&mut f, "send_time_us maybe_recv_time maybe_rtt_us")?;
            for (send, rcv) in all {
                let (recv_time, rtt) = rcv
                    .map(|(rcv_time, rtt)| (rcv_time.as_micros(), rtt.as_micros()))
                    .unwrap_or((0, 0));
                writeln!(&mut f, "{} {} {}", send.as_micros(), recv_time, rtt,)?;
            }
        }

        Ok(())
    } else {
        let local_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, opt.port));
        info!(?local_addr, "Dpdk Inline Chunnel Test - Server");
        let ch = <$dpdkDatapath>::new(opt.config, opt.datapath_choice)?;
        let mut ch = <$dpdkReqDatapath>::from(ch);
        let swap_spec = ServerSwapSpec {
            swap_after_msgs_num: opt.swap_delay_msgs,
            swap_to: opt.swap_to,
            ch: Arc::new(Mutex::new(ch.clone())),
            did_swap: Default::default(),
        };

        for thread in 1..opt.threads {
            debug!(?thread, "spawning thread");
            let mut ch = ch.clone();
            let swap_spec = swap_spec.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async move {
                    debug!(?thread, "awaiting stream future");
                    let stream = match ch.listen(local_addr).await {
                        Err(err) => {
                            error!(?err, "failed to get connection stream");
                            return Err(err);
                        }
                        Ok(s) => s,
                    };

                    server(stream, thread, Some(swap_spec)).await
                })
            });
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async move {
            debug!(thread = 0, "using main tokio thread");
            let stream = ch
                .listen(local_addr)
                .await
                .wrap_err("failed to get connection stream")?;
            server(stream, 0, Some(swap_spec)).await
        })
    }
}

#[derive(Debug, Clone)]
pub struct ServerSwapSpec {
    swap_after_msgs_num: usize,
    swap_to: DpdkDatapathChoice,
    ch: Arc<Mutex<$dpdkReqDatapath>>,
    did_swap: Arc<AtomicBool>,
}

#[instrument(skip(stream, swap_dp), level = "info", err)]
pub async fn server(
    stream: impl Stream<Item = Result<$datapathCn, Report>> + Send + 'static,
    thread_idx: usize,
    swap_dp: Option<ServerSwapSpec>,
) -> Result<(), Report> {
    let tot_recv_count: Arc<AtomicUsize> = Default::default();
    info!("start listening");
    stream
        .try_for_each_concurrent(None, move |cn| {
            let port = cn.local_port();
            let peer = cn.remote_addr();
            let tot_recv_count = tot_recv_count.clone();
            let swap_dp = swap_dp.clone();
            async move {
                let mut slots: Vec<_> = (0..16).map(|_| None).collect();
                info!("new connection");
                loop {
                    if let Some(ServerSwapSpec {
                        swap_after_msgs_num,
                        swap_to,
                        ch,
                        did_swap,
                    }) = &swap_dp
                    {
                        let cnt = tot_recv_count.load(Ordering::Relaxed);
                        if !did_swap.load(Ordering::Relaxed) && cnt > *swap_after_msgs_num {
                            info!(?swap_to, ?cnt, "swapping datapath now");
                            did_swap.store(true, Ordering::SeqCst);
                            if let Err(err) = ch.lock().unwrap().trigger_transition(*swap_to).await {
                                error!(?err, "trigger_transition failed");
                                return Err(err);
                            }
                        }
                    }

                    let msgs = cn.recv(&mut slots[..]).await?;
                    let mut recv_count = 0;
                    let echoes = msgs.iter_mut().map_while(|m| {
                        let x = m.take()?;
                        recv_count += 1;
                        trace!(?x, ?recv_count, "got msg");
                        Some(x)
                    });
                    cn.send(echoes).await?;
                    tot_recv_count.fetch_add(recv_count, Ordering::SeqCst);
                    debug!(?tot_recv_count, ?recv_count, "received message burst");
                }
            }
            .instrument(info_span!("connection", ?port, ?peer))
        })
    .await
}

#[derive(Debug)]
pub struct ClientSwapSpec {
    swap_after_msgs_num: usize,
    swap_to: DpdkDatapathChoice,
    ch: $dpdkDatapath,
}

#[instrument(skip(num_msgs, remote_addr), level = "info", err)]
pub async fn closed_loop_client(
    cn: $datapathCn,
    num_msgs: usize,
    remote_addr: SocketAddr,
    mut swap_dp: Option<ClientSwapSpec>,
) -> Result<Vec<(Duration, Option<(Duration, Duration)>)>, Report> {
    let mut slots: Vec<_> = (0..16).map(|_| None).collect();
    let mut msgs = (0..num_msgs).map(|i| {
        let seq_bytes: [u8; 4] = (i as u32).to_le_bytes();
        let mut payload = vec![0; 32];
        payload[0..4].copy_from_slice(&seq_bytes[..]);
        (i, (remote_addr, payload))
    });

    let mut snd_msg_count = 0;
    let mut batch = Vec::with_capacity(16);
    let mut recvs: Vec<(Duration, Option<(Duration, Duration)>)> = Vec::with_capacity(num_msgs);
    let mut recv_cnt = 0usize;
    let clk = quanta::Clock::new();

    info!("starting");
    let mut swapped = false;
    let start = clk.now();

    let mut inflight_seqs = [0usize; 8];

    while snd_msg_count < num_msgs {
        if let Some(ClientSwapSpec {
            swap_after_msgs_num,
            swap_to,
            ch,
        }) = &mut swap_dp
        {
            if !swapped && snd_msg_count >= *swap_after_msgs_num {
                // called *non-concurrently* with send/recv.
                info!(?swap_to, "swapping datapath now");
                ch.trigger_transition(*swap_to).await?;
                swapped = true;
            }
        }

        let send_time = clk.now().duration_since(start).as_micros() as u64;
        for i in 0..8 {
            if let Some((seq, msg)) = msgs.next() {
                ensure!(recvs.len() == seq, "Recv tracking buffer out of sync");
                inflight_seqs[i] = seq;
                recvs.push((Duration::from_micros(send_time), None));
                batch.push(msg);
                snd_msg_count += 1;
            } else {
                break;
            }
        }

        cn.send(batch.drain(..)).await?;

        let ms = match tokio::time::timeout(
            std::time::Duration::from_millis(500),
            cn.recv(&mut slots[..]),
        )
            .await
            {
                Ok(ms) => ms?,
                Err(_) => {
                    debug!("timed out burst");
                    for seq in inflight_seqs {
                        recvs[seq].1 = Some((Duration::from_secs(0), Duration::from_secs(0)));
                    }

                    continue;
            }
            };
        let cnt = super::handle_received(remote_addr, ms, &mut recvs[..], start, &clk)?;
        recv_cnt += cnt;
        debug!(?cnt, ?recv_cnt, "received burst");
    }

    debug!("last recv");

    match tokio::time::timeout(std::time::Duration::from_secs(1), cn.recv(&mut slots[..])).await {
        Ok(ms) => {
            let cnt = super::handle_received(remote_addr, ms?, &mut recvs[..], start, &clk)?;
            recv_cnt += cnt;
            debug!(?cnt, ?recv_cnt, "received burst");
        }
        Err(_) => (),
    };

    info!(?recv_cnt, "done");
    Ok(recvs)
}

#[instrument(skip(num_msgs, interarrival, remote_addr), level = "info", err)]
pub async fn open_loop_client(
    cn: $datapathCn,
    num_msgs: usize,
    interarrival: Duration,
    remote_addr: SocketAddr,
    mut swap_dp: Option<ClientSwapSpec>,
) -> Result<Vec<(Duration, Option<(Duration, Duration)>)>, Report> {
    let mut ticker = super::AsyncSpinTimer::new(interarrival);
    let mut slots: Vec<_> = (0..16).map(|_| None).collect();
    let mut msgs = (0..num_msgs).map(|i| {
        let seq_bytes: [u8; 4] = (i as u32).to_le_bytes();
        let mut payload = vec![0; 32];
        payload[0..4].copy_from_slice(&seq_bytes[..]);
        (i, (remote_addr, payload))
    });

    let mut snd_msg_count = 0;
    let mut batch = Vec::with_capacity(16);
    let mut recvs: Vec<(Duration, Option<(Duration, Duration)>)> = Vec::with_capacity(num_msgs);
    let mut recv_cnt = 0usize;
    let clk = quanta::Clock::new();

    info!("starting");
    let mut swapped = false;
    let start = clk.now();
    loop {
        if let Some(ClientSwapSpec {
            swap_after_msgs_num,
            swap_to,
            ch,
        }) = &mut swap_dp
        {
            if !swapped && snd_msg_count >= *swap_after_msgs_num {
                // called *non-concurrently* with send/recv.
                info!(?swap_to, "swapping datapath now");
                ch.trigger_transition(*swap_to).await?;
                swapped = true;
            }
        }

        let t = ticker.wait();
        tokio::pin!(t);
        match futures_util::future::select(t, cn.recv(&mut slots[..])).await {
            Either::Left((mut num_msgs, _)) => {
                let send_time = clk.now().duration_since(start).as_micros() as u64;
                while num_msgs > 0 {
                    if let Some((seq, msg)) = msgs.next() {
                        ensure!(recvs.len() == seq, "Recv tracking buffer out of sync");
                        recvs.push((Duration::from_micros(send_time), None));
                        batch.push(msg);
                        num_msgs -= 1;
                        snd_msg_count += 1;
                    } else {
                        break;
                    }
                }

                debug!(?snd_msg_count, batch_sz = ?batch.len(), "sending");
                cn.send(batch.drain(..)).await?;
                if num_msgs > 0 {
                    info!("done sending");
                    break;
                }
            }
            Either::Right((ms, _)) => {
                let cnt = crate::handle_received(remote_addr, ms?, &mut recvs[..], start, &clk)?;
                recv_cnt += cnt;
                debug!(?cnt, ?recv_cnt, "received burst");
            }
        }
    }

    while recv_cnt < num_msgs {
        let ms =
            match tokio::time::timeout(std::time::Duration::from_secs(3), cn.recv(&mut slots[..]))
            .await
            {
                Ok(ms) => ms?,
                Err(_) => {
                    break;
                }
            };
        let cnt = crate::handle_received(remote_addr, ms, &mut recvs[..], start, &clk)?;
        recv_cnt += cnt;
        debug!(?cnt, ?recv_cnt, "received burst");
    }

    info!(?recv_cnt, "done");
    Ok(recvs)
}
    }
}

mod lock {
    exp_impl!(
        dpdk_direct::switcher_lock::DatapathCn,
        dpdk_direct::switcher_lock::DpdkDatapath,
        dpdk_direct::switcher_lock::DpdkReqDatapath
    );
}

mod lockfree {
    exp_impl!(
        dpdk_direct::switcher_lockfree::DatapathCn,
        dpdk_direct::switcher_lockfree::DpdkDatapath,
        dpdk_direct::switcher_lockfree::DpdkReqDatapath
    );
}

struct AsyncSpinTimer {
    clk: quanta::Clock,
    interarrival: Duration,
    deficit: Duration,
    last_return: Option<Instant>,
}

impl AsyncSpinTimer {
    pub fn new(interarrival: Duration) -> Self {
        AsyncSpinTimer {
            clk: quanta::Clock::new(),
            interarrival,
            deficit: Duration::from_micros(0),
            last_return: None,
        }
    }

    pub async fn wait(&mut self) -> usize {
        let mut num_ticks = 0;
        loop {
            while self.deficit > self.interarrival {
                self.deficit -= self.interarrival;
                num_ticks += 1;
            }

            if num_ticks > 0 {
                self.last_return = Some(self.clk.now());
                return std::cmp::min(num_ticks, 32);
            }

            if self.last_return.is_none() {
                self.last_return = Some(self.clk.now());
            }

            let target = self.last_return.unwrap() + self.interarrival;
            loop {
                let now = self.clk.now();
                if now >= target {
                    break;
                }

                if target - now > Duration::from_micros(10) {
                    tokio::time::sleep(Duration::from_micros(5)).await;
                } else {
                    tokio::task::yield_now().await
                }
            }

            self.deficit += self.clk.now() - self.last_return.unwrap();
        }
    }
}
