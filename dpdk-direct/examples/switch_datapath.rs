use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, Report, WrapErr};
use dpdk_direct::{DatapathCn, DpdkDatapath, DpdkDatapathChoice, DpdkReqDatapath};
use futures_util::Stream;
use futures_util::{future::Either, TryStreamExt};
use quanta::Instant;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, debug_span, error, info, info_span, instrument, trace};
use tracing_futures::Instrument;
use tracing_subscriber::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "Switch datapaths test")]
struct Opt {
    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long, default_value = "100")]
    num_msgs: usize,

    #[structopt(short, long)]
    threads: usize,

    /// If specified, this is a client. Otherwise it is a server.
    #[structopt(short, long)]
    ip_addr: Option<std::net::Ipv4Addr>,

    #[structopt(short, long)]
    config: std::path::PathBuf,

    #[structopt(long)]
    datapath_choice: DpdkDatapathChoice,

    #[structopt(long)]
    swap_delay_msgs: usize,

    #[structopt(long)]
    swap_to: DpdkDatapathChoice,
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

    if let Some(addr) = opt.ip_addr {
        let remote_addr = SocketAddr::V4(SocketAddrV4::new(addr, opt.port));
        let interarrival = std::time::Duration::from_micros(100);
        info!(
            ?remote_addr,
            ?interarrival,
            "Dpdk Inline Chunnel Test - Client"
        );

        let mut ch = DpdkDatapath::new(opt.config, opt.datapath_choice)?;

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
                    client(
                        cn,
                        opt.num_msgs / opt.threads,
                        interarrival,
                        remote_addr,
                        None,
                    )
                    .await
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
            client(
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
        })?;
        std::mem::drop(thread_span_g);

        let t0_tot = res.len();
        let t0_done = res.into_iter().filter(Option::is_some).count();
        let done_span = debug_span!("wait_done");
        info!(thread = 0, completed = ?t0_done, dropped = ?(t0_tot - t0_done), "client done");
        let _done_span_g = done_span.enter();
        for jh in jhs {
            match jh.join() {
                Ok(r) => {
                    let (thread, res) = r?;
                    let t_tot = res.len();
                    let t_done = res.into_iter().filter(Option::is_some).count();
                    info!(?thread, completed = ?t_done, dropped = ?(t_tot - t_done), "client done");
                }
                Err(e) => std::panic::resume_unwind(e),
            }
        }

        Ok(())
    } else {
        let local_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, opt.port));
        info!(?local_addr, "Dpdk Inline Chunnel Test - Server");
        let ch = DpdkDatapath::new(opt.config, opt.datapath_choice)?;
        let mut ch = DpdkReqDatapath::from(ch);

        for thread in 1..opt.threads {
            debug!(?thread, "spawning thread");
            let mut ch = ch.clone();
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

                    server(stream, thread, None).await
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
            server(
                stream,
                0,
                Some(ServerSwapSpec {
                    swap_after_msgs_num: opt.swap_delay_msgs,
                    swap_to: opt.swap_to,
                    ch: Arc::new(Mutex::new(ch)),
                }),
            )
            .await
        })
    }
}

#[derive(Debug, Clone)]
struct ServerSwapSpec {
    swap_after_msgs_num: usize,
    swap_to: DpdkDatapathChoice,
    ch: Arc<Mutex<DpdkReqDatapath>>,
}

#[instrument(skip(stream), level = "info", err)]
async fn server(
    stream: impl Stream<Item = Result<DatapathCn, Report>> + Send + 'static,
    thread_idx: usize,
    swap_dp: Option<ServerSwapSpec>,
) -> Result<(), Report> {
    let tot_recv_count: Arc<AtomicUsize> = Default::default();
    let did_swap: Arc<AtomicBool> = Default::default();
    info!("start listening");
    stream
        .try_for_each_concurrent(None, move |cn| {
            let port = cn.local_port();
            let peer = cn.remote_addr();
            let tot_recv_count = tot_recv_count.clone();
            let swap_dp = swap_dp.clone();
            let did_swap = did_swap.clone();
            async move {
                let mut slots: Vec<_> = (0..16).map(|_| None).collect();
                info!("new connection");
                let mut recv_count = 0;
                loop {
                    if let Some(ServerSwapSpec {
                        swap_after_msgs_num,
                        swap_to,
                        ref ch,
                    }) = swap_dp
                    {
                        let cnt = tot_recv_count.load(Ordering::Relaxed);
                        if !did_swap.load(Ordering::Relaxed) && cnt > swap_after_msgs_num {
                            info!(?swap_to, ?cnt, "swapping datapath now");
                            did_swap.store(true, Ordering::SeqCst);
                            ch.lock().unwrap().trigger_transition(swap_to)?;
                        }
                    }

                    let msgs = cn.recv(&mut slots[..]).await?;
                    let echoes = msgs.iter_mut().map_while(|m| {
                        let x = m.take()?;
                        recv_count += 1;
                        tot_recv_count.fetch_add(1, Ordering::SeqCst);
                        trace!(?x, ?recv_count, "got msg");
                        Some(x)
                    });
                    cn.send(echoes).await?;
                    debug!(?recv_count, "received message burst");
                }
            }
            .instrument(info_span!("connection", ?port, ?peer))
        })
        .await
}

#[derive(Debug)]
struct ClientSwapSpec {
    swap_after_msgs_num: usize,
    swap_to: DpdkDatapathChoice,
    ch: DpdkDatapath,
}

#[instrument(skip(num_msgs, interarrival, remote_addr), level = "info", err)]
async fn client(
    cn: DatapathCn,
    num_msgs: usize,
    interarrival: Duration,
    remote_addr: SocketAddr,
    mut swap_dp: Option<ClientSwapSpec>,
) -> Result<Vec<Option<Duration>>, Report> {
    let mut ticker = AsyncSpinTimer::new(interarrival);
    let mut slots: Vec<_> = (0..16).map(|_| None).collect();
    let mut msgs = (0..num_msgs).map(|i| {
        let seq_bytes: [u8; 4] = (i as u32).to_le_bytes();
        let mut payload = vec![0; 32];
        payload[0..4].copy_from_slice(&seq_bytes[..]);
        (remote_addr, payload)
    });

    let mut snd_msg_count = 0;
    let mut batch = Vec::with_capacity(16);
    let mut recvs: Vec<Option<Duration>> = (0..num_msgs).map(|_| None).collect();
    let mut recv_cnt = 0usize;
    let clk = quanta::Clock::new();

    fn handle_received(
        remote_addr: SocketAddr,
        ms: &mut [Option<(SocketAddr, Vec<u8>)>],
        rs: &mut [Option<Duration>],
        start: quanta::Instant,
        clk: &quanta::Clock,
    ) -> Result<usize, Report> {
        let elapsed = clk.now().duration_since(start);
        // return cumulative ack and number of drops.
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
                    rs[msg_seq].is_none(),
                    "received duplicate echo packet {}",
                    msg_seq
                );

                trace!(?msg_seq, "received");
                rs[msg_seq] = Some(elapsed);
                Ok(cnt + 1)
            })
    }

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
                ch.trigger_transition(*swap_to)?;
                swapped = true;
            }
        }

        let t = ticker.wait();
        tokio::pin!(t);
        match futures_util::future::select(t, cn.recv(&mut slots[..])).await {
            Either::Left((mut num_msgs, _)) => {
                while num_msgs > 0 {
                    if let Some(msg) = msgs.next() {
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
                let cnt = handle_received(remote_addr, ms?, &mut recvs[..], start, &clk)?;
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
        let cnt = handle_received(remote_addr, ms, &mut recvs[..], start, &clk)?;
        recv_cnt += cnt;
        debug!(?cnt, ?recv_cnt, "received burst");
    }

    info!(?recv_cnt, "done");
    Ok(recvs)
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
