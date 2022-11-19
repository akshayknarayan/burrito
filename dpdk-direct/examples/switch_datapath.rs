use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, Report, WrapErr};
use dpdk_direct::{DatapathCn, DpdkDatapath, DpdkDatapathChoice, DpdkReqDatapath};
use futures_util::Stream;
use futures_util::{future::Either, TryStreamExt};
use quanta::Instant;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, debug_span, error, info, info_span, instrument, trace};
use tracing_futures::Instrument;

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
    datapath_cfg: std::path::PathBuf,

    #[structopt(short, long)]
    init_dp_choice: DpdkDatapathChoice,
}

fn main() -> Result<(), Report> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();
    let mut opt = Opt::from_args();

    match &mut opt.init_dp_choice {
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

        let ch = DpdkDatapath::new(opt.datapath_cfg, opt.init_dp_choice)?;
        let ch = Arc::new(Mutex::new(ch));

        let mut jhs = Vec::with_capacity(opt.threads);
        for thread in 1..opt.threads {
            let ch = Arc::clone(&ch);
            let jh = std::thread::spawn(move || {
                let thread_span = debug_span!("thread", ?thread);
                let _thread_span_g = thread_span.enter();

                debug!("spawning thread");
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async move {
                    let cn = {
                        let mut ch_g = ch.lock().unwrap();
                        ch_g.connect(remote_addr).await?
                    };

                    client(cn, opt.num_msgs / opt.threads, interarrival, remote_addr).await
                })?;
                Ok::<_, Report>(thread)
            });
            jhs.push(jh);
        }

        let thread_span = debug_span!("thread", thread = 0);
        let thread_span_g = thread_span.enter();
        debug!("using main tokio thread");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async move {
            let cn = {
                let mut ch_g = ch.lock().unwrap();
                ch_g.connect(remote_addr).await?
            };

            client(cn, opt.num_msgs / opt.threads, interarrival, remote_addr).await
        })?;
        std::mem::drop(thread_span_g);

        let done_span = debug_span!("wait_done");
        info!(thread = 0, "client done");
        let _done_span_g = done_span.enter();
        for jh in jhs {
            match jh.join() {
                Ok(r) => {
                    let thread = r?;
                    info!(?thread, "client done");
                }
                Err(e) => std::panic::resume_unwind(e),
            }
        }

        Ok(())
    } else {
        let local_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, opt.port));
        info!(?local_addr, "Dpdk Inline Chunnel Test - Server");
        let ch = DpdkDatapath::new(opt.datapath_cfg, opt.init_dp_choice)?;
        let ch = DpdkReqDatapath::from(ch);
        let ch = Arc::new(Mutex::new(ch));

        for thread in 1..opt.threads {
            debug!(?thread, "spawning thread");
            let ch = Arc::clone(&ch);
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(async move {
                    let cn_stream_fut = {
                        let mut ch_g = ch.lock().unwrap();
                        ch_g.listen(local_addr)
                    };

                    debug!(?thread, "awaiting stream future");
                    let stream = match cn_stream_fut.await {
                        Err(err) => {
                            error!(?err, "failed to get connection stream");
                            return Err(err);
                        }
                        Ok(s) => s,
                    };

                    server(stream, thread).await
                })
            });
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async move {
            debug!(thread = 0, "using main tokio thread");
            let stream_fut = {
                let mut ch_g = ch.lock().unwrap();
                ch_g.listen(local_addr)
            };

            let stream = stream_fut
                .await
                .wrap_err("failed to get connection stream")?;
            server(stream, 0).await
        })
    }
}

#[instrument(skip(stream), level = "info", err)]
async fn server(
    stream: impl Stream<Item = Result<DatapathCn, Report>> + Send + 'static,
    thread_idx: usize,
) -> Result<(), Report> {
    info!("start listening");
    stream
        .try_for_each_concurrent(None, |cn| {
            let port = cn.local_port();
            let peer = cn.remote_addr();
            async move {
                let mut slots: Vec<_> = (0..16).map(|_| None).collect();
                info!("new connection");
                let mut recv_count = 0;
                loop {
                    let msgs = cn.recv(&mut slots[..]).await?;
                    let echoes = msgs.iter_mut().map_while(|m| {
                        let x = m.take()?;
                        recv_count += 1;
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

#[instrument(skip(num_msgs, interarrival, remote_addr), level = "info", err)]
async fn client(
    cn: DatapathCn,
    num_msgs: usize,
    interarrival: Duration,
    remote_addr: SocketAddr,
) -> Result<(), Report> {
    let mut ticker = AsyncSpinTimer::new(interarrival);
    let mut slots: Vec<_> = (0..16).map(|_| None).collect();
    let mut msgs = (0..num_msgs).map(|i| (remote_addr, vec![(i % u8::MAX as usize) as u8; 32]));

    let mut tot_msg_count = 0;
    let mut tot_recv_count = 0;
    let mut batch = Vec::with_capacity(16);

    info!("starting");

    fn handle_received(
        remote_addr: SocketAddr,
        ms: &mut [Option<(SocketAddr, Vec<u8>)>],
    ) -> Result<usize, Report> {
        ms.iter_mut()
            .map_while(Option::take)
            .try_fold(0, |cnt, msg| {
                trace!(?msg, "received");
                ensure!(
                    msg.0 == remote_addr,
                    "received response from unexpected address"
                );
                Ok(cnt + 1)
            })
    }

    loop {
        let t = ticker.wait();
        tokio::pin!(t);
        match futures_util::future::select(t, cn.recv(&mut slots[..])).await {
            Either::Left((mut num_msgs, _)) => {
                while num_msgs > 0 {
                    if let Some(msg) = msgs.next() {
                        batch.push(msg);
                        num_msgs -= 1;
                        tot_msg_count += 1;
                    } else {
                        break;
                    }
                }

                debug!(?tot_msg_count, "sending");
                cn.send(batch.drain(..)).await?;
                if num_msgs > 0 {
                    info!("done sending");
                    break;
                }
            }
            Either::Right((ms, _)) => {
                tot_recv_count += handle_received(remote_addr, ms?)?;
                debug!(?tot_recv_count, "received burst");
            }
        }
    }

    while tot_recv_count < num_msgs {
        let ms = cn.recv(&mut slots[..]).await?;
        tot_recv_count += handle_received(remote_addr, ms)?;
        debug!(?tot_recv_count, "received burst");
    }

    info!("done");
    Ok(())
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
                return num_ticks;
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

            let elapsed = self.clk.now() - self.last_return.unwrap();
            if elapsed > self.interarrival {
                self.deficit += elapsed - self.interarrival;
            }
        }
    }
}
