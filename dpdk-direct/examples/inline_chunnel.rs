use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, Report};
use dpdk_direct::{DpdkInlineChunnel, DpdkInlineReqChunnel};
use futures_util::TryStreamExt;
use quanta::Instant;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, info, trace};

#[derive(Debug, StructOpt)]
#[structopt(name = "Inline Chunnel Test")]
struct Opt {
    #[structopt(short, long)]
    port: u16,

    #[structopt(short, long, default_value = "100")]
    num_msgs: usize,

    /// If specified, this is a client. Otherwise it is a server.
    #[structopt(short, long)]
    ip_addr: Option<std::net::Ipv4Addr>,

    #[structopt(short, long)]
    datapath_cfg: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();

    if let Some(addr) = opt.ip_addr {
        let remote_addr = SocketAddr::V4(SocketAddrV4::new(addr, opt.port));
        info!(?remote_addr, "Dpdk Inline Chunnel Test - Client");

        let mut ch = DpdkInlineChunnel::new(opt.datapath_cfg, 1)?;
        let cn = ch.connect(()).await?;

        let interarrival = std::time::Duration::from_micros(100);
        let mut ticker = AsyncSpinTimer::new(interarrival);
        let mut slots: Vec<_> = (0..16).map(|_| None).collect();
        let mut msgs =
            (0..opt.num_msgs).map(|i| (remote_addr, vec![(i % u8::MAX as usize) as u8; 32]));

        use futures_util::future::Either;
        let mut tot_msg_count = 0;
        let mut tot_recv_count = 0;
        let mut batch = Vec::with_capacity(16);

        fn handle_received(
            remote_addr: SocketAddr,
            ms: &mut [Option<(SocketAddr, Vec<u8>)>],
        ) -> Result<usize, Report> {
            ms.iter_mut()
                .map_while(Option::take)
                .try_fold(0, |cnt, msg| {
                    debug!(?msg, "received");
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
                    debug!(?tot_recv_count, "received");
                }
            }
        }

        while tot_recv_count < opt.num_msgs {
            let ms = cn.recv(&mut slots[..]).await?;
            tot_recv_count += handle_received(remote_addr, ms)?;
            debug!(?tot_recv_count, "received messages");
        }

        info!("done");
    } else {
        let local_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, opt.port));
        info!(?local_addr, "Dpdk Inline Chunnel Test - Server");
        let ch = DpdkInlineChunnel::new(opt.datapath_cfg, 1)?;
        let mut ch = DpdkInlineReqChunnel::from(ch);
        let cn_stream = ch.listen(local_addr).await?;

        cn_stream
            .try_for_each_concurrent(None, |cn| async move {
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
            })
            .await?;
    }
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
