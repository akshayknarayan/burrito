use bertha::{ChunnelConnection, ChunnelConnector, ChunnelListener};
use color_eyre::eyre::{ensure, Report};
use dpdk_direct::{DpdkInlineChunnel, DpdkInlineReqChunnel};
use futures_util::TryStreamExt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use structopt::StructOpt;
use tracing::{debug, info};

#[derive(Debug, StructOpt)]
#[structopt(name = "Inline Chunnel Test")]
struct Opt {
    #[structopt(short, long)]
    port: u16,

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

        let mut slot = [None];
        for i in 0..100 {
            debug!(?i, "sending");
            cn.send(std::iter::once((remote_addr, vec![i; 32]))).await?;
            debug!(?i, "sent");
            let msg = cn.recv(&mut slot[..]).await?;
            debug!(?msg, "received");
            let msg = msg[0].take().unwrap();
            ensure!(
                msg.0 == remote_addr,
                "received response from unexpected address"
            )
        }
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
                loop {
                    let msgs = cn.recv(&mut slots[..]).await?;
                    cn.send(msgs.iter_mut().map_while(Option::take)).await?;
                }
            })
            .await?;
    }
    Ok(())
}
