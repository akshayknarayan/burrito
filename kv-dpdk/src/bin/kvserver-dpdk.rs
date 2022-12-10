use bertha::{ChunnelConnection, ChunnelListener};
use color_eyre::eyre::Report;
use dpdk_direct::{DpdkInlineChunnel, DpdkInlineCn, DpdkInlineReqChunnel};
use futures_util::stream::TryStreamExt;
use kvstore::{kv::Store, Msg};
use std::net::{SocketAddr, SocketAddrV4};
use structopt::StructOpt;
use tracing::{debug, info, instrument, trace, warn};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvserver")]
struct Opt {
    #[structopt(short, long, default_value = "0.0.0.0:5001")]
    addr: SocketAddrV4,

    #[structopt(short, long)]
    num_shards: u16,

    #[structopt(short, long)]
    cfg: std::path::PathBuf,

    #[structopt(long)]
    conns: bool,

    #[structopt(long)]
    log: bool,
}

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    color_eyre::install()?;
    if opt.log {
        tracing_subscriber::fmt::init();
    }

    info!(?opt.conns, ?opt.num_shards, ?opt.addr, "KV Server, no chunnels");
    let ch = DpdkInlineChunnel::new(opt.cfg, opt.num_shards as _)?;
    for i in 1..opt.num_shards {
        let ip = opt.addr.ip();
        let base_port = opt.addr.port();
        let shard_addr = SocketAddrV4::new(*ip, base_port + i + 1);
        let ch = ch.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(single_shard_no_conns(ch, shard_addr)).unwrap();
        });
    }

    let shard_addr = SocketAddrV4::new(*opt.addr.ip(), opt.addr.port() + 1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    if opt.conns {
        let ch = DpdkInlineReqChunnel::from(ch);
        rt.block_on(single_shard_conns(ch, shard_addr))?;
    } else {
        rt.block_on(single_shard_no_conns(ch, shard_addr))?;
    }

    Ok(())
}

#[instrument(level = "debug", err)]
async fn single_shard_no_conns(
    mut ch: DpdkInlineChunnel,
    addr: SocketAddrV4,
) -> Result<(), Report> {
    info!(?addr, "listening");

    // initialize the kv store.
    let store = Store::default();

    let cn: DpdkInlineCn = ch
        .listen(SocketAddr::V4(addr))
        .into_inner()?
        .try_next()
        .await?
        .unwrap();

    conn(cn, store).await
}

async fn conn(cn: DpdkInlineCn, store: Store) -> Result<(), Report> {
    let mut slots: Vec<_> = (0..16).map(|_| None).collect();
    debug!("new");
    loop {
        trace!("call recv");
        let msgs = match cn.recv(&mut slots).await {
            Ok(ms) => ms,
            Err(e) => {
                warn!(err = ?e, "exiting on recv error");
                break Ok(());
            }
        };

        trace!(sz = ?msgs.iter().map_while(|x| x.as_ref().map(|_| 1)).sum::<usize>(), "got batch");

        match cn
            .send(msgs.iter_mut().map_while(Option::take).map(|(a, mut buf)| {
                let msg: Msg = bincode::deserialize(&buf[..]).unwrap();
                let rsp = store.call(msg);

                let sz = bincode::serialized_size(&rsp).unwrap() as usize;
                buf.resize(sz, 0);
                bincode::serialize_into(&mut buf[..], &rsp).unwrap();
                (a, buf)
            }))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                warn!(err = ?e, "exiting on send error");
                break Ok(());
            }
        }
    }
}

#[instrument(level = "debug", err)]
async fn single_shard_conns(
    mut ch: DpdkInlineReqChunnel,
    addr: SocketAddrV4,
) -> Result<(), Report> {
    info!(?addr, "listening");

    // initialize the kv store.
    let store = Store::default();

    let st = ch.listen(SocketAddr::V4(addr)).await?;
    st.try_for_each_concurrent(None, |cn| conn(cn, store.clone()))
        .await
}
