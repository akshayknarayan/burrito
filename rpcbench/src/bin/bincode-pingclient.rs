use bertha::{
    bincode::SerializeChunnelProject,
    either::Either,
    negotiate::{GetOffers, NegotiatePicked},
    negotiate_client,
    udp::UdpSkChunnel,
    uds::UnixSkChunnel,
    util::ProjectLeft,
    Chunnel, ChunnelConnector, CxList,
};
use burrito_localname_ctl::LocalNameChunnel;
use color_eyre::eyre::{bail, Report, WrapErr};
use rpcbench::EncryptOpt;
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;
use tls_tunnel::{TLSChunnel, TlsConnAddr};
use tracing::{debug, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_client")]
struct Opt {
    #[structopt(short, long)]
    burrito_root: Option<PathBuf>,
    #[structopt(long)]
    addr: Option<SocketAddr>,
    #[structopt(long)]
    unix_addr: Option<PathBuf>,

    #[structopt(long)]
    no_negotiation: bool,

    #[structopt(short, long)]
    work: rpcbench::Work,
    #[structopt(short, long)]
    size_of_req: Option<usize>,
    #[structopt(short, long)]
    iters: usize,
    #[structopt(long)]
    reqs_per_iter: usize,

    #[structopt(long, default_value = "/tmp")]
    encr_unix_root: PathBuf,
    #[structopt(long)]
    encr_ghostunnel_root: Option<PathBuf>,

    #[structopt(short, long)]
    out_file: Option<std::path::PathBuf>,
}

impl rpcbench::AsEncryptOpt for Opt {
    fn gt_root(&self) -> Option<PathBuf> {
        self.encr_ghostunnel_root.clone()
    }
    fn unix_root(&self) -> PathBuf {
        self.encr_unix_root.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install().unwrap();
    let opt = Opt::from_args();
    let subscriber = tracing_subscriber::registry();
    let timing_downcaster = if opt.out_file.is_some() {
        let timing_layer = tracing_timing::Builder::default()
            .no_span_recursion()
            .span_close_events()
            .events(|e: &tracing::Event| {
                let mut val = String::new();
                let mut f = |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                    if field.name() == "message" {
                        val.push_str(&format!(" {:?} ", value));
                    } else if field.name() == "which" {
                        val.push_str(&format!(" which={:?} ", value));
                    };
                };
                e.record(&mut f);
                val
            })
            .layer(|| tracing_timing::Histogram::new_with_max(1_000_000, 2).unwrap());
        let timing_downcaster = timing_layer.downcaster();
        let subscriber = subscriber
            .with(timing_layer)
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.clone().init();
        Some((timing_downcaster, d))
    } else {
        let subscriber = subscriber
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(ErrorLayer::default());
        let d = tracing::Dispatch::new(subscriber);
        d.init();
        None
    };

    let enc = EncryptOpt::from(&opt);

    let pp = rpcbench::PingParams {
        work: opt.work,
        padding: vec![0u8; opt.size_of_req.unwrap_or_default()],
    };

    let per_iter = opt.reqs_per_iter;
    let out_file = opt.out_file.clone();
    let durs = match (opt, enc) {
        (
            Opt {
                unix_addr: Some(addr),
                burrito_root,
                iters,
                reqs_per_iter,
                no_negotiation,
                ..
            },
            _,
        ) => {
            // raw unix mode
            info!(?addr, "uds mode");
            let ctr = |addr: PathBuf| {
                let br = burrito_root.clone();
                async move {
                    let u = if let Some(r) = br {
                        UnixSkChunnel::with_root(r).connect(()).await?
                    } else {
                        UnixSkChunnel::default().connect(()).await?
                    };
                    let cn = negotiate_client(
                        CxList::from(SerializeChunnelProject::default()),
                        u,
                        addr.clone(),
                    )
                    .await?;
                    Ok(ProjectLeft::new(addr, cn))
                }
            };

            let noneg_ctr = |addr: PathBuf| {
                let br = burrito_root.clone();
                async move {
                    let u = if let Some(r) = br {
                        UnixSkChunnel::with_root(r).connect(()).await?
                    } else {
                        UnixSkChunnel::default().connect(()).await?
                    };
                    let mut ch = SerializeChunnelProject::default();
                    let cn = ch.connect_wrap(u).await?;
                    Ok(ProjectLeft::new(addr, cn))
                }
            };

            if no_negotiation {
                rpcbench::client_ping(addr, noneg_ctr, pp, iters, reqs_per_iter).await?
            } else {
                rpcbench::client_ping(addr, ctr, pp, iters, reqs_per_iter).await?
            }
        }
        (
            Opt {
                burrito_root: Some(root),
                addr: Some(addr),
                iters,
                reqs_per_iter,
                no_negotiation,
                ..
            },
            None,
        ) => {
            let fncl = |addr| {
                let r = root.clone();
                async move {
                    let lch = LocalNameChunnel::<_, _, SocketAddr>::new(
                        r.clone(),
                        None,
                        UnixSkChunnel::with_root(r),
                        bertha::CxNil,
                    )
                    .await?;
                    let stack = CxList::from(SerializeChunnelProject::default()).wrap(lch);
                    let cn = negotiate_client(stack, UdpSkChunnel.connect(()).await?, addr).await?;
                    Ok(ProjectLeft::new(Either::Left(addr), cn))
                }
            };
            let noneg_fncl = |addr| {
                let r = root.clone();
                async move {
                    let lch = LocalNameChunnel::<_, _, SocketAddr>::new(
                        r.clone(),
                        None,
                        UnixSkChunnel::with_root(r),
                        bertha::CxNil,
                    )
                    .await?;
                    let mut stack = CxList::from(SerializeChunnelProject::default()).wrap(lch);
                    let cn = stack.connect_wrap(UdpSkChunnel.connect(()).await?).await?;
                    Ok(ProjectLeft::new(Either::Left(addr), cn))
                }
            };
            info!(?root, ?addr, encryption = "no", "Burrito mode");
            if no_negotiation {
                rpcbench::client_ping(addr, noneg_fncl, pp, iters, reqs_per_iter).await?
            } else {
                rpcbench::client_ping(addr, fncl, pp, iters, reqs_per_iter).await?
            }
        }
        (
            Opt {
                burrito_root: Some(root),
                addr: Some(addr),
                iters,
                reqs_per_iter,
                no_negotiation,
                ..
            },
            Some(enc),
        ) => {
            let fncl = |(addr, x)| {
                let enc = enc.clone();
                let r = root.clone();
                async move {
                    let lch = LocalNameChunnel::new(
                        r.clone(),
                        None,
                        UnixSkChunnel::with_root(r),
                        bertha::CxNil,
                    )
                    .await?;
                    let tls = TLSChunnel::<(SocketAddr, TlsConnAddr)>::new(
                        enc.unix_root(),
                        enc.bin_path(),
                        enc.cert_dir_path(),
                    )
                    .connect(addr);
                    let stack = CxList::from(SerializeChunnelProject::default())
                        .wrap(lch)
                        .wrap(tls);
                    let cn = negotiate_client(stack, UdpSkChunnel.connect(()).await?, addr)
                        .await
                        .wrap_err("burrito-mode yes-neg connector")?;
                    Ok(ProjectLeft::new(Either::Left((addr, x)), cn))
                }
            };
            let noneg_fncl = |(addr, x)| {
                let enc = enc.clone();
                let r = root.clone();
                async move {
                    let lch = LocalNameChunnel::new(
                        r.clone(),
                        None,
                        UnixSkChunnel::with_root(r),
                        bertha::CxNil,
                    )
                    .await?;
                    let tls = TLSChunnel::<(SocketAddr, TlsConnAddr)>::new(
                        enc.unix_root(),
                        enc.bin_path(),
                        enc.cert_dir_path(),
                    )
                    .connect(addr);
                    let mut stack = CxList::from(SerializeChunnelProject::default())
                        .wrap(lch)
                        .wrap(tls);
                    let nonce = stack.offers().next().unwrap();
                    let nonce_buf = bincode::serialize(&nonce)?;
                    stack.call_negotiate_picked(&nonce_buf).await;
                    let cn = stack
                        .connect_wrap(UdpSkChunnel.connect(()).await?)
                        .await
                        .wrap_err("burrito-mode no-neg connector")?;
                    Ok(ProjectLeft::new(Either::Left((addr, x)), cn))
                }
            };

            info!(?root, ?addr, encryption = "yes", "Burrito mode");
            if no_negotiation {
                rpcbench::client_ping(
                    (addr, TlsConnAddr::Request),
                    noneg_fncl,
                    pp,
                    iters,
                    reqs_per_iter,
                )
                .await?
            } else {
                rpcbench::client_ping((addr, TlsConnAddr::Request), fncl, pp, iters, reqs_per_iter)
                    .await?
            }
        }
        (
            Opt {
                burrito_root: None,
                addr: Some(addr),
                iters,
                reqs_per_iter,
                no_negotiation,
                ..
            },
            None,
        ) => {
            // raw udp mode
            info!(?addr, encrypt = "no", "UDP mode");
            let fncl = |addr| async move {
                let cn = negotiate_client(
                    CxList::from(SerializeChunnelProject::default()),
                    UdpSkChunnel.connect(()).await?,
                    addr,
                )
                .await?;
                Ok(ProjectLeft::new(addr, cn))
            };
            let noneg_fncl = |addr| async move {
                let mut ch = SerializeChunnelProject::default();
                let cn = ch.connect_wrap(UdpSkChunnel.connect(()).await?).await?;
                Ok(ProjectLeft::new(addr, cn))
            };

            if no_negotiation {
                rpcbench::client_ping(addr, noneg_fncl, pp, iters, reqs_per_iter).await?
            } else {
                rpcbench::client_ping(addr, fncl, pp, iters, reqs_per_iter).await?
            }
        }
        (
            Opt {
                burrito_root: None,
                addr: Some(addr),
                iters,
                reqs_per_iter,
                no_negotiation,
                ..
            },
            Some(enc),
        ) => {
            // raw udp mode
            info!(?addr, encrypt = "yes", "UDP mode");
            let fncl = |addr| {
                let enc = enc.clone();
                async move {
                    let tls = TLSChunnel::<TlsConnAddr>::new(
                        enc.unix_root(),
                        enc.bin_path(),
                        enc.cert_dir_path(),
                    )
                    .connect(addr);
                    let cn = negotiate_client(
                        CxList::from(SerializeChunnelProject::default()).wrap(tls),
                        UdpSkChunnel.connect(()).await?,
                        addr,
                    )
                    .await?;
                    Ok(ProjectLeft::new(TlsConnAddr::Request, cn))
                }
            };

            let noneg_fncl = |addr| {
                let enc = enc.clone();
                async move {
                    let tls = TLSChunnel::<TlsConnAddr>::new(
                        enc.unix_root(),
                        enc.bin_path(),
                        enc.cert_dir_path(),
                    )
                    .connect(addr);
                    let mut ch = CxList::from(SerializeChunnelProject::default()).wrap(tls);
                    let nonce = ch.offers().next().unwrap();
                    let nonce_buf = bincode::serialize(&nonce)?;
                    ch.call_negotiate_picked(&nonce_buf).await;
                    let cn = ch
                        .connect_wrap(bertha::util::Nothing::<()>::default())
                        .await
                        .wrap_err("encr-mode no-neg connector")?;
                    Ok(ProjectLeft::new(TlsConnAddr::Request, cn))
                }
            };

            if no_negotiation {
                rpcbench::client_ping(addr, noneg_fncl, pp, iters, reqs_per_iter).await?
            } else {
                rpcbench::client_ping(addr, fncl, pp, iters, reqs_per_iter).await?
            }
        }
        _ => {
            bail!("Bad option set");
        }
    };

    info!("done");
    if let Some(ref path) = out_file {
        rpcbench::write_durs(path, durs)?;

        debug!("writing trace file");
        let (downcaster, d) = timing_downcaster.unwrap();
        let path = path.with_extension("trace");
        rpcbench::write_tracing(&path, downcaster, &d, &per_iter.to_string())?;
    }
    Ok(())
}
