//! Achieved throughput as number of connections and file size increases.
//!
//! One connection per request, n simultaneous clients looping on establishing connections that
//! each download m bytes.

use bertha::udp::{UdpReqChunnel, UdpSkChunnel};
use color_eyre::eyre::{bail, eyre, Report, WrapErr};
#[cfg(feature = "dpdk-direct")]
use dpdk_direct::{DpdkInlineChunnel, DpdkUdpReqChunnel, DpdkUdpSkChunnel};
#[cfg(feature = "shenango-chunnel")]
use shenango_chunnel::{ShenangoUdpReqChunnel, ShenangoUdpSkChunnel};
use std::net::Ipv4Addr;
use std::time::Duration;
use structopt::StructOpt;
#[cfg(feature = "tcp")]
use tcp::TcpChunnel;
use tracing::{debug, info, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[cfg(feature = "shenango-bertha")]
mod shenangort;

#[cfg(feature = "shenango")]
mod shenango_raw;

#[cfg(feature = "dpdk-direct")]
mod dpdk_inline_raw;

#[cfg(feature = "tcp")]
mod kernel_tcp_raw;

mod using_chunnel_connection;

#[derive(Clone, Copy, Debug)]
enum BerthaNess {
    Full { stack_depth: usize },
    UsingChunnelConnection,
    Raw,
}

impl BerthaNess {
    fn try_get_stack_depth(&self) -> Result<usize, ()> {
        match self {
            BerthaNess::Full { stack_depth } => Ok(*stack_depth),
            _ => Err(()),
        }
    }

    #[cfg(any(feature = "use_shenango", feature = "dpdk-direct", feature = "tcp"))]
    fn is_raw(&self) -> bool {
        match self {
            BerthaNess::Raw => true,
            _ => false,
        }
    }
}

impl std::str::FromStr for BerthaNess {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        let parts: Vec<_> = lower.split(':').collect();
        Ok(match &parts[..] {
            ["full", depth] | ["false", depth] => BerthaNess::Full {
                stack_depth: depth
                    .parse()
                    .wrap_err_with(|| eyre!("could not parse stack depth: {:?}", depth))?,
            },
            ["berthaconn"] | ["true"] => BerthaNess::UsingChunnelConnection,
            ["raw"] => BerthaNess::Raw,
            x => bail!("unknown use_bertha specifier {:?}", x),
        })
    }
}

#[derive(Debug, Clone, StructOpt)]
struct Opt {
    #[cfg(any(feature = "use_shenango", feature = "dpdk-direct"))]
    #[structopt(long)]
    cfg: std::path::PathBuf,

    // TODO shenango-TCP support
    #[cfg(feature = "tcp")]
    #[structopt(long)]
    use_tcp: bool,

    #[structopt(short, long)]
    port: u16,

    #[structopt(long)]
    datapath: String,

    #[structopt(long)]
    no_bertha: BerthaNess,

    #[structopt(long)]
    num_threads: usize,

    #[structopt(subcommand)]
    mode: Mode,
}

#[derive(Debug, Clone, StructOpt)]
pub struct Client {
    #[structopt(long)]
    addr: Ipv4Addr,

    #[structopt(long)]
    num_clients: usize,

    #[structopt(long)]
    download_size: usize,

    #[structopt(long)]
    packet_size: usize,

    #[structopt(long)]
    out_file: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, StructOpt)]
pub enum Mode {
    Client(Client),
    Server,
}

fn write_results(
    of: Option<impl AsRef<std::path::Path>>,
    tot_bytes: usize,
    tot_pkts: usize,
    elapsed: Duration,
    num_clients: usize,
    download_size: usize,
    packet_size: usize,
) -> Result<(), Report> {
    let rate = (tot_bytes as f64 * 8.) / elapsed.as_secs_f64();
    let pps = (tot_pkts as f64) / elapsed.as_secs_f64();
    info!(?num_clients, ?download_size, ?packet_size, rate_mbps=?(rate / 1e6), rate_mpps=?pps, "finished");
    if let Some(of) = of {
        use std::io::Write;
        let mut f = std::fs::File::create(of)?;
        writeln!(
            &mut f,
            "num_clients,download_size,packet_size,tot_bytes,tot_pkts,elapsed_us,rate_bps,rate_pps"
        )?;
        writeln!(
            &mut f,
            "{:?},{:?},{:?},{:?},{:?},{:?},{:?},{:?}",
            num_clients,
            download_size,
            packet_size,
            tot_bytes,
            tot_pkts,
            elapsed.as_micros(),
            rate,
            pps,
        )?;
    } else {
        println!(
                "num_clients={:?},download_size={:?},packet_size={:?},tot_bytes={:?},tot_pkts={:?},elapsed_us={:?},rate_bps={:?},rate_pps={:?}",
                num_clients,
                download_size,
                packet_size,
                tot_bytes,
                tot_pkts,
                elapsed.as_micros(),
                rate,
                pps
            );
    }
    Ok(())
}

#[inline(always)]
fn validate_message(ms: &[u8]) -> Result<(u64, u64), Report> {
    let msg = match ms {
        msg if msg[..8] == [1, 2, 3, 4, 5, 6, 7, 8] => msg,
        msg => {
            warn!(?msg, "bad client request");
            bail!("bad connection: {:?}", msg);
        }
    };

    let remaining = u64::from_le_bytes(msg[8..16].try_into().unwrap());
    let pkt_size = u64::from_le_bytes(msg[16..24].try_into().unwrap());
    #[cfg(not(feature = "tcp"))]
    if !(64..=1460).contains(&pkt_size) {
        debug!(?pkt_size, "bad client request");
        bail!(
            "bad client request: remaining {:?}, pkt_size {:?}",
            remaining,
            pkt_size
        );
    }

    #[cfg(feature = "tcp")]
    if pkt_size == 0 {
        debug!(?pkt_size, "bad client request");
        bail!(
            "bad client request: remaining {:?}, pkt_size {:?}",
            remaining,
            pkt_size
        );
    }

    Ok((remaining, pkt_size))
}

fn main() -> Result<(), Report> {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    color_eyre::install()?;
    let Opt {
        #[cfg(any(feature = "use_shenango", feature = "dpdk-direct"))]
        cfg,
        #[cfg(feature = "tcp")]
        use_tcp,
        port,
        datapath,
        num_threads,
        no_bertha,
        mode,
    } = Opt::from_args();

    #[cfg(feature = "use_shenango")]
    {
        if no_bertha.is_raw() && (datapath == "shenango" || datapath == "shenangort") {
            shenango_raw::shenango_nobertha(cfg, port, mode);
            unreachable!();
        } else if datapath == "shenangort" {
            let stack_depth = match no_bertha {
                BerthaNess::Full { stack_depth } => stack_depth,
                BerthaNess::UsingChunnelConnection => 0,
                BerthaNess::Raw => unreachable!(),
            };

            shenangort::shenangort_bertha(cfg, port, mode, stack_depth);
            unreachable!();
        }
    }
    #[cfg(not(feature = "use_shenango"))]
    {
        if datapath.contains("shenango") {
            bail!("`shenango_direct` feature not enabled");
        }
    }

    #[cfg(feature = "dpdk-direct")]
    {
        if datapath == "dpdkinline" && no_bertha.is_raw() {
            return dpdk_inline_raw::dpdk_inline_nobertha(cfg, port, mode, num_threads);
        }
    }

    #[cfg(feature = "tcp")]
    {
        if datapath == "kernel" && no_bertha.is_raw() {
            return kernel_tcp_raw::kernel_tcp_inline_raw(port, mode, num_threads);
        }
    }

    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let packet_size = cl.packet_size;
        let num_clients = cl.num_clients;
        let of = cl.out_file.take();

        let (tot_bytes, tot_pkts, elapsed) = if no_bertha.try_get_stack_depth().is_err() {
            match datapath.as_str() {
                #[cfg(feature = "dpdk-direct")]
                "dpdkinline" => {
                    let ch = DpdkInlineChunnel::new(cfg, num_threads)?;
                    using_chunnel_connection::run_clients_no_bertha(ch, cl, port, num_threads)?
                }
                #[cfg(feature = "dpdk-direct")]
                "dpdkthread" => {
                    let ch = DpdkUdpSkChunnel::new(cfg)?;
                    using_chunnel_connection::run_clients_no_bertha(ch, cl, port, num_threads)?
                }
                #[cfg(not(feature = "tcp"))]
                "kernel" => {
                    let ch = UdpSkChunnel;
                    using_chunnel_connection::run_clients_no_bertha(ch, cl, port, num_threads)?
                }
                #[cfg(feature = "tcp")]
                "kernel" if !use_tcp => {
                    let ch = UdpSkChunnel;
                    using_chunnel_connection::run_clients_no_bertha(ch, cl, port, num_threads)?
                }
                #[cfg(feature = "tcp")]
                "kernel" if use_tcp => {
                    let ch: TcpChunnel<true> = TcpChunnel;
                    using_chunnel_connection::run_clients_no_bertha(ch, cl, port, num_threads)?
                }
                #[cfg(not(feature = "dpdk-direct"))]
                x if x.contains("dpdk") => bail!("`dpdk-direct` feature not enabled"),
                x => bail!("no_bertha mode not implemented for {}", x),
            }
        } else {
            let stack_depth = no_bertha.try_get_stack_depth().unwrap();
            match datapath.as_str() {
                #[cfg(feature = "dpdk-direct")]
                "dpdkthread" => {
                    let ch = DpdkUdpSkChunnel::new(cfg).wrap_err("make dpdk-thread chunnel")?;
                    using_chunnel_connection::run_clients(ch, cl, port, num_threads, stack_depth)?
                }
                #[cfg(feature = "dpdk-direct")]
                "dpdkinline" => {
                    let ch = DpdkInlineChunnel::new(cfg, num_threads)
                        .wrap_err("make dpdk-multi chunnel")?;
                    using_chunnel_connection::run_clients(ch, cl, port, num_threads, stack_depth)?
                }
                #[cfg(feature = "use_shenango")]
                "shenango" => {
                    let ch = ShenangoUdpSkChunnel::new(cfg);
                    using_chunnel_connection::run_clients(ch, cl, port, num_threads, stack_depth)?
                }
                #[cfg(not(feature = "tcp"))]
                "kernel" => {
                    let ch = UdpSkChunnel;
                    using_chunnel_connection::run_clients(ch, cl, port, num_threads, stack_depth)?
                }
                #[cfg(feature = "tcp")]
                "kernel" if !use_tcp => {
                    let ch = UdpSkChunnel;
                    using_chunnel_connection::run_clients(ch, cl, port, num_threads, stack_depth)?
                }
                #[cfg(feature = "tcp")]
                "kernel" if use_tcp => {
                    let ch: TcpChunnel<true> = TcpChunnel;
                    using_chunnel_connection::run_clients(ch, cl, port, num_threads, stack_depth)?
                }
                #[cfg(not(feature = "dpdk-direct"))]
                x if x.contains("dpdk") => bail!("`dpdk-direct` feature not enabled"),
                d => bail!("unknown datapath {:?}", d),
            }
        };

        write_results(
            of,
            tot_bytes,
            tot_pkts,
            elapsed,
            num_clients,
            download_size,
            packet_size,
        )?;
    } else {
        // server
        if let Ok(stack_depth) = no_bertha.try_get_stack_depth() {
            match datapath.as_str() {
                #[cfg(feature = "dpdk-direct")]
                "dpdkthread" => {
                    let ch = DpdkUdpSkChunnel::new(cfg)?;
                    let ch = DpdkUdpReqChunnel(ch);
                    using_chunnel_connection::run_server(ch, port, num_threads, stack_depth)?;
                }
                #[cfg(feature = "dpdk-direct")]
                "dpdkinline" => {
                    let ch = DpdkInlineChunnel::new(cfg, num_threads)?;
                    using_chunnel_connection::run_server(ch, port, num_threads, stack_depth)?;
                }
                #[cfg(feature = "use_shenango")]
                "shenango" => {
                    let ch = ShenangoUdpSkChunnel::new(cfg);
                    let ch = ShenangoUdpReqChunnel(ch);
                    using_chunnel_connection::run_server(ch, port, num_threads, stack_depth)?;
                }
                #[cfg(not(feature = "tcp"))]
                "kernel" => {
                    let ch = UdpReqChunnel;
                    using_chunnel_connection::run_server(ch, port, num_threads, stack_depth)?;
                }
                #[cfg(feature = "tcp")]
                "kernel" if !use_tcp => {
                    let ch = UdpReqChunnel;
                    using_chunnel_connection::run_server(ch, port, num_threads, stack_depth)?;
                }
                #[cfg(feature = "tcp")]
                "kernel" if use_tcp => {
                    let ch: TcpChunnel<true> = TcpChunnel;
                    using_chunnel_connection::run_server(ch, port, num_threads, stack_depth)?;
                }
                #[cfg(not(feature = "dpdk-direct"))]
                x if x.contains("dpdk") => bail!("`dpdk-direct` feature not enabled"),
                d => bail!("unknown datapath {:?}", d),
            }
        } else {
            match datapath.as_ref() {
                #[cfg(feature = "dpdk-direct")]
                "dpdkinline" => {
                    let ch = DpdkInlineChunnel::new(cfg, num_threads)?;
                    using_chunnel_connection::run_server_no_bertha(ch, port, num_threads)?;
                }
                #[cfg(feature = "dpdk-direct")]
                "dpdkthread" => {
                    let ch = DpdkUdpSkChunnel::new(cfg)?;
                    let ch = DpdkUdpReqChunnel(ch);
                    using_chunnel_connection::run_server_no_bertha(ch, port, num_threads)?;
                }
                #[cfg(not(feature = "tcp"))]
                "kernel" => {
                    let ch = UdpReqChunnel;
                    using_chunnel_connection::run_server_no_bertha(ch, port, num_threads)?;
                }
                #[cfg(feature = "tcp")]
                "kernel" if !use_tcp => {
                    let ch = UdpReqChunnel;
                    using_chunnel_connection::run_server_no_bertha(ch, port, num_threads)?;
                }
                #[cfg(feature = "tcp")]
                "kernel" if use_tcp => {
                    let ch: TcpChunnel<true> = TcpChunnel;
                    using_chunnel_connection::run_server_no_bertha(ch, port, num_threads)?;
                }
                #[cfg(not(feature = "dpdk-direct"))]
                x if x.contains("dpdk") => bail!("`dpdk-direct` feature not enabled"),
                x => bail!("no_bertha mode not implemented for {}", x),
            }
        }
    }

    Ok(())
}
