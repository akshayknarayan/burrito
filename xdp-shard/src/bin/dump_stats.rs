use std::sync::Arc;
use structopt::StructOpt;

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "i", long = "interface")]
    interface: String,
}

fn main() -> Result<(), StdError> {
    let opt = Opt::from_args();

    tracing_subscriber::fmt::init();

    let ifindex = xdp_port::get_interface_id(&opt.interface)?;
    let stop: Arc<std::sync::atomic::AtomicBool> = Arc::new(false.into());

    let s = stop.clone();
    ctrlc::set_handler(move || {
        tracing::warn!("stopping, removing XDP program");
        s.store(true, std::sync::atomic::Ordering::SeqCst);
    })
    .unwrap();

    let prog = xdp_port::libbpf::BpfHandles::new(ifindex, stop)?;

    let ifn = opt.interface;
    prog.dump_loop(std::time::Duration::from_secs(1), move |stats, prev| {
        let mut rxqs = stats.get_rxq_cpu_port_count();
        let prev_rxqs = prev.get_rxq_cpu_port_count();
        xdp_port::diff_maps(&mut rxqs, &prev_rxqs);
        for (rxq, cpus) in rxqs.iter().enumerate() {
            for (cpu, portcounts) in cpus.iter().enumerate() {
                for (port, count) in portcounts.iter() {
                    if *count > 0 {
                        tracing::info!(interface = ?&ifn, rxq, cpu, port, count, "");
                    }
                }
            }
        }
    })
}
