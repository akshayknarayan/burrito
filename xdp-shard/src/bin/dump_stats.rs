use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
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

    let mut prog = xdp_port::BpfHandles::load_on_interface_name(&opt.interface)?;
    let ifn = opt.interface;
    let stop: Arc<AtomicBool> = Arc::new(false.into());
    let s = stop.clone();
    ctrlc::set_handler(move || {
        tracing::warn!("stopping");
        s.store(true, Ordering::SeqCst);
    })
    .unwrap();

    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        std::time::Duration::from_secs(1);
        let (stats, prev) = prog.get_stats()?;

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
    }

    Ok(())
}
