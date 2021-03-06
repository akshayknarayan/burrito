use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "i", long = "interface")]
    interface: String,
}

fn main() -> Result<(), color_eyre::eyre::Report> {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    let opt = Opt::from_args();

    tracing_subscriber::fmt::init();
    color_eyre::install()?;

    let mut prog =
        xdp_shard::BpfHandles::<xdp_shard::Ingress>::load_on_interface_name(&opt.interface)?;
    let ifn = opt.interface;
    let stop: Arc<AtomicBool> = Arc::new(false.into());
    let s = stop.clone();
    ctrlc::set_handler(move || {
        tracing::warn!("stopping");
        s.store(true, Ordering::SeqCst);
    })
    .unwrap();

    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let (stats, prev) = prog.get_stats()?;
        tracing::debug!("Checking rxqs");

        let mut rxqs = stats.get_rxq_cpu_port_count();
        let prev_rxqs = prev.get_rxq_cpu_port_count();
        xdp_shard::diff_maps(&mut rxqs, &prev_rxqs);
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
