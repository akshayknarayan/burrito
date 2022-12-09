use color_eyre::eyre::{eyre, Report, WrapErr};
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone)]
pub enum Op {
    Get(usize, String),
    Update(usize, String, String),
}

impl Op {
    pub fn client_id(&self) -> usize {
        match self {
            Op::Get(i, _) | Op::Update(i, _, _) => *i,
        }
    }

    pub fn key(&self) -> &str {
        match self {
            Op::Get(_, k) | Op::Update(_, k, _) => k,
        }
    }

    pub async fn exec(
        self,
        cl: &kvstore::KvClient<
            impl bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static,
        >,
    ) -> Result<Option<String>, Report> {
        match self {
            Op::Get(_, k) => cl.get(k).await,
            Op::Update(_, k, v) => cl.update(k, v).await,
        }
    }
}

impl std::str::FromStr for Op {
    type Err = Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp: Vec<&str> = s.split_whitespace().collect();
        Ok(if sp.len() == 3 && sp[1] == "GET" {
            Op::Get(sp[0].parse()?, sp[2].into())
        } else if sp.len() == 4 && sp[1] == "UPDATE" {
            Op::Update(sp[0].parse()?, sp[2].into(), sp[3].into())
        } else {
            return Err(eyre!("Invalid line: {:?}", s));
        })
    }
}

// accesses group_by client
pub fn group_by_client(accesses: Vec<Op>) -> HashMap<usize, Vec<Op>> {
    let mut access_by_client: HashMap<usize, Vec<Op>> = Default::default();
    for o in accesses {
        let k = o.client_id();
        access_by_client.entry(k).or_default().push(o);
    }

    access_by_client
}

pub fn ops(f: std::path::PathBuf) -> Result<Vec<Op>, Report> {
    use std::io::BufRead;
    let f = std::fs::File::open(&f).wrap_err(eyre!("Could not open {:?}", &f))?;
    let f = std::io::BufReader::new(f);
    Ok(f.lines().filter_map(|l| l.ok()?.parse().ok()).collect())
}

pub fn const_paced_ops_stream(
    ops: Vec<Op>,
    interarrival_micros: u64,
    client_id: usize,
) -> impl futures_util::stream::Stream<Item = (usize, Op)> + Send {
    let len = ops.len();
    let tkr = poisson_ticker::SpinTicker::new_const_with_log_id(
        Duration::from_micros(interarrival_micros),
        client_id,
    )
    .zip(futures_util::stream::iter((0..len).rev()));
    tkr.zip(futures_util::stream::iter(ops))
        .map(|((_, i), o)| (i, o))
}

pub fn poisson_paced_ops_stream(
    ops: Vec<Op>,
    interarrival_micros: u64,
    client_id: usize,
) -> impl futures_util::stream::Stream<Item = (usize, Op)> + Send {
    let len = ops.len();
    let tkr = poisson_ticker::SpinTicker::new_const_with_log_id(
        Duration::from_micros(interarrival_micros),
        client_id,
    )
    .zip(futures_util::stream::iter((0..len).rev()));
    tkr.zip(futures_util::stream::iter(ops))
        .map(|((_, i), o)| (i, o))
}

pub fn write_results(
    mut durs: Vec<Duration>,
    remaining_inflight: usize,
    time: Duration,
    num_clients: usize,
    interarrival_client_micros: usize,
    out_file: Option<PathBuf>,
) {
    durs.sort();
    let len = durs.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| durs[i])
        .collect();
    let num = durs.len() as f64;
    let achieved_load_req_per_sec = num / time.as_secs_f64();
    let offered_load_req_per_sec = (num + remaining_inflight as f64) / time.as_secs_f64();
    let attempted_load_req_per_sec = num_clients as f64 / (interarrival_client_micros as f64 / 1e6);
    info!(
        num = ?&durs.len(), elapsed = ?time, ?remaining_inflight,
        ?achieved_load_req_per_sec, ?offered_load_req_per_sec, ?attempted_load_req_per_sec,
        min = ?durs[0], p25 = ?quantiles[0], p50 = ?quantiles[1],
        p75 = ?quantiles[2], p95 = ?quantiles[3], max = ?durs[durs.len() - 1],
        "Did accesses"
    );

    println!(
        "Did accesses:\
        num = {:?},\
        elapsed_sec = {:?},\
        remaining_inflight = {:?},\
        achieved_load_req_per_sec = {:?},\
        offered_load_req_per_sec = {:?},\
        attempted_load_req_per_sec = {:?},\
        min_us = {:?},\
        p25_us = {:?},\
        p50_us = {:?},\
        p75_us = {:?},\
        p95_us = {:?},\
        max_us = {:?}",
        durs.len(),
        time.as_secs_f64(),
        remaining_inflight,
        achieved_load_req_per_sec,
        offered_load_req_per_sec,
        attempted_load_req_per_sec,
        durs[0].as_micros(),
        quantiles[0].as_micros(),
        quantiles[1].as_micros(),
        quantiles[2].as_micros(),
        quantiles[3].as_micros(),
        durs[durs.len() - 1].as_micros(),
    );

    if let Some(f) = out_file {
        let mut f = std::fs::File::create(f).expect("Open out file");
        use std::io::Write;
        writeln!(&mut f, "Interarrival_us NumOps Completion_ms Latency_us").expect("write");
        let len = durs.len();
        for d in durs {
            writeln!(
                &mut f,
                "{} {} {} {}",
                interarrival_client_micros,
                len,
                time.as_millis(),
                d.as_micros()
            )
            .expect("write");
        }
    }
}

pub fn dump_tracing(
    timing: &'_ tracing_timing::TimingLayer,
    f: &mut std::fs::File,
) -> Result<(), Report> {
    use std::io::prelude::*;
    timing.force_synchronize();
    timing.with_histograms(|hs| {
        for (span_group, hs) in hs {
            for (event_group, h) in hs {
                let tag = format!("{}.{}", span_group, event_group);
                write!(
                    f,
                    "tracing: \
                        event = {}, \
                        min   = {}, \
                        p25   = {}, \
                        p50   = {}, \
                        p75   = {}, \
                        p95   = {}, \
                        max   = {}, \
                        cnt   = {} \
                        ",
                    &tag,
                    h.min(),
                    h.value_at_quantile(0.25),
                    h.value_at_quantile(0.5),
                    h.value_at_quantile(0.75),
                    h.value_at_quantile(0.95),
                    h.max(),
                    h.len(),
                )?;
            }
        }

        Ok::<_, Report>(())
    })?;

    Ok(())
}

// TODO invoke ycsbc-mock
//pub fn generate(specfile: impl AsRef<std::path::Path>) {
//    // hardcode workload b for now
//    let out = std::process::Command::new("./ycsbc-mock/ycsbc")
//        .args(&[
//            "-db",
//            "mock",
//            "-threads",
//            "1",
//            "-P",
//            specfile.as_ref().to_str().unwrap(),
//        ])
//        .output()
//        .unwrap();
//}
