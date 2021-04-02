use bertha::{
    bincode::{Base64Chunnel, SerializeChunnelProject},
    negotiate::{negotiate_rendezvous, Select},
    ChunnelConnection, CxList,
};
use color_eyre::eyre::{eyre, Report, WrapErr};
use queue_steer::bin_help::{dump_results, Mode, Msg, RecvdMsg};
use queue_steer::{Ordered, OrderedSqsChunnelWrap, SetGroup, SqsChunnelWrap};
use redis_basechunnel::RedisBase;
use sqs::{SqsAddr, SqsChunnel, SqsClient};
use std::fmt::Debug;
use std::iter::once;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, info_span, instrument};
use tracing_error::ErrorLayer;
use tracing_futures::Instrument;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    num_reqs: usize,
    #[structopt(short, long)]
    redis: String,
    #[structopt(short, long)]
    inter_request_ms: u64,
    #[structopt(short, long)]
    out_file: std::path::PathBuf,

    #[structopt(long)]
    aws_access_key_id: String,
    #[structopt(long)]
    aws_secret_access_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;
    let subscriber = tracing_subscriber::registry();
    let subscriber = subscriber
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(ErrorLayer::default());
    let d = tracing::Dispatch::new(subscriber);
    d.init();
    let Opt {
        redis,
        num_reqs,
        inter_request_ms,
        out_file,
        aws_access_key_id,
        aws_secret_access_key,
    } = Opt::from_args();
    let mode = Mode::Ordered {
        num_groups: Some(5),
    };
    info!(?num_reqs, ?inter_request_ms, "starting");
    let sqs_client = sqs::sqs_client_from_creds(aws_access_key_id, aws_secret_access_key)?;
    let msgs = run_exp(
        num_reqs,
        Duration::from_millis(inter_request_ms),
        sqs_client,
        redis,
    )
    .await?;

    let mut latencies: Vec<_> = msgs.iter().map(|m| m.elapsed).collect();
    latencies.sort_unstable();
    let len = latencies.len() as f64;
    let quantile_idxs = [0.25, 0.5, 0.75, 0.95];
    let quantiles: Vec<_> = quantile_idxs
        .iter()
        .map(|q| (len * q) as usize)
        .map(|i| latencies[i])
        .collect();

    info!(
        ?num_reqs,
        p25 = ?quantiles[0],
        p50 = ?quantiles[1],
        p75 = ?quantiles[2],
        p95 = ?quantiles[3],
        "done",
    );

    dump_results(
        out_file,
        msgs,
        Duration::from_millis(0),
        mode,
        inter_request_ms,
        "aws",
    )?;
    Ok(())
}

#[instrument(skip(sqs_client, redis))]
async fn run_exp(
    num_reqs: usize,
    inter_request: Duration,
    sqs_client: SqsClient,
    redis: String,
) -> Result<Vec<RecvdMsg>, Report> {
    let redis_base = RedisBase::new(&redis).await?;
    let be_queue: String = gen_id(false);
    let mut fifo_queue = be_queue.clone();
    fifo_queue.push_str(".fifo");
    let be_queue = sqs::make_be_queue(&sqs_client, be_queue.clone()).await?;
    let fifo_queue = sqs::make_fifo_queue(&sqs_client, fifo_queue.clone()).await?;

    debug!(?be_queue, "AWS queue");
    let start = std::time::Instant::now();

    let (s, mut r) = mpsc::unbounded_channel();
    let (recv_err_s, mut recv_err_r1) = oneshot::channel::<Report>();
    // start consumer 1.
    tokio::spawn(
        receiver(
            sqs_client.clone(),
            redis.clone(),
            be_queue.clone(),
            start,
            s.clone(),
            recv_err_s,
        )
        .instrument(info_span!("consumer1")),
    );

    // producer
    let cn: SqsChunnelWrap = SqsChunnel::new(sqs_client.clone(), once(be_queue.as_str())).into();
    let st = Select::from((
        CxList::from(Ordered::default())
            .wrap(SerializeChunnelProject::default())
            .wrap(Base64Chunnel::default())
            .wrap(cn.clone()),
        CxList::from(SerializeChunnelProject::<queue_steer::bin_help::Msg>::default())
            .wrap(Base64Chunnel::default())
            .wrap(OrderedSqsChunnelWrap::from(cn.clone())),
    ))
    .into();
    let cn = negotiate_rendezvous(st, redis_base, be_queue.clone()).await?;
    let addr = SqsAddr {
        queue_id: be_queue.clone(),
        group: None,
    };

    // use 5 groups.
    let groups: Vec<_> = (0..4).map(|g| g.to_string()).collect();
    // one group (total
    //let groups = vec!["1".to_string()];

    #[instrument(skip(cn, groups, start))]
    async fn do_reqs(
        addr: SqsAddr,
        cn: &impl ChunnelConnection<Data = (SqsAddr, Msg)>,
        groups: &[String],
        start: std::time::Instant,
        inter_request: Duration,
        num_reqs: usize,
        req_num_offset: usize,
    ) -> Result<(), Report> {
        for req_num in req_num_offset..req_num_offset + num_reqs {
            let mut a = addr.clone();
            let grp_num = req_num % groups.len();
            a.set_group(groups[grp_num].clone());
            debug!(?grp_num, ?req_num, "sending");
            cn.send((
                a,
                Msg {
                    send_time: start.elapsed(),
                    req_num,
                },
            ))
            .await
            .wrap_err("send error")?;
            tokio::time::sleep(inter_request).await;
        }

        Ok(())
    }

    let mut recvs = vec![];
    // send num_reqs messages with 1 consumer, then add a second consumer for another num_reqs messages.
    info!("phase 1: 1 producer, 1 consumer");
    tokio::select! {
        res = do_reqs(addr.clone(), &cn, &groups, start, inter_request, num_reqs, 0) => {
            res?;
        }
        recv_err = &mut recv_err_r1 => {
            recv_err?;
        }
    };

    info!("phase 1 done sending");
    for i in 0..num_reqs {
        let m = r
            .recv()
            .await
            .ok_or_else(|| eyre!("{:?} < {} msgs received", i, num_reqs))?;
        recvs.push(m);
    }

    info!("phase 2: 1 producer, 2 consumers");
    let (recv_err_s, mut recv_err_r2) = oneshot::channel::<Report>();
    tokio::spawn(
        receiver(
            sqs_client.clone(),
            redis.clone(),
            be_queue.clone(),
            start,
            s.clone(),
            recv_err_s,
        )
        .instrument(info_span!("consumer2")),
    );

    tokio::select! {
        res = do_reqs(addr.clone(), &cn, &groups, start, inter_request, num_reqs, num_reqs) => {
            res.wrap_err("phase 2 sends failed")?;
        }
        recv_err = &mut recv_err_r1 => {
            recv_err.wrap_err("receiver 1 failed")?;
        }
        recv_err = &mut recv_err_r2 => {
            recv_err.wrap_err("receiver 2 failed")?;
        }
    };

    info!("phase 2 done sending");
    for i in 0..num_reqs {
        let m = r
            .recv()
            .await
            .ok_or_else(|| eyre!("{:?} < {} msgs received", i, num_reqs))?;
        recvs.push(m);
    }

    sqs::delete_queue(&sqs_client, fifo_queue).await?;
    sqs::delete_queue(&sqs_client, be_queue).await?;
    Ok(recvs)
}

#[instrument(skip(sqs_client, recvds, err, redis_addr, addr, start))]
async fn receiver(
    sqs_client: SqsClient,
    redis_addr: String,
    addr: String,
    start: std::time::Instant,
    recvds: mpsc::UnboundedSender<RecvdMsg>,
    err: oneshot::Sender<Report>,
) {
    async fn run(
        sqs_client: SqsClient,
        redis_addr: String,
        addr: String,
        start: std::time::Instant,
        recvds: mpsc::UnboundedSender<RecvdMsg>,
    ) -> Result<(), Report> {
        let redis_base = RedisBase::new(&redis_addr).await?;
        let cn: SqsChunnelWrap = SqsChunnel::new(sqs_client, once(addr.as_str())).into();
        let st = Select::from((
            CxList::from(Ordered::default())
                .wrap(SerializeChunnelProject::default())
                .wrap(Base64Chunnel::default())
                .wrap(cn.clone()),
            CxList::from(SerializeChunnelProject::<queue_steer::bin_help::Msg>::default())
                .wrap(Base64Chunnel::default())
                .wrap(OrderedSqsChunnelWrap::from(cn.clone())),
        ))
        .into();
        let cn = negotiate_rendezvous(st, redis_base, addr.clone()).await?;
        loop {
            let (_, r) = cn.recv().await?;
            let m = RecvdMsg::from_start(start, r);
            info!(?m, "got msg");
            recvds.send(m)?;
        }
    }

    info!("starting");
    if let Err(e) = run(sqs_client, redis_addr, addr, start, recvds).await {
        err.send(e).expect("send error on oneshot");
    }
}

fn gen_id(fifo: bool) -> String {
    use rand::Rng;
    let rng = rand::thread_rng();
    let start = "bertha-".chars().chain(
        rng.sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .flat_map(char::to_lowercase),
    );
    if fifo {
        start.chain(".fifo".chars()).collect()
    } else {
        start.collect()
    }
}
