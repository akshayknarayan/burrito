//! Measure message throughput, latency, and ordered-ness for an experiment [`Mode`].
//!
//! This program is both the producer and the consumer (in different threads).
//! The chunnel stacks it benchmarks should support the (addr, data) = (impl SetGroup, String)
//! datatype.

use az_queues::{AsStorageClient, AzStorageQueueChunnel};
use bertha::{
    bincode::{Base64Chunnel, SerializeChunnelProject},
    Chunnel, CxList,
};
use color_eyre::{eyre::eyre, Report};
use gcp_pubsub::{PubSubAddr, PubSubChunnel};
use queue_steer::bin_help::{
    do_atmostonce_exp, do_best_effort_exp, do_ordered_groups_exp, dump_results, Mode, RecvdMsg,
};
use queue_steer::{
    AtMostOnce, AzQueueChunnelWrap, GcpPubSubWrap, KafkaChunnelWrap, Ordered, OrderedGcpPubSubWrap,
    OrderedSqsChunnelWrap, SqsChunnelWrap,
};
use sqs::{SqsAddr, SqsChunnel};
use std::fmt::Debug;
use std::iter::once;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tracing::{debug, info};
use tracing_error::ErrorLayer;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    mode: Mode,
    #[structopt(short = "n", long)]
    num_reqs: usize,
    #[structopt(short, long)]
    queue: Option<String>,
    #[structopt(short, long)]
    inter_request_ms: u64,
    #[structopt(short = "r", long)]
    num_receivers: usize,
    #[structopt(long)]
    service_mode: bool, // client-side or service-side impl.
    #[structopt(short, long)]
    out_file: std::path::PathBuf,

    #[structopt(subcommand)]
    provider: Provider,
}

#[derive(Debug, Clone, StructOpt)]
pub enum Provider {
    Aws {
        #[structopt(long)]
        aws_access_key_id: String,
        #[structopt(long)]
        aws_secret_access_key: String,
    },
    Azure {
        #[structopt(long)]
        az_account_name: String,
        #[structopt(long)]
        az_key: String,
    },
    Gcp {
        #[structopt(long)]
        gcp_key_file: std::path::PathBuf,
        #[structopt(long)]
        gcp_project_name: String,
    },
    Kafka {
        #[structopt(long)]
        addr: String,
    },
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
        mode,
        num_reqs,
        num_receivers,
        inter_request_ms,
        service_mode,
        out_file,
        queue,
        provider,
    } = Opt::from_args();
    let prov = provider.provider().to_owned();
    info!(?mode, ?num_reqs, ?inter_request_ms, provider = ?provider, "starting");
    let (msgs, elapsed) = provider
        .run_exp(
            queue,
            mode,
            service_mode,
            num_reqs,
            inter_request_ms,
            num_receivers,
        )
        .await?;

    if !msgs.is_empty() {
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
            ?elapsed,
            ?num_reqs,
            p25 = ?quantiles[0],
            p50 = ?quantiles[1],
            p75 = ?quantiles[2],
            p95 = ?quantiles[3],
            "done",
        );
    } else {
        info!("skipped",);
    }

    dump_results(out_file, msgs, elapsed, mode, inter_request_ms, &prov)?;
    Ok(())
}

impl Provider {
    pub fn provider(&self) -> &str {
        match self {
            Provider::Aws { .. } => "aws",
            Provider::Azure { .. } => "azure",
            Provider::Gcp { .. } => "gcp",
            Provider::Kafka { .. } => "kafka",
        }
    }

    pub async fn run_exp(
        self,
        queue: Option<String>,
        mode: Mode,
        service_mode: bool,
        num_reqs: usize,
        inter_request_ms: u64,
        num_receivers: usize,
    ) -> Result<(Vec<RecvdMsg>, Duration), Report> {
        if !service_mode && num_receivers != 1 {
            return Err(eyre!("Cannot have > 1 receiver with client mode"));
        }

        let mut generated = false;
        let queue: String = queue.unwrap_or_else(|| {
            generated = true;
            use rand::Rng;
            let rng = rand::thread_rng();
            "bertha-"
                .chars()
                .chain(
                    rng.sample_iter(&rand::distributions::Alphanumeric)
                        .take(10)
                        .flat_map(char::to_lowercase),
                )
                .collect()
        });

        macro_rules! do_exp_service {
            ($mode: expr, $cn: expr, $addr: expr, $num_reqs: expr, $inter_request_ms: expr, $num_receivers: expr) => {{
                use bertha::util::NeverCn;
                match mode {
                    Mode::BestEffort => {
                        info!("skipping service-side besteffort");
                        (vec![], Duration::from_millis(0))
                    }
                    Mode::Ordered { num_groups: None } => {
                        let mut stack = CxList::from(SerializeChunnelProject::default())
                            .wrap(Base64Chunnel::default())
                            .wrap($cn);
                        let ch = stack.connect_wrap(NeverCn::default()).await?;
                        do_atmostonce_exp(
                            ch,
                            $addr.clone(),
                            $num_reqs,
                            $inter_request_ms,
                            $num_receivers,
                        )
                        .await?
                    }
                    Mode::Ordered {
                        num_groups: Some(n),
                    } => {
                        let mut stack = CxList::from(SerializeChunnelProject::default())
                            .wrap(Base64Chunnel::default())
                            .wrap($cn);
                        let ch = stack.connect_wrap(NeverCn::default()).await?;
                        do_ordered_groups_exp(
                            ch,
                            $addr.clone(),
                            $num_reqs,
                            n,
                            $inter_request_ms,
                            $num_receivers,
                        )
                        .await?
                    }
                }
            }};
        }

        macro_rules! do_exp {
            ($mode: expr, $cn: expr, $addr: expr, $num_reqs: expr, $inter_request_ms: expr, $num_receivers: expr) => {{
                use bertha::util::NeverCn;
                match mode {
                    Mode::BestEffort => {
                        let mut stack = CxList::from(SerializeChunnelProject::default())
                            .wrap(Base64Chunnel::default())
                            .wrap($cn);
                        let ch = stack.connect_wrap(NeverCn::default()).await?;
                        do_best_effort_exp(
                            ch,
                            $addr.clone(),
                            $num_reqs,
                            $inter_request_ms,
                            $num_receivers,
                        )
                        .await?
                    }
                    Mode::Ordered { num_groups: None } => {
                        // at most once.
                        let mut stack = CxList::from(AtMostOnce::default())
                            .wrap(SerializeChunnelProject::default())
                            .wrap(Base64Chunnel::default())
                            .wrap($cn);
                        let ch = stack.connect_wrap(NeverCn::default()).await?;
                        do_atmostonce_exp(
                            ch,
                            $addr.clone(),
                            $num_reqs,
                            $inter_request_ms,
                            $num_receivers,
                        )
                        .await?
                    }
                    Mode::Ordered {
                        num_groups: Some(n),
                    } => {
                        // ordered, with potentially many groups
                        let mut stack = CxList::from(Ordered::default())
                            .wrap(SerializeChunnelProject::default())
                            .wrap(Base64Chunnel::default())
                            .wrap($cn);
                        let ch = stack.connect_wrap(NeverCn::default()).await?;
                        do_ordered_groups_exp(
                            ch,
                            $addr.clone(),
                            $num_reqs,
                            n,
                            $inter_request_ms,
                            $num_receivers,
                        )
                        .await?
                    }
                }
            }};
        }

        use Provider::*;
        match self {
            Aws {
                aws_access_key_id,
                aws_secret_access_key,
            } => {
                let sqs_client =
                    sqs::sqs_client_from_creds(aws_access_key_id, aws_secret_access_key)?;
                let (queue, generated): (_, Option<ProviderCleanup>) = if generated {
                    if service_mode {
                        let mut q = queue.clone();
                        q.push_str(".fifo");
                        let queue = sqs::make_fifo_queue(&sqs_client, q.clone()).await?;
                        (queue.clone(), Some((queue, sqs_client.clone()).into()))
                    } else {
                        let queue = sqs::make_be_queue(&sqs_client, queue.clone()).await?;
                        (queue.clone(), Some((queue, sqs_client.clone()).into()))
                    }
                } else {
                    (queue, None)
                };

                debug!(?queue, "AWS queue");
                let addr = SqsAddr {
                    queue_id: queue.clone(),
                    group: None,
                };
                let cn: SqsChunnelWrap = SqsChunnel::new(sqs_client, once(queue.as_str())).into();
                let (msgs, elapsed) = if service_mode {
                    let cn: OrderedSqsChunnelWrap = cn.into();
                    do_exp_service!(mode, cn, addr, num_reqs, inter_request_ms, num_receivers)
                } else {
                    do_exp!(mode, cn, addr, num_reqs, inter_request_ms, num_receivers)
                };

                if let Some(gen) = generated {
                    gen.cleanup().await?;
                }
                Ok((msgs, elapsed))
            }
            Azure {
                az_account_name,
                az_key,
            } => {
                if service_mode {
                    debug!("skipping service-mode Azure");
                    return Ok((vec![], Duration::from_millis(0)));
                }

                let az_client = az_queues::AzureAccountBuilder::default()
                    .with_name(az_account_name)
                    .with_key(az_key)
                    .finish()?;

                let generated: Option<ProviderCleanup> = if generated {
                    az_queues::make_queue(&az_client.as_storage_client(), queue.clone()).await?;
                    Some((queue.clone(), az_client.as_storage_client()).into())
                } else {
                    None
                };

                use queue_steer::{FakeSetGroup, FakeSetGroupAddr};
                debug!(?queue, "Azure queue");
                let cn: AzQueueChunnelWrap =
                    AzStorageQueueChunnel::new(az_client, once(queue.as_str())).into();
                let cn = CxList::from(FakeSetGroup::default()).wrap(cn);
                let addr: FakeSetGroupAddr = queue.into();
                let (msgs, elapsed) =
                    do_exp!(mode, cn, addr, num_reqs, inter_request_ms, num_receivers);
                if let Some(gen) = generated {
                    gen.cleanup().await?;
                }
                Ok((msgs, elapsed))
            }
            Kafka { addr } => {
                kafka::make_topic(&addr, &queue).await?;
                info!(?queue, ?addr, "Kafka queue");
                let ch = kafka::KafkaChunnel::new(&addr)?;
                ch.listen(&[&queue])?;
                let cn: KafkaChunnelWrap = ch.into();
                let c_addr = kafka::KafkaAddr {
                    topic_id: queue.clone(),
                    group: None,
                };

                let (msgs, elapsed) = if service_mode {
                    do_exp_service!(mode, cn, c_addr, num_reqs, inter_request_ms, num_receivers)
                } else {
                    do_exp!(mode, cn, c_addr, num_reqs, inter_request_ms, num_receivers)
                };

                kafka::delete_topic(&addr, &queue).await?;
                Ok((msgs, elapsed))
            }
            Gcp {
                gcp_key_file,
                gcp_project_name,
            } => {
                let mut gcp_client = gcp_pubsub::GcpCreds::default()
                    .with_creds_path(gcp_key_file)
                    .with_project_name(gcp_project_name)
                    .finish()
                    .await?;

                let (queue, generated): (_, Option<ProviderCleanup>) = if generated {
                    let queue = gcp_pubsub::make_topic(&mut gcp_client, queue.clone()).await?;
                    (queue.clone(), Some((queue, gcp_client.clone()).into()))
                } else {
                    (queue, None)
                };

                debug!(?queue, "GCP queue");
                let addr = PubSubAddr {
                    topic_id: queue.clone(),
                    group: None,
                };

                // todo figure out how to call .cleanup
                let cn: GcpPubSubWrap = PubSubChunnel::new(gcp_client, once(queue.as_str()))
                    .await?
                    .into();
                let (msgs, elapsed) = if service_mode {
                    let cn = OrderedGcpPubSubWrap::convert(cn).await?;
                    do_exp_service!(mode, cn, addr, num_reqs, inter_request_ms, num_receivers)
                } else {
                    do_exp!(mode, cn, addr, num_reqs, inter_request_ms, num_receivers)
                };

                if let Some(gen) = generated {
                    gen.cleanup().await?;
                }
                Ok((msgs, elapsed))
            }
        }
    }
}

struct ProviderCleanup {
    queue: String,
    inner: ProviderCleanupInner,
}

impl ProviderCleanup {
    async fn cleanup(self) -> Result<(), Report> {
        use ProviderCleanupInner::*;
        let queue = self.queue;
        debug!(?queue, provider = ?self.inner.provider(), "deleting queue");
        match self.inner {
            Aws(c) => sqs::delete_queue(&c, queue).await,
            Azure(c) => az_queues::delete_queue(&c, queue).await,
            Gcp(mut c) => gcp_pubsub::delete_topic(&mut c, queue).await,
        }
    }
}

impl From<(String, sqs::SqsClient)> for ProviderCleanup {
    fn from((queue, inner): (String, sqs::SqsClient)) -> Self {
        Self {
            queue,
            inner: ProviderCleanupInner::Aws(inner),
        }
    }
}
impl From<(String, Arc<az_queues::StorageClient>)> for ProviderCleanup {
    fn from((queue, inner): (String, Arc<az_queues::StorageClient>)) -> Self {
        Self {
            queue,
            inner: ProviderCleanupInner::Azure(inner),
        }
    }
}
impl From<(String, gcp_pubsub::GcpClient)> for ProviderCleanup {
    fn from((queue, inner): (String, gcp_pubsub::GcpClient)) -> Self {
        Self {
            queue,
            inner: ProviderCleanupInner::Gcp(inner),
        }
    }
}

enum ProviderCleanupInner {
    Aws(sqs::SqsClient),
    Azure(Arc<az_queues::StorageClient>),
    Gcp(gcp_pubsub::GcpClient),
}

impl ProviderCleanupInner {
    fn provider(&self) -> &str {
        match self {
            ProviderCleanupInner::Aws(_) => "aws",
            ProviderCleanupInner::Azure(_) => "azure",
            ProviderCleanupInner::Gcp(_) => "gcp",
        }
    }
}

fn sample_opt(s: SqsChunnel) {
    // input
    let stack =
        CxList::from(AtMostOnce::default()) // or OrderedChunnelProj
            .wrap(SerializeChunnelProject::<(u32, queue_steer::bin_help::Msg)>::default())
            .wrap(Base64Chunnel::default())
            .wrap(SqsChunnelWrap::from(s));

    // if AtMostOnce |> .* |> SqsChunnelWrap
    // replace with .* |> OrderedSqsChunnelWrap
    let mut opt = stack.transform();

    let _t = opt.connect_wrap(bertha::util::NeverCn::default());

    // end state if things match up.
    //let end = CxList::from(SerializeChunnelProject::<queue_steer::bin_help::Msg>::default())
    //    .wrap(Base64Chunnel::default())
    //    .wrap(OrderedSqsChunnelWrap::from(OrderedSqsChunnel::from(
    //        s.clone(),
    //    )));
}

use bertha::CxNil;

trait OrdOpt {
    type Opt;
    fn transform(self) -> Self::Opt;
}

impl<T> OrdOpt for CxList<SqsChunnelWrap, CxList<T, CxList<AtMostOnce, CxNil>>> {
    type Opt = CxList<OrderedSqsChunnelWrap, CxList<T, CxNil>>;

    fn transform(self) -> Self::Opt {
        let CxList {
            head: sqs,
            tail:
                CxList {
                    head: head_t,
                    tail:
                        CxList {
                            head: _head_amo,
                            tail: CxNil,
                        },
                },
        } = self;
        CxList {
            head: sqs.into(),
            tail: CxList {
                head: head_t,
                tail: CxNil,
            },
        }
    }
}

impl<T1, T2> OrdOpt for CxList<SqsChunnelWrap, CxList<T1, CxList<T2, CxList<AtMostOnce, CxNil>>>> {
    type Opt = CxList<OrderedSqsChunnelWrap, CxList<T1, CxList<T2, CxNil>>>;

    fn transform(self) -> Self::Opt {
        let CxList {
            head: sqs,
            tail:
                CxList {
                    head: head_t1,
                    tail:
                        CxList {
                            head: head_t2,
                            tail:
                                CxList {
                                    head: _head_amo,
                                    tail: CxNil,
                                },
                        },
                },
        } = self;
        CxList {
            head: sqs.into(),
            tail: CxList {
                head: head_t1,
                tail: CxList {
                    head: head_t2,
                    tail: CxNil,
                },
            },
        }
    }
}
