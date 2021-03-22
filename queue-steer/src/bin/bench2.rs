//! Measure message throughput, latency, and ordered-ness for an experiment [`Mode`].
//!
//! This program is both the producer and the consumer (in different threads).
//! The chunnel stacks it benchmarks should support the (addr, data) = (QueueAddr, String)
//! datatype.

use az_queues::{AsStorageClient, AzStorageQueueChunnel};
use bertha::{
    bincode::{Base64Chunnel, SerializeChunnelProject},
    Chunnel, CxList,
};
use color_eyre::Report;
use gcp_pubsub::{PubSubAddr, PubSubChunnel};
use queue_steer::bin_help::{
    do_atmostonce_exp, do_best_effort_exp, do_ordered_groups_exp, dump_results, Mode, RecvdMsg,
};
use queue_steer::{AtMostOnce, Ordered, OrderedSqsChunnelWrap, QueueAddr, SqsChunnelWrap};
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
    #[structopt(short, long)]
    num_reqs: usize,
    #[structopt(short, long)]
    queue: Option<String>,
    #[structopt(short, long)]
    inter_request_ms: u64,
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
        inter_request_ms,
        out_file,
        queue,
        provider,
    } = Opt::from_args();
    let prov = provider.provider().to_owned();
    info!(?mode, ?num_reqs, ?inter_request_ms, provider = ?provider, "starting");
    let (msgs, elapsed) = provider
        .run_exp(queue, mode, num_reqs, inter_request_ms)
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
        ?elapsed,
        ?num_reqs,
        p25 = ?quantiles[0],
        p50 = ?quantiles[1],
        p75 = ?quantiles[2],
        p95 = ?quantiles[3],
        "done",
    );

    dump_results(out_file, msgs, elapsed, mode, inter_request_ms, &prov)?;
    Ok(())
}

impl Provider {
    pub fn provider(&self) -> &str {
        match self {
            Provider::Aws { .. } => "aws",
            Provider::Azure { .. } => "azure",
            Provider::Gcp { .. } => "gcp",
        }
    }

    pub async fn run_exp(
        self,
        queue: Option<String>,
        mode: Mode,
        num_reqs: usize,
        inter_request_ms: u64,
    ) -> Result<(Vec<RecvdMsg>, Duration), Report> {
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

        macro_rules! do_exp {
            ($mode: expr, $cn: expr, $addr: expr, $num_reqs: expr, $inter_request_ms: expr) => {{
                use bertha::util::NeverCn;
                match mode {
                    Mode::BestEffort => {
                        let mut stack = CxList::from(SerializeChunnelProject::default())
                            .wrap(Base64Chunnel::default())
                            .wrap($cn);
                        let ch = stack.connect_wrap(NeverCn::default()).await?;
                        do_best_effort_exp(ch, $addr.clone(), $num_reqs, $inter_request_ms).await?
                    }
                    Mode::Ordered { num_groups: None } => {
                        // at most once.
                        let mut stack = CxList::from(AtMostOnce::default())
                            .wrap(SerializeChunnelProject::default())
                            .wrap(Base64Chunnel::default())
                            .wrap($cn);
                        let ch = stack.connect_wrap(NeverCn::default()).await?;
                        do_atmostonce_exp(ch, $addr.clone(), $num_reqs, $inter_request_ms).await?
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
                        do_ordered_groups_exp(ch, $addr.clone(), $num_reqs, n, $inter_request_ms)
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
                    // what if we want to optimize to a fifo queue?
                    let queue = sqs::make_be_queue(&sqs_client, queue.clone()).await?;
                    (queue.clone(), Some((queue, sqs_client.clone()).into()))
                } else {
                    (queue, None)
                };

                debug!(?queue, "AWS queue");
                let cn: SqsChunnelWrap = SqsChunnel::new(sqs_client, once(queue.as_str())).into();
                let addr = SqsAddr {
                    queue_id: queue,
                    group: None,
                };
                let (msgs, elapsed) = do_exp!(mode, cn, addr, num_reqs, inter_request_ms);
                if let Some(gen) = generated {
                    gen.cleanup().await?;
                }
                Ok((msgs, elapsed))
            }
            Azure {
                az_account_name,
                az_key,
            } => {
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

                debug!(?queue, "Azure queue");
                unimplemented!()
                //let cn = AzStorageQueueChunnel::new(az_client, once(queue.as_str()));
                //let (msgs, elapsed) = do_exp!(mode, cn, queue, num_reqs, inter_request_ms)?;
                //if let Some(gen) = generated {
                //    gen.cleanup().await?;
                //}
                //Ok((msgs, elapsed))
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
                    topic_id: queue,
                    group: None,
                };
                unimplemented!()
                //let cn = PubSubChunnel::new(gcp_client, once(queue.as_str())).await?;
                //let (msgs, elapsed) = do_exp!(mode, cn, addr, num_reqs, inter_request_ms)?;
                //if let Some(gen) = generated {
                //    gen.cleanup().await?;
                //}
                //Ok((msgs, elapsed))
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
    use sqs::OrderedSqsChunnel;

    // input
    let stack =
        CxList::from(AtMostOnce::default()) // or OrderedChunnelProj
            .wrap(SerializeChunnelProject::<(u32, queue_steer::bin_help::Msg)>::default())
            .wrap(Base64Chunnel::default())
            .wrap(SqsChunnelWrap::from(s.clone()));

    use bertha::CxListReverse;

    // if AtMostOnce |> .* |> SqsChunnelWrap
    // replace with .* |> OrderedSqsChunnelWrap
    let opt = stack.rev().transform(Default::default());
    let opt = opt.rev();

    let t = opt.connect_wrap(NeverCn::default());

    // end state if things match up.
    let end = CxList::from(SerializeChunnelProject::<queue_steer::bin_help::Msg>::default())
        .wrap(Base64Chunnel::default())
        .wrap(OrderedSqsChunnelWrap::from(OrderedSqsChunnel::from(
            s.clone(),
        )));
}

use bertha::{either::Either, CxNil};
trait OrdOpt<Ctx> {
    type Output;
    fn transform(self, ctx: Ctx) -> Self::Output;
}

struct OrdStackCtx<T1, T2> {
    matched: bool,
    if_matched: T1,
    non_match: T2,
}

impl Default for OrdStackCtx<CxNil, CxNil> {
    fn default() -> Self {
        OrdStackCtx {
            matched: false,
            if_matched: Default::default(),
            non_match: Default::default(),
        }
    }
}

// base case
impl<T1, T2> OrdOpt<OrdStackCtx<T1, T2>> for CxList<SqsChunnelWrap, CxNil>
where
    T1: bertha::AppendBack<OrderedSqsChunnelWrap>,
    T2: bertha::AppendBack<SqsChunnelWrap>,
{
    type Output = Either<T1::Appended, T2::Appended>;

    fn transform(self, c: OrdStackCtx<T1, T2>) -> Self::Output {
        if c.matched {
            Either::Left(c.if_matched.append(self.head.into()))
        } else {
            Either::Right(c.non_match.append(self.head))
        }
    }
}

use bertha::Negotiate;
// generic case
impl<T1, T2, Head, Tail> OrdOpt<OrdStackCtx<T1, T2>> for CxList<Head, Tail>
where
    Head: Negotiate + Clone,
    Tail: OrdOpt<OrdStackCtx<T1::Appended, T2::Appended>> + OrdOpt<OrdStackCtx<T1, T2::Appended>>,
    T1: bertha::AppendBack<Head>,
    T2: bertha::AppendBack<Head>,
{
    type Output = Either<
        <Tail as OrdOpt<OrdStackCtx<T1::Appended, T2::Appended>>>::Output,
        <Tail as OrdOpt<OrdStackCtx<T1, T2::Appended>>>::Output,
    >;

    fn transform(
        self,
        OrdStackCtx {
            matched,
            if_matched,
            non_match,
        }: OrdStackCtx<T1, T2>,
    ) -> Self::Output {
        if Head::guid() == AtMostOnce::guid() {
            if matched {
                panic!("matched twice");
            }
            Either::Right(self.tail.transform(OrdStackCtx {
                matched: true,
                if_matched,
                non_match: non_match.append(self.head.clone()),
            }))
        } else {
            Either::Left(self.tail.transform(OrdStackCtx {
                matched,
                if_matched: if_matched.append(self.head.clone()),
                non_match: non_match.append(self.head.clone()),
            }))
        }
    }
}
