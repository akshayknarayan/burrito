// before splits:
// CxList<A, CxList<B, CxList<C, CxNil>>>.connect(Base) -> CConn<BConn< AConn<Base>>>
//
// with splits:
// CxList<A, CxList<Split, CxList<B, CxList<Split, CxList<C, CxNil>>>>>.connect(Base)
// -> [SplitCnTop<A<Base>>, SplitCnTop<BConn<SplitCnBottom>>, CConn<SplitCnBottom>] (3 threads)
//            ^----------channel--------------------^
//                                 ^---------------channel-----------------^
// how?
// - return only CConn<SplitCnBottom> to application in impl Chunnel
// - SplitCn has the rest stuffed inside it (somehow?)
// - SplitCn top is added to the other two to drive threads
/// Split the Chunnel stack into two sub-stacks connected by a channel pair.
///
/// The lower-level chunnel stack will be spawned off to run concurrently, and the higher-level one
/// will be returned for either the application or to be split off again.
pub struct Split;

impl Split {
    fn pair<S, D>(inner: S) -> SplitCnBottom<D, SplitCnTop<S, D>> {
        // 100 is arbitrary, but it should *not* be unbounded because otherwise the bottom layer
        // might overwhelm a slower upper layer.
        let (up_path_sender, up_path_receiver) = flume::bounded(100);
        let (down_path_sender, down_path_receiver) = flume::bounded(100);
        let top = SplitCnTop {
            cn: inner,
            channel: SplitConn {
                send: down_path_sender,
                recv: up_path_receiver,
            },
        };

        SplitCnBottom {
            channel: SplitConn {
                send: up_path_sender,
                recv: down_path_receiver,
            },
            split_starter: Arc::new(StdMutex::new(Some(top))),
        }
    }
}

pub struct SplitConn<T> {
    recv: flume::Receiver<T>,
    send: flume::Sender<T>,
}

pub trait Start {
    fn start(self) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send>>;
}

pub struct SplitCnTop<InC, D> {
    channel: SplitConn<D>,
    cn: InC,
}

impl<InC, D> Start for SplitCnTop<InC, D>
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    fn start(self) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send>> {
        // drive the toplevel channels (should be spawned)
        // anything we get on recv we should call cn.send
        // anything cn.recv() gives we pass to send
        use futures_util::future::Either;
        Box::pin(async move {
            async fn run<InC, D>(channel: SplitConn<D>, cn: InC) -> Result<(), Report>
            where
                InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
                D: Send + Sync + 'static,
            {
                let mut chan_recv_fut = None;
                let mut conn_recv_fut = None;
                loop {
                    if chan_recv_fut.is_none() {
                        chan_recv_fut = Some(channel.recv.recv_async());
                    }
                    if conn_recv_fut.is_none() {
                        conn_recv_fut = Some(cn.recv());
                    }
                    match futures_util::future::select(
                        chan_recv_fut.take().unwrap(),
                        conn_recv_fut.take().unwrap(),
                    )
                    .await
                    {
                        Either::Left((to_send_on_conn, leftover_conn_recv_fut)) => {
                            conn_recv_fut = Some(leftover_conn_recv_fut);
                            cn.send(to_send_on_conn?).await?;
                        }
                        Either::Right((to_send_on_chan, leftover_chan_recv_fut)) => {
                            chan_recv_fut = Some(leftover_chan_recv_fut);
                            channel.send.send_async(to_send_on_chan?).await?;
                        }
                    }
                }
            }

            if let Err(err) = run(self.channel, self.cn).await {
                tracing::warn!(?err, "SplitCnTop loop errored");
                Ok(())
            } else {
                unreachable!()
            }
        })
    }
}

pub struct SplitCnBottom<D, S> {
    channel: SplitConn<D>,
    // initialized with a Some(S). when spawned, this is taken out.
    split_starter: Arc<StdMutex<Option<S>>>,
}

impl<D, S> ChunnelConnection for SplitCnBottom<D, S>
where
    S: Start + Send + 'static,
    D: Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        d: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        let starter = self.split_starter.lock().unwrap().take();
        let sender = self.channel.send.clone();
        Box::pin(async move {
            if let Some(top) = starter {
                tokio::spawn(top.start());
            }

            Ok(sender.send_async(d).await?)
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let starter = self.split_starter.lock().unwrap().take();
        let recvr = self.channel.recv.clone();
        Box::pin(async move {
            if let Some(top) = starter {
                tokio::spawn(top.start());
            }

            Ok(recvr.recv_async().await?)
        })
    }
}

// CxList<A, CxList<Split, CxList<B, CxNil>>>.connect(base)
// -> [SplitCnTop<AConn<Base>>, BConn<SplitCnBottom>]
//            ^--------------------------^
//
// sequence of connect_wrap calls:
// 1. A.connect(base) -> AConn<Base>
// 2. Split.connect(AConn<Base>) -> **SplitCnBottom** + <spawn SplitCnTop<AConn<Base>>>
// 3. B.connect(SplitCnBottom) -> BConn<SplitCnBottom>
impl<InC, D> Chunnel<InC> for Split
//                ^^^ InC is AConn<Base>
where
    InC: ChunnelConnection<Data = D> + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = SplitCnBottom<D, SplitCnTop<InC, D>>;
    //                ^^^^^^^^^^^^^ InC, or AConn<Base>, will be spawned off. We return a
    //                SplitCnBottom to connect the pieces.
    type Error = std::convert::Infallible;

    fn connect_wrap(&mut self, inner: InC) -> Self::Future {
        // 1. make SplitCnTop<InC> (for spawning)
        // 2. connect a SplitCnBottom to that SplitCnTop
        // 3. return the SplitCnBottom.
        //
        // when .send()/.recv() happen on the `SplitCnBottom`, it knows it must spawn off its
        // `SplitCnTop` to make progress (and then it can wait on its channels).
        let bottom = Split::pair(inner);
        ready(Ok(bottom))
    }
}

#[cfg(test)]
mod t {}
