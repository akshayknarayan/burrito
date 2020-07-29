//! Chunnel wrapper types to negotiate between multiple implementations.

use super::{ChunnelConnection, ChunnelConnector, ChunnelListener, Either, Endedness, Scope};
use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;

/// 2-ary negotiator.
pub struct NegotiatorChunnel<T1, T2>(T1, T2);

impl<A, E, T1, C1, T2, C2, D> ChunnelListener for NegotiatorChunnel<T1, T2>
where
    T1: ChunnelListener<Addr = A, Error = E, Connection = C1>,
    T2: ChunnelListener<Addr = A, Error = E, Connection = C2>,
    C1: ChunnelConnection<Data = D> + 'static,
    C2: ChunnelConnection<Data = D> + 'static,
    E: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = Either<T1::Connection, T2::Connection>;
    type Error = E;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn listen(&mut self, a: Self::Addr) -> Self::Future {
        let use_t1 = match (T1::scope(), T2::scope()) {
            (Scope::Application, _) => true,
            (_, Scope::Application) => false,
            (Scope::Host, _) => true,
            (_, Scope::Host) => false,
            (Scope::Local, _) => true,
            (_, Scope::Local) => false,
            (Scope::Global, _) => true,
        };

        use futures_util::TryStreamExt;

        if use_t1 {
            let fut = self.0.listen(a);
            Box::pin(async move {
                Ok::<_, E>(Box::pin(fut.await?.map_ok(|c| Either::Left(c)))
                    as Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                + Send
                                + 'static,
                        >,
                    >)
            }) as _
        } else {
            let fut = self.1.listen(a);
            Box::pin(async move {
                Ok::<_, E>(Box::pin(fut.await?.map_ok(|c| Either::Right(c)))
                    as Pin<
                        Box<
                            dyn Stream<Item = Result<Self::Connection, Self::Error>>
                                + Send
                                + 'static,
                        >,
                    >)
            }) as _
        }
    }

    fn scope() -> Scope {
        unimplemented!()
    }
    fn endedness() -> Endedness {
        unimplemented!()
    }
    fn implementation_priority() -> usize {
        unimplemented!()
    }
}

impl<A, E, T1, C1, T2, C2, D> ChunnelConnector for NegotiatorChunnel<T1, T2>
where
    T1: ChunnelConnector<Addr = A, Error = E, Connection = C1>,
    T2: ChunnelConnector<Addr = A, Error = E, Connection = C2>,
    C1: ChunnelConnection<Data = D> + 'static,
    C2: ChunnelConnection<Data = D> + 'static,
    E: Send + Sync + 'static,
{
    type Addr = A;
    type Connection = Either<T1::Connection, T2::Connection>;
    type Error = E;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn connect(&mut self, a: Self::Addr) -> Self::Future {
        let use_t1 = match (T1::scope(), T2::scope()) {
            (Scope::Application, _) => true,
            (_, Scope::Application) => false,
            (Scope::Host, _) => true,
            (_, Scope::Host) => false,
            (Scope::Local, _) => true,
            (_, Scope::Local) => false,
            (Scope::Global, _) => true,
        };

        if use_t1 {
            let fut = self.0.connect(a);
            Box::pin(async move { Ok(Either::Left(fut.await?)) }) as _
        } else {
            let fut = self.1.connect(a);
            Box::pin(async move { Ok(Either::Right(fut.await?)) }) as _
        }
    }

    fn scope() -> Scope {
        unimplemented!()
    }
    fn endedness() -> Endedness {
        unimplemented!()
    }
    fn implementation_priority() -> usize {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use crate::{ChunnelConnector, ChunnelListener, Endedness, Scope};

    macro_rules! test_scope_impl {
        ($name:ident,$scope:expr) => {
            struct $name<C>(C);

            impl<C> ChunnelConnector for $name<C>
            where
                C: ChunnelConnector + Send + Sync + 'static,
            {
                type Addr = C::Addr;
                type Connection = C::Connection;
                type Future = C::Future;
                type Error = C::Error;

                fn connect(&mut self, a: Self::Addr) -> Self::Future {
                    self.0.connect(a)
                }

                fn scope() -> Scope {
                    $scope
                }
                fn endedness() -> Endedness {
                    C::endedness()
                }
                fn implementation_priority() -> usize {
                    C::implementation_priority()
                }
            }

            impl<C> ChunnelListener for $name<C>
            where
                C: ChunnelListener + Send + Sync + 'static,
            {
                type Addr = C::Addr;
                type Connection = C::Connection;
                type Future = C::Future;
                type Stream = C::Stream;
                type Error = C::Error;

                fn listen(&mut self, a: Self::Addr) -> Self::Future {
                    self.0.listen(a)
                }

                fn scope() -> Scope {
                    $scope
                }
                fn endedness() -> Endedness {
                    C::endedness()
                }
                fn implementation_priority() -> usize {
                    C::implementation_priority()
                }
            }
        };
    }

    test_scope_impl!(ImplA, Scope::Host);
    test_scope_impl!(ImplB, Scope::Local);

    use super::NegotiatorChunnel;
    use crate::chan_transport::RendezvousChannel;
    use crate::util::{Never, OptionUnwrap};
    use crate::ChunnelConnection;
    use futures_util::TryStreamExt;
    use tracing::info;
    use tracing_futures::Instrument;

    #[test]
    fn negotiate() {
        let _guard = tracing_subscriber::fmt::try_init();
        color_eyre::install().unwrap();

        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        rt.block_on(
            async move {
                let (srv, cln) = RendezvousChannel::new(10).split();
                let mut srv = OptionUnwrap::from(srv);
                let (s, r) = tokio::sync::oneshot::channel();

                tokio::spawn(
                    async move {
                        let st = srv.listen(3u8).await.unwrap();
                        s.send(()).unwrap();
                        st.try_for_each_concurrent(None, |cn| async move {
                            let m = cn.recv().await?;
                            cn.send(m).await?;
                            Ok::<_, eyre::Report>(())
                        })
                        .await
                    }
                    .instrument(tracing::debug_span!("server")),
                );

                let mut cln = NegotiatorChunnel(ImplA(cln.clone()), ImplB(Never::from(cln)));
                let _: () = r.await.unwrap();
                info!("connecting client");
                let cn = cln
                    .connect(3u8)
                    .instrument(tracing::debug_span!("connect"))
                    .await
                    .unwrap();

                cn.send(vec![1u8; 8])
                    .instrument(tracing::debug_span!("send"))
                    .await
                    .unwrap();
                let d = cn
                    .recv()
                    .instrument(tracing::debug_span!("recv"))
                    .await
                    .unwrap();
                assert_eq!(d, vec![1u8; 8]);
            }
            .instrument(tracing::debug_span!("negotiate")),
        );
    }
}
