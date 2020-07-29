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
