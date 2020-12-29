//! The `Either` type implements traits where both variants implement the trait with the same
//! output type.

use super::{ChunnelConnection, Client};
use color_eyre::eyre::Report;
use futures_util::{future::FutureExt, stream::Stream};
use std::future::Future;
use std::pin::Pin;

#[pin_project::pin_project(project = EitherProj)]
#[derive(Debug, Clone, Copy)]
pub enum Either<A, B> {
    Left(#[pin] A),
    Right(#[pin] B),
}

impl<A, B> Either<A, B> {
    pub fn flip(self) -> Either<B, A> {
        match self {
            Either::Left(a) => Either::Right(a),
            Either::Right(a) => Either::Left(a),
        }
    }
}

impl<A, B, O> Future for Either<A, B>
where
    A: Future<Output = O>,
    B: Future<Output = O>,
{
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        match self.project() {
            EitherProj::Left(a) => a.poll(cx),
            EitherProj::Right(a) => a.poll(cx),
        }
    }
}

impl<A, B, I> Stream for Either<A, B>
where
    A: Stream<Item = I>,
    B: Stream<Item = I>,
{
    type Item = I;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.project() {
            EitherProj::Left(a) => a.poll_next(cx),
            EitherProj::Right(a) => a.poll_next(cx),
        }
    }
}

impl<A, B, D> ChunnelConnection for Either<A, B>
where
    A: ChunnelConnection<Data = D>,
    B: ChunnelConnection<Data = D>,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        match self {
            Either::Left(a) => a.send(data),
            Either::Right(b) => b.send(data),
        }
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        match self {
            Either::Left(a) => a.recv(),
            Either::Right(b) => b.recv(),
        }
    }
}

impl<A, B, C, Din, Dout> Client<C> for Either<A, B>
where
    Din: Send + Sync + 'static,
    Dout: Send + Sync + 'static,
    C: ChunnelConnection<Data = Din>,
    A: Client<C> + Send + 'static,
    B: Client<C> + Send + 'static,
    <A as Client<C>>::Connection: ChunnelConnection<Data = Dout>,
    <B as Client<C>>::Connection: ChunnelConnection<Data = Dout>,
    <A as Client<C>>::Error: Into<Report> + Send + Sync + 'static,
    <B as Client<C>>::Error: Into<Report> + Send + Sync + 'static,
{
    // have to box - the Either strategy doesn't work.
    // future::Map type that goes in the Either includes the closures, which are different
    // could box the clousures, but why bother at that point
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Connection = Either<A::Connection, B::Connection>;
    type Error = Report;

    fn connect_wrap(&mut self, inner: C) -> Self::Future {
        match self {
            Either::Left(a) => {
                let fut = a
                    .connect_wrap(inner)
                    .map(|out| Ok(Either::Left(out.map_err(Into::into)?)));
                Box::pin(fut) as _
            }
            Either::Right(b) => {
                let fut = b
                    .connect_wrap(inner)
                    .map(|out| Ok(Either::Right(out.map_err(Into::into)?)));
                Box::pin(fut) as _
            }
        }
    }
}

impl<A, B, I> Iterator for Either<A, B>
where
    A: Iterator<Item = I>,
    B: Iterator<Item = I>,
{
    type Item = I;
    fn next(&mut self) -> Option<Self::Item> {
        use Either::*;
        match self {
            Left(i) => i.next(),
            Right(i) => i.next(),
        }
    }
}
