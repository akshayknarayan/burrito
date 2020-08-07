//! The `Either` type implements traits where both variants implement the trait with the same
//! output type.

use super::ChunnelConnection;
use futures_util::stream::Stream;
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
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + 'static>> {
        match self {
            Either::Left(a) => a.send(data),
            Either::Right(b) => b.send(data),
        }
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + 'static>> {
        match self {
            Either::Left(a) => a.recv(),
            Either::Right(b) => b.recv(),
        }
    }
}
