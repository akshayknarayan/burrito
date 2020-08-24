//! The `Either` type implements traits where both variants implement the trait with the same
//! output type.

use super::{ChunnelConnection, Serve};
use color_eyre::eyre::Report;
use futures_util::stream::{Stream, StreamExt};
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

impl<A, B, I, C, E, D> Serve<I> for Either<A, B>
where
    I: Stream<Item = Result<C, E>> + Send + 'static,
    C: ChunnelConnection<Data = D>,
    E: Into<Report> + Send + Sync + 'static,
    D: Send + Sync + 'static,
    A: Serve<I> + Send + 'static,
    B: Serve<I> + Send + 'static,
    <A as Serve<I>>::Connection: ChunnelConnection<Data = D>,
    <B as Serve<I>>::Connection: ChunnelConnection<Data = D>,
    <A as Serve<I>>::Error: Into<Report> + Send + Sync + 'static,
    <B as Serve<I>>::Error: Into<Report> + Send + Sync + 'static,
{
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;
    type Connection = Either<A::Connection, B::Connection>;
    type Error = Report;
    type Stream =
        Pin<Box<dyn Stream<Item = Result<Self::Connection, Self::Error>> + Send + 'static>>;

    fn serve(&mut self, inner: I) -> Self::Future {
        match self {
            Either::Left(a) => {
                let fut = a.serve(inner);
                Box::pin(async move {
                    Ok(Box::pin(
                        fut.await
                            .map_err(|e| e.into())?
                            .map(|cn| Ok(Either::Left(cn.map_err(|e| e.into())?))),
                    ) as _)
                })
            }
            Either::Right(b) => {
                let fut = b.serve(inner);
                Box::pin(async move {
                    Ok(Box::pin(
                        fut.await
                            .map_err(|e| e.into())?
                            .map(|cn| Ok(Either::Right(cn.map_err(|e| e.into())?))),
                    ) as _)
                })
            }
        }
    }
}
