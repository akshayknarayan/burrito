//! The `Either` type implements traits where both variants implement the trait with the same
//! output type.

use super::{Chunnel, ChunnelConnection};
use color_eyre::eyre::{eyre, Report};
use futures_util::{
    future::{FutureExt, TryFutureExt},
    stream::Stream,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;

pub trait MakeEither<A, B> {
    type Either;
    fn left(inner: A) -> Self::Either;
    fn right(inner: B) -> Self::Either;
}

#[pin_project::pin_project(project = EitherProj)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Either<A, B> {
    Left(#[pin] A),
    Right(#[pin] B),
}

impl<T1, T2, A, B> MakeEither<A, B> for Either<T1, T2> {
    type Either = Either<A, B>;

    fn left(inner: A) -> Self::Either {
        Either::Left(inner)
    }

    fn right(inner: B) -> Self::Either {
        Either::Right(inner)
    }
}

impl<A, B> Either<A, B> {
    pub fn flip(self) -> Either<B, A> {
        match self {
            Either::Left(a) => Either::Right(a),
            Either::Right(a) => Either::Left(a),
        }
    }

    pub fn is_left(&self) -> bool {
        matches!(self, Either::Left(_))
    }

    pub fn is_right(&self) -> bool {
        matches!(self, Either::Right(_))
    }
}

pub trait FlipEither {
    type Flipped;
    fn flip(self) -> Self::Flipped;
}

impl<A, B> FlipEither for Either<A, B> {
    type Flipped = Either<B, A>;
    fn flip(self) -> Self::Flipped {
        self.flip()
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

impl<A, B, C, Din, Dout> Chunnel<C> for Either<A, B>
where
    Din: Send + Sync + 'static,
    Dout: Send + Sync + 'static,
    C: ChunnelConnection<Data = Din>,
    A: Chunnel<C> + Send + 'static,
    B: Chunnel<C> + Send + 'static,
    <A as Chunnel<C>>::Connection: ChunnelConnection<Data = Dout>,
    <B as Chunnel<C>>::Connection: ChunnelConnection<Data = Dout>,
    <A as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
    <B as Chunnel<C>>::Error: Into<Report> + Send + Sync + 'static,
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

/// More expensive version of `Either<A, B>` which doesn't require A and B to have the same data
/// type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DataEither<A, B> {
    Left(A),
    Right(B),
}

impl<T1, T2, A, B> MakeEither<A, B> for DataEither<T1, T2> {
    type Either = DataEither<A, B>;

    fn left(inner: A) -> Self::Either {
        DataEither::Left(inner)
    }

    fn right(inner: B) -> Self::Either {
        DataEither::Right(inner)
    }
}

impl<A, B> DataEither<A, B> {
    pub fn flip(self) -> DataEither<B, A> {
        match self {
            DataEither::Left(a) => DataEither::Right(a),
            DataEither::Right(a) => DataEither::Left(a),
        }
    }

    pub fn is_left(&self) -> bool {
        matches!(self, DataEither::Left(_))
    }

    pub fn is_right(&self) -> bool {
        matches!(self, DataEither::Right(_))
    }
}

impl<A, B> FlipEither for DataEither<A, B> {
    type Flipped = DataEither<B, A>;
    fn flip(self) -> Self::Flipped {
        self.flip()
    }
}

impl<C, A, Da, B, Db, Addr, Dout> Chunnel<C> for DataEither<A, B>
where
    // C has data type Either<Da, Db>
    C: ChunnelConnection<Data = (Addr, DataEither<Da, Db>)> + Send,
    // A and B work for connections of type Da and Db respectively
    A: Chunnel<EitherCn<C, Left>> + Send,
    B: Chunnel<EitherCn<C, Right>> + Send,
    <A as Chunnel<EitherCn<C, Left>>>::Connection: ChunnelConnection<Data = (Addr, Dout)> + Send,
    <B as Chunnel<EitherCn<C, Right>>>::Connection: ChunnelConnection<Data = (Addr, Dout)> + Send,
    <A as Chunnel<EitherCn<C, Left>>>::Error: Into<Report>,
    <B as Chunnel<EitherCn<C, Right>>>::Error: Into<Report>,
    Da: Send,
    Db: Send,
{
    //type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send + 'static>>;
    type Error = Report;

    // EitherCn makes A and B operate over DataEither data types. So, after this, both sides
    // of the Either will have data type DataEither<Da, Db>. So we can Either the connections and it
    // will work out.
    type Connection = Either<
        <A as Chunnel<EitherCn<C, Left>>>::Connection,
        <B as Chunnel<EitherCn<C, Right>>>::Connection,
    >;

    fn connect_wrap(&mut self, inner: C) -> Self::Future {
        match self {
            DataEither::Left(ref mut a) => {
                let inner_either_wrap = EitherCn(inner, Default::default());
                Box::pin(
                    a.connect_wrap(inner_either_wrap)
                        .map_ok(Either::Left)
                        .map_err(Into::into),
                ) as _
            }
            DataEither::Right(ref mut b) => {
                let inner_either_wrap = EitherCn(inner, Default::default());
                Box::pin(
                    b.connect_wrap(inner_either_wrap)
                        .map_ok(Either::Right)
                        .map_err(Into::into),
                ) as _
            }
        }
    }
}

/// Marker type to extract the left side of an `Either`.
pub struct Left;

/// Marker type to extract the right side of an `Either`.
pub struct Right;

/// Takes a `ChunnelConnection` over `DataEither<A, B>` and extracts the given side.
///
/// A connection `C` has type `DataEither<A, B>`. We take our type parameter `Left` or `Right` and
/// extract `A` or `B` respectively. If we see the wrong DataEither variant, return an error.
pub struct EitherCn<C, Side>(C, std::marker::PhantomData<Side>);

impl<A, C, Cd, Od> ChunnelConnection for EitherCn<C, Left>
where
    C: ChunnelConnection<Data = (A, DataEither<Cd, Od>)>,
    Cd: Send + 'static,
    Od: Send + 'static,
    A: Send + Sync + 'static,
{
    type Data = (A, Cd);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.0.send((data.0, DataEither::<Cd, Od>::left(data.1)))
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let fut = self.0.recv();
        Box::pin(async move {
            match fut.await? {
                (a, DataEither::Left(d)) => Ok((a, d)),
                _ => Err(eyre!("Incompatible data type")),
            }
        })
    }
}

impl<A, C, Cd, Od> ChunnelConnection for EitherCn<C, Right>
where
    C: ChunnelConnection<Data = (A, DataEither<Od, Cd>)>,
    Cd: Send + 'static,
    Od: Send + 'static,
    A: Send + Sync + 'static,
{
    type Data = (A, Cd);

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.0.send((data.0, DataEither::<Od, Cd>::right(data.1)))
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let fut = self.0.recv();
        Box::pin(async move {
            match fut.await? {
                (a, DataEither::Right(d)) => Ok((a, d)),
                _ => Err(eyre!("Incompatible data type")),
            }
        })
    }
}
