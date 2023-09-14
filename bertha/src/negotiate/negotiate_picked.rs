//! Internal `NegotiatePicked` trait.

use futures_util::future::ready;

use super::Negotiate;
use crate::{CxList, DataEither, Either};
use std::future::Future;
use std::pin::Pin;

pub trait NegotiatePicked {
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>>;
}

impl<N> NegotiatePicked for N
where
    N: Negotiate,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        self.picked(nonce)
    }
}

impl<T> NegotiatePicked for Option<T>
where
    T: NegotiatePicked,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        if let Some(inner) = self {
            inner.call_negotiate_picked(nonce)
        } else {
            Box::pin(ready(()))
        }
    }
}

impl<H, T> NegotiatePicked for CxList<H, T>
where
    H: NegotiatePicked,
    T: NegotiatePicked,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let head_fut = self.head.call_negotiate_picked(nonce);
        let tail_fut = self.tail.call_negotiate_picked(nonce);
        Box::pin(async move {
            head_fut.await;
            tail_fut.await;
        })
    }
}

impl<L, R> NegotiatePicked for DataEither<L, R>
where
    L: NegotiatePicked,
    R: NegotiatePicked,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let f = match self {
            DataEither::Left(l) => l.call_negotiate_picked(nonce),
            DataEither::Right(r) => r.call_negotiate_picked(nonce),
        };

        Box::pin(f)
    }
}

impl<L, R> NegotiatePicked for Either<L, R>
where
    L: NegotiatePicked,
    R: NegotiatePicked,
{
    fn call_negotiate_picked<'s>(
        &mut self,
        nonce: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 's>> {
        let f = match self {
            Either::Left(l) => l.call_negotiate_picked(nonce),
            Either::Right(r) => r.call_negotiate_picked(nonce),
        };

        Box::pin(f)
    }
}
