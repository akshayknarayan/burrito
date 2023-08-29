use bertha::{Chunnel, ChunnelConnection, Negotiate};
use color_eyre::eyre::Report;
use futures_util::future::{ready, Ready};
use std::{future::Future, pin::Pin};

pub trait SetGroup {
    fn set_group(&mut self, group: String);
}

#[cfg(feature = "sqs")]
impl SetGroup for sqs::SqsAddr {
    fn set_group(&mut self, group: String) {
        self.group = Some(group);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FakeSetGroupAddr(String, String);
impl From<String> for FakeSetGroupAddr {
    fn from(s: String) -> Self {
        Self(s, String::new())
    }
}

impl SetGroup for FakeSetGroupAddr {
    fn set_group(&mut self, group: String) {
        self.1 = group;
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeSetGroup;

impl Negotiate for FakeSetGroup {
    type Capability = ();
    fn guid() -> u64 {
        0x8850250c3bbd66ed
    }
}

impl<InC> Chunnel<InC> for FakeSetGroup
where
    InC: ChunnelConnection<Data = (String, String)> + Send + Sync + 'static,
{
    type Future = Ready<Result<Self::Connection, Self::Error>>;
    type Connection = FakeSetGroupCn<InC>;
    type Error = Report;

    fn connect_wrap(&mut self, inner: InC) -> Self::Future {
        ready(Ok(FakeSetGroupCn(inner)))
    }
}

#[derive(Debug, Clone)]
pub struct FakeSetGroupCn<C>(C);
impl<C> From<C> for FakeSetGroupCn<C> {
    fn from(c: C) -> Self {
        Self(c)
    }
}

impl<C> ChunnelConnection for FakeSetGroupCn<C>
where
    C: ChunnelConnection<Data = (String, String)> + Send + Sync,
{
    type Data = (FakeSetGroupAddr, String);

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.0.send(
            burst
                .into_iter()
                .map(|(FakeSetGroupAddr(addr, _), body)| (addr, body)),
        )
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        Box::pin(async move {
            let mut slots = vec![None; msgs_buf.len()];
            let bufs = self.0.recv(&mut slots).await?;
            let mut ret_len = 0;
            for (buf, slot) in bufs
                .iter_mut()
                .map_while(|x| x.take())
                .zip(msgs_buf.iter_mut())
            {
                *slot = Some((FakeSetGroupAddr(buf.0, String::new()), buf.1));
                ret_len += 1;
            }

            Ok(&mut msgs_buf[..ret_len])
        })
    }

    //fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
    //    let fut = self.0.recv();
    //    Box::pin(async move {
    //        let (a, d) = fut.await?;
    //        Ok((FakeSetGroupAddr(a, String::new()), d))
    //    })
    //}
}
