//! Chunnel implementing reliability.

use crate::{Chunnel, Endedness, Scope};
use std::collections::HashMap;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Default)]
struct ReliabilityState {
    inflight: HashMap<u32, Vec<u8>>, // list of inflight seqs
}

#[derive(Default)]
pub struct Reliability<C> {
    inner: Arc<C>,
    state: Arc<RwLock<ReliabilityState>>,
}

impl<Cx> Reliability<Cx> {
    pub fn with_context<C>(self, cx: C) -> Reliability<C> {
        Reliability {
            inner: Arc::new(cx),
            state: Default::default(),
        }
    }
}

impl<C> Chunnel for Reliability<C>
where
    C: Chunnel<Data = Vec<u8>> + Send + Sync + 'static,
{
    type Data = (u32, Vec<u8>); // a tag and its data.

    fn send(&self, data: Self::Data) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>>>> {
        if data.1.is_empty() {
            return Box::pin(async move { Ok(()) });
        }

        let state = self.state.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = state.write().await;
            let (seq, data) = data;
            let mut buf = seq.to_be_bytes().to_vec();
            buf.extend(&data);
            state.inflight.insert(seq, data);
            inner.send(buf).await?;
            Ok(())
        })
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>>>> {
        let inner = self.inner.clone();
        let state = self.state.clone();
        Box::pin(async move {
            loop {
                let mut seq = inner.recv().await?;
                // pop off the seqno
                let data = seq.split_off(4);
                let seq = u32::from_be_bytes(seq[0..4].try_into().unwrap());

                if data.is_empty() {
                    // it was an ack
                    let mut st = state.write().await;
                    st.inflight.remove(&seq);
                    continue;
                } else {
                    // send an ack
                    inner.send(seq.to_be_bytes().to_vec()).await?;
                }

                return Ok((seq, data));
            }
        })
    }

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Both // we add a header
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn no_drops() {
        let msgs = vec![(0, vec![0u8; 10]), (1, vec![1u8; 10]), (2, vec![2u8; 10])];
    }
}
