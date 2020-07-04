//! Channel-based Chunnel which acts as a transport.

use crate::{Chunnel, Connector, Endedness, Scope};
use futures_util::stream::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

pub struct Chan<Data> {
    snd1: Option<mpsc::Sender<Data>>,
    rcv1: Option<mpsc::Receiver<Data>>,
    snd2: Option<mpsc::Sender<Data>>,
    rcv2: Option<mpsc::Receiver<Data>>,
    loss: Arc<dyn Fn() -> bool + Send + Sync + 'static>,
}

impl Default for Chan<Vec<u8>> {
    fn default() -> Self {
        let (s1, r1) = mpsc::channel(100);
        let (s2, r2) = mpsc::channel(100);
        Self {
            snd1: Some(s1),
            rcv1: Some(r1),
            snd2: Some(s2),
            rcv2: Some(r2),
            loss: Arc::new(|| false),
        }
    }
}

impl<T> Chan<T> {
    pub fn lossy(&mut self, loss: impl Fn() -> bool + Send + Sync + 'static) -> &mut Self {
        self.loss = Arc::new(loss);
        self
    }
}

impl<D> Connector for Chan<D>
where
    D: Send + Sync + 'static,
{
    type Addr = ();
    type Connection = ChanChunnel<D>;

    fn listen(
        &mut self,
        _: Self::Addr,
    ) -> Pin<Box<dyn Future<Output = Pin<Box<dyn Stream<Item = Self::Connection>>>>>> {
        let r = ChanChunnel::new(
            self.snd1.take().unwrap(),
            self.rcv2.take().unwrap(),
            Arc::clone(&self.loss),
        );
        Box::pin(async move { Box::pin(futures_util::stream::once(async { r })) as _ })
    }

    fn connect(&mut self, _: Self::Addr) -> Pin<Box<dyn Future<Output = Self::Connection>>> {
        let r = ChanChunnel::new(
            self.snd2.take().unwrap(),
            self.rcv1.take().unwrap(),
            Arc::clone(&self.loss),
        );
        Box::pin(async { r })
    }
}

pub struct ChanChunnel<Data> {
    snd: Arc<Mutex<mpsc::Sender<Data>>>,
    rcv: Arc<Mutex<mpsc::Receiver<Data>>>,
    loss: Arc<dyn Fn() -> bool + Send + Sync + 'static>,
}

impl<D> ChanChunnel<D> {
    fn new(
        s: mpsc::Sender<D>,
        r: mpsc::Receiver<D>,
        loss: Arc<dyn Fn() -> bool + Send + Sync + 'static>,
    ) -> Self {
        Self {
            snd: Arc::new(Mutex::new(s)),
            rcv: Arc::new(Mutex::new(r)),
            loss,
        }
    }
}

impl<D> Chunnel for ChanChunnel<D>
where
    D: Send + Sync + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), eyre::Report>> + Send + Sync>> {
        let s = self.snd.clone();
        let drop = (self.loss)();

        Box::pin(async move {
            if !drop {
                s.lock().await.send(data).await.map_err(|_| ()).unwrap();
            } else {
                debug!("dropping message");
            }
            Ok(())
        })
    }

    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Data, eyre::Report>> + Send + Sync>> {
        let r = Arc::clone(&self.rcv);
        Box::pin(async move { Ok(r.lock().await.recv().await.unwrap()) })
    }

    fn scope(&self) -> Scope {
        Scope::Application
    }

    fn endedness(&self) -> Endedness {
        Endedness::Both
    }

    fn implementation_priority(&self) -> usize {
        1
    }
}
