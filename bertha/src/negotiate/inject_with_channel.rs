use crate::ChunnelConnection;
use color_eyre::eyre::Report;
use futures_util::future::{select, FutureExt};
use std::sync::{atomic::AtomicBool, Arc};
use std::{future::Future, pin::Pin};
use tokio::sync::{oneshot, Mutex};
use tracing::trace;

/// Enable recirculating packets into lower layers of a `ChunnelConnection` via a oneshot channel.
pub struct InjectWithChannel<C, D>(C, Arc<AtomicBool>, Arc<Mutex<oneshot::Receiver<D>>>);

impl<C, D> InjectWithChannel<C, D> {
    pub fn make(inner: C) -> (Self, oneshot::Sender<D>) {
        let (s, r) = oneshot::channel();
        (
            Self(
                inner,
                Arc::new(AtomicBool::new(false)),
                Arc::new(Mutex::new(r)),
            ),
            s,
        )
    }
}

impl<C, D> ChunnelConnection for InjectWithChannel<C, D>
where
    C: ChunnelConnection<Data = D>,
    D: Send + 'static,
{
    type Data = D;

    fn send(
        &self,
        data: Self::Data,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'static>> {
        self.0.send(data)
    }

    fn recv(&self) -> Pin<Box<dyn Future<Output = Result<Self::Data, Report>> + Send + 'static>> {
        let f = self.0.recv();
        if self.1.load(std::sync::atomic::Ordering::SeqCst) {
            f
        } else {
            let done = Arc::clone(&self.1);
            let r = Arc::clone(&self.2);
            let sel = select(
                f,
                Box::pin(async move {
                    use std::ops::DerefMut;
                    match r.lock().await.deref_mut().await {
                        Ok(d) => {
                            trace!("recirculate first packet on prenegotiated connection");
                            done.store(true, std::sync::atomic::Ordering::SeqCst);
                            Ok(d)
                        }
                        Err(_) => futures_util::future::pending().await,
                    }
                }),
            )
            .map(|e| e.factor_first().0);
            Box::pin(sel)
        }
    }
}
