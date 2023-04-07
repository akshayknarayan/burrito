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

    pub fn conn(&self) -> &C {
        &self.0
    }
}

impl<C, D> ChunnelConnection for InjectWithChannel<C, D>
where
    C: ChunnelConnection<Data = D> + Send + Sync,
    D: Send + 'static,
{
    type Data = D;

    fn send<'cn, B>(
        &'cn self,
        burst: B,
    ) -> Pin<Box<dyn Future<Output = Result<(), Report>> + Send + 'cn>>
    where
        B: IntoIterator<Item = Self::Data> + Send + 'cn,
        <B as IntoIterator>::IntoIter: Send,
    {
        self.0.send(burst)
    }

    fn recv<'cn, 'buf>(
        &'cn self,
        msgs_buf: &'buf mut [Option<Self::Data>],
    ) -> Pin<Box<dyn Future<Output = Result<&'buf mut [Option<Self::Data>], Report>> + Send + 'cn>>
    where
        'buf: 'cn,
    {
        if self.1.load(std::sync::atomic::Ordering::SeqCst) {
            self.0.recv(msgs_buf)
        } else {
            Box::pin(async move {
                let mut slot_one = [None];
                let mut slot_two = [None];
                let slot_two = &mut slot_two;
                let either_slot = select(
                    self.0.recv(&mut slot_one),
                    Box::pin(async move {
                        use std::ops::DerefMut;
                        match self.2.lock().await.deref_mut().await {
                            Ok(d) => {
                                trace!("recirculate first packet on prenegotiated connection");
                                self.1.store(true, std::sync::atomic::Ordering::SeqCst);
                                slot_two[0] = Some(d);
                                Ok(&mut slot_two[..1])
                            }
                            Err(_) => futures_util::future::pending().await,
                        }
                    }),
                )
                .map(|e| e.factor_first().0)
                .await?;
                msgs_buf[0] = either_slot[0].take();
                Ok(&mut msgs_buf[..1])
            })
        }
    }
}
