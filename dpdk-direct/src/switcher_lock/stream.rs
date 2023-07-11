use super::ReqDatapathStream;
use color_eyre::Report;
use flume::Receiver;
use futures_util::TryStreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct UpgradeStream {
    inner: Arc<Mutex<ReqDatapathStream>>,
    pause_update: Receiver<()>,
}

impl UpgradeStream {
    pub(crate) fn new(inner: Arc<Mutex<ReqDatapathStream>>, pause_update: Receiver<()>) -> Self {
        Self {
            inner,
            pause_update,
        }
    }

    pub async fn next(self) -> Result<Option<(super::DatapathCn, Self)>, Report> {
        let cn = loop {
            let rcv = Box::pin(async { self.inner.lock().await.try_next().await });
            match futures_util::future::select(self.pause_update.recv_async(), rcv).await {
                futures_util::future::Either::Left((r, _)) => {
                    r.expect("datapath transition sender disappeared");
                }
                futures_util::future::Either::Right((cn, _)) => {
                    break cn?;
                }
            }
        };

        return match cn {
            Some(c) => Ok::<_, Report>(Some((c, self))),
            None => Ok(None),
        };
    }
}
