use futures_util::{
    future::TryFuture,
    stream::{FuturesUnordered, Stream, TryStream},
};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Extension to [`futures_util::stream::TryStreamExt`]
pub trait TryStreamExtExt: TryStream {
    /// Chain a computation when a stream value is ready, passing `Ok` values to the closure `f`.
    ///
    /// This function is similar to [`futures_util::stream::TryStreamExt::and_then`], but the
    /// stream is polled concurrently with the futures returned by `f`. An unbounded number of
    /// futures corresponding to past stream values is kept via
    /// [`futures_util::stream::FuturesUnordered`].
    fn and_then_concurrent<Fut, F>(self, f: F) -> AndThenConcurrent<Self, Fut, F>
    where
        Self: Sized,
        Fut: TryFuture<Error = Self::Error>,
        F: FnMut(Self::Ok) -> Fut;
}

impl<S: TryStream> TryStreamExtExt for S {
    fn and_then_concurrent<Fut, F>(self, f: F) -> AndThenConcurrent<Self, Fut, F>
    where
        Self: Sized,
        Fut: TryFuture<Error = Self::Error>,
        F: FnMut(Self::Ok) -> Fut,
    {
        AndThenConcurrent {
            stream: self,
            futs: FuturesUnordered::new(),
            fun: f,
        }
    }
}

/// Stream type for [`and_then_concurrent`].
#[pin_project(project = AndThenConcurrentProj)]
pub struct AndThenConcurrent<St, Fut: TryFuture, F> {
    #[pin]
    stream: St,
    #[pin]
    futs: FuturesUnordered<Fut>,
    fun: F,
}

impl<St, Fut, F, T> Stream for AndThenConcurrent<St, Fut, F>
where
    St: TryStream,
    Fut: Future<Output = Result<T, St::Error>>,
    F: FnMut(St::Ok) -> Fut,
{
    type Item = Result<T, St::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let AndThenConcurrentProj {
            mut stream,
            mut futs,
            fun,
        } = self.project();

        match stream.as_mut().try_poll_next(cx) {
            Poll::Ready(Some(Ok(n))) => {
                futs.push(fun(n));
            }
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
            Poll::Pending => {
                if futs.is_empty() {
                    return Poll::Pending;
                }
            }
            _ => (),
        }

        let x = futs.as_mut().poll_next(cx);
        if let Poll::Pending = x {
            // check stream once more
            match stream.as_mut().try_poll_next(cx) {
                Poll::Ready(Some(Ok(n))) => {
                    futs.push(fun(n));
                }
                _ => (),
            }
        }
        x
    }
}
