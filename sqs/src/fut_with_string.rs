use color_eyre::eyre::Report;
use rusoto_sqs::ReceiveMessageResult;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct FutWithString {
    q: String,
    f: Pin<
        Box<dyn Future<Output = Result<(ReceiveMessageResult, String), Report>> + Send + 'static>,
    >,
}

impl FutWithString {
    pub fn key(&self) -> &str {
        &self.q
    }

    pub fn new(
        q: String,
        f: Pin<
            Box<
                dyn Future<Output = Result<(ReceiveMessageResult, String), Report>>
                    + Send
                    + 'static,
            >,
        >,
    ) -> Self {
        Self { q, f }
    }
}

impl Future for FutWithString {
    type Output = Result<(ReceiveMessageResult, String), Report>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::as_mut(&mut self.f).poll(cx)
    }
}
