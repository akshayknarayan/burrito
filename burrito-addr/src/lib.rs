use core::{
    pin::Pin,
    task::{Context, Poll},
};

mod conn;
mod uri;

pub use conn::*;
pub use uri::*;

pub mod bincode;
pub mod flatbuf;
pub mod tonic;

fn poll_select_accept(
    mut ui: tokio::net::unix::Incoming,
    mut ti: tokio::net::tcp::Incoming,
    cx: &mut Context,
) -> Poll<Option<Result<conn::Conn, failure::Error>>> {
    // If there is a high rate of incoming Unix connections, this approach could starve
    // the TcpListener, since if ui.poll_accept() is ready, we just return.
    // This is no better than what it replaced: a call to futures_util::future::select.
    // If starvation becomes an issue in the future (hah), consider a coinflip to pick.
    match Pin::new(&mut ui).poll_accept(cx) {
        Poll::Pending => (),
        Poll::Ready(Ok(x)) => return Poll::Ready(Some(Ok(Conn::Unix(x)))),
        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
    };

    match Pin::new(&mut ti).poll_accept(cx) {
        Poll::Pending => (),
        Poll::Ready(Ok(s)) => {
            s.set_nodelay(true)
                .expect("set nodelay on accepted connection");
            return Poll::Ready(Some(Ok(Conn::Tcp(s))));
        }
        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
    };

    Poll::Pending
}
