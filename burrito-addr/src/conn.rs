use core::{
    pin::Pin,
    task::{Context, Poll},
};
use failure::Error;
use pin_project::{pin_project, project};
use tracing::trace;

pub enum Addr {
    Unix(String),
    Tcp(String),
}

impl Addr {
    pub async fn connect(self) -> Result<Conn, Error> {
        Ok(match self {
            Self::Unix(addr) => {
                let st = tokio::net::UnixStream::connect(&addr).await?;
                trace!("burrito-addr::Conn Connected");
                Conn::Unix(st)
            }
            Self::Tcp(addr) => {
                let st = tokio::net::TcpStream::connect(&addr).await?;
                st.set_nodelay(true)?;
                trace!("burrito-addr::Conn Connected");
                Conn::Tcp(st)
            }
        })
    }
}

/// A wrapper around a unix or tcp socket.
#[pin_project]
#[derive(Debug)]
pub enum Conn {
    Unix(#[pin] tokio::net::UnixStream),
    Tcp(#[pin] tokio::net::TcpStream),
}

// Implement a function on Pin<&mut Conn> that both tokio::net::UnixStream and
// tokio::net::TcpStream implement
macro_rules! conn_impl_fn {
    ($fn: ident |$first_var: ident: $first_typ: ty, $($var: ident: $typ: ty),*| -> $ret: ty ;;) => {
        #[project]
        fn $fn ($first_var: $first_typ, $( $var: $typ ),* ) -> $ret {
            #[project]
            match self.project() {
                Conn::Unix(u) => {
                    let ux: Pin<&mut tokio::net::UnixStream> = u;
                    ux.$fn($($var),*)
                }
                Conn::Tcp(t) => {
                    let tc: Pin<&mut tokio::net::TcpStream> = t;
                    tc.$fn($($var),*)
                }
            }
        }
    };
}

impl tokio::io::AsyncRead for Conn {
    conn_impl_fn!(poll_read |self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]| -> Poll<std::io::Result<usize>> ;;);
}

impl tokio::io::AsyncWrite for Conn {
    conn_impl_fn!(poll_write    |self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]| -> Poll<std::io::Result<usize>> ;;);
    conn_impl_fn!(poll_flush    |self: Pin<&mut Self>, cx: &mut Context<'_>| -> Poll<std::io::Result<()>> ;;);
    conn_impl_fn!(poll_shutdown |self: Pin<&mut Self>, cx: &mut Context<'_>| -> Poll<std::io::Result<()>> ;;);
}
