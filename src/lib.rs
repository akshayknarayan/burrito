//! Container manager
#![warn(clippy::all)]

pub fn logger() -> slog::Logger {
    use slog::Drain;
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

pub mod docker_proxy;
pub use docker_proxy::*;

pub mod burrito_net;
pub use burrito_net::*;
