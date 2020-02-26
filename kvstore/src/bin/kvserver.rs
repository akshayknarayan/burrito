use futures_util::stream::StreamExt;
use slog::{info, warn};
use std::error::Error;
use std::future::Future;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvserver")]
struct Opt {
    #[structopt(short, long)]
    unix_addr: Option<std::path::PathBuf>,

    #[structopt(long)]
    burrito_addr: Option<String>,

    #[structopt(short, long)]
    port: Option<u16>,

    #[structopt(long, default_value = "/tmp/burrito")]
    burrito_root: String,

    #[structopt(long, default_value = "flatbuf")]
    burrito_proto: String,
}

fn serve<S, C, E>(li: S) -> impl Future<Output = ()>
where
    S: futures_util::stream::Stream<Item = Result<C, E>>,
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    E: Into<Box<dyn Error + Sync + Send + 'static>> + std::fmt::Debug,
{
    li.for_each_concurrent(None, |st| {
        async move {
            kvstore::server(st.unwrap()).await.unwrap();
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send + 'static>> {
    let log = burrito_ctl::logger();
    let opt = Opt::from_args();

    info!(&log, "KV Server");

    if let Some(path) = opt.unix_addr {
        info!(&log, "UDS mode"; "addr" => ?&path);
        let mut l = tokio::net::UnixListener::bind(&path)?;
        serve(l.incoming()).await;
        return Ok(());
    }

    if opt.port.is_none() {
        warn!(&log, "Must specify port if not using unix address");
        Err(format!("Must specify port if not using unix address"))?
    }

    let port = opt.port.unwrap();

    if let Some(addr) = opt.burrito_addr {
        match opt.burrito_proto {
            x if x == "tonic" => {
                info!(&log, "burrito mode"; "proto" => &x, "burrito_root" => ?&opt.burrito_root, "addr" => ?&addr, "tcp port" => port);
                serve(burrito_addr::tonic::Server::start(&addr, port, &opt.burrito_root).await?)
                    .await
            }
            x if x == "flatbuf" => {
                info!(&log, "burrito mode"; "proto" => &x, "burrito_root" => ?&opt.burrito_root, "addr" => ?&addr, "tcp port" => port);
                serve(burrito_addr::flatbuf::Server::start(&addr, port, &opt.burrito_root).await?)
                    .await
            }
            x => Err(format!("Unknown burrito protocol {:?}", &x))?,
        };

        return Ok(());
    }

    info!(&log, "TCP mode"; "port" => port);
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    // can't use generic serve() because we need to set tcp_nodelay(true)
    let mut l = tokio::net::TcpListener::bind(addr).await?;
    l.incoming()
        .for_each_concurrent(None, |st| {
            async move {
                let st = st.unwrap();
                st.set_nodelay(true).unwrap();
                kvstore::server(st).await.unwrap()
            }
        })
        .await;

    Ok(())
}
