use futures_util::stream::TryStreamExt;
use incoming::IntoIncoming;
use slog::{info, warn};
use std::error::Error;
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

async fn serve<S, C, E>(li: S)
where
    S: futures_util::stream::Stream<Item = Result<C, E>> + Send + 'static,
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    E: Into<Box<dyn Error + Sync + Send + 'static>> + std::fmt::Debug + Unpin + Send,
{
    kvstore::shard_server(vec![li], |_| 0).await.unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send + 'static>> {
    let log = burrito_ctl::logger();
    let opt = Opt::from_args();

    info!(&log, "KV Server");

    if let Some(path) = opt.unix_addr {
        info!(&log, "UDS mode"; "addr" => ?&path);
        let l = tokio::net::UnixListener::bind(&path)?;
        serve(l.into_incoming()).await;
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

    let l = tokio::net::TcpListener::bind(addr).await?;
    let l = l.into_incoming().map_ok(|st| {
        st.set_nodelay(true).unwrap();
        st
    });
    serve(l).await;

    Ok(())
}
