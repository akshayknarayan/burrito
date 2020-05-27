use futures_util::stream::StreamExt;
use rpcbench::{SPingParams, SPong};
use slog::{info, warn};
use std::convert::TryInto;
use structopt::StructOpt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_server")]
struct Opt {
    #[structopt(short, long)]
    unix_addr: Option<std::path::PathBuf>,

    #[structopt(long)]
    burrito_addr: Option<String>,

    #[structopt(short, long)]
    port: Option<u16>,

    #[structopt(long, default_value = "/tmp/burrito")]
    burrito_root: String,
}

async fn serve(
    l: impl futures_util::stream::Stream<
        Item = Result<impl AsyncRead + AsyncWrite + Unpin, impl std::fmt::Debug>,
    >,
    srv: rpcbench::Server,
) -> Result<(), failure::Error> {
    l.for_each_concurrent(None, |st| async {
        let mut buf = [0u8; 12000];
        let mut st = st.expect("accept failed");
        tracing::trace!("New connection");
        loop {
            if let Err(_) = st.read_exact(&mut buf[0..4]).await {
                tracing::trace!("connection done");
                return;
            }

            let resp_len = u32::from_be_bytes(buf[0..4].try_into().unwrap());
            st.read_exact(&mut buf[0..resp_len as usize]).await.unwrap();
            let p: SPingParams = bincode::deserialize(&buf[..resp_len as usize]).unwrap();
            let ans: SPong = srv.do_ping(p.into()).await.unwrap().into();
            let resp = bincode::serialize(&ans).unwrap();
            let resp_len = resp.len() as u32;
            st.write(&resp_len.to_be_bytes()).await.unwrap();
            st.write(&resp).await.unwrap();
        }
    })
    .await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_util::logger();
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();

    let srv_impl = rpcbench::Server::default();

    if let Some(path) = opt.unix_addr {
        info!(&log, "UDS mode"; "addr" => ?&path);
        let l = tokio::net::UnixListener::bind(&path)?;
        serve(l, srv_impl).await?;
        return Ok(());
    }

    if opt.port.is_none() {
        warn!(&log, "Must specify port if not using unix address");
        failure::bail!("Must specify port if not using unix address");
    }

    let port = opt.port.unwrap();

    if let Some(addr) = opt.burrito_addr {
        info!(&log, "burrito mode";  "burrito_root" => ?&opt.burrito_root, "addr" => ?&addr, "tcp port" => port);
        let srv = burrito_addr::Server::start(&addr, ("tcp", port), &opt.burrito_root).await?;
        serve(srv, srv_impl).await?;
        return Ok(());
    }

    info!(&log, "TCP mode"; "port" => port);
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let l = tokio::net::TcpListener::bind(addr).await?;
    serve(l, srv_impl).await?;
    Ok(())
}
