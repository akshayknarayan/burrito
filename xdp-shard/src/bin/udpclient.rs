use structopt::StructOpt;

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "c", long = "addr")]
    addr: String,

    #[structopt(short = "p", long = "port")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), StdError> {
    let opt = Opt::from_args();

    tracing_subscriber::fmt::init();

    let mut sk = tokio::net::UdpSocket::bind(("0.0.0.0", opt.port))
        .await
        .unwrap();

    let a: std::net::SocketAddr = format!("{}:{}", opt.addr, opt.port).parse().unwrap();
    sk.connect(a).await?;

    let pp: rpcbench::SPingParams = rpcbench::PingParams {
        work: rpcbench::Work::BusyWorkConst as _,
        amount: 100,
    }
    .into();

    let msg = bincode::serialize(&pp)?;
    let mut buf = [0u8; 1024];
    let mut durs = vec![];
    loop {
        sk.send(&msg).await?;
        let len = sk.recv(&mut buf).await?;
        let pong: rpcbench::SPong = bincode::deserialize(&buf[..len])?;
        durs.push(pong.duration_us);
    }
}
