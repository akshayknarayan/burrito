use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_server")]
struct Opt {
    #[structopt(short, long)]
    addr: String,

    #[structopt(short, long, default_value = "/tmp/burrito")]
    burrito_root: String,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let log = burrito_ctl::logger();
    let opt = Opt::from_args();

    use std::str::FromStr;
    if let Ok(addr) = std::net::SocketAddr::from_str(&opt.addr) {
        tonic::transport::Server::builder()
            .add_service(rpcbench::PingServer::new(rpcbench::Server))
            .serve(addr)
            .await?;
    } else {
        let srv = burrito_addr::Server::start(&opt.addr, &opt.burrito_root, Some(&log)).await?;

        let ping_srv = rpcbench::PingServer::new(rpcbench::Server);
        hyper::server::Server::builder(srv)
            .serve(hyper::service::make_service_fn(move |_| {
                let ps = ping_srv.clone();
                async move { Ok::<_, hyper::Error>(ps) }
            }))
            .await?;
    }

    Ok(())
}
