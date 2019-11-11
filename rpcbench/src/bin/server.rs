use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "ping_server")]
struct Opt {
    #[structopt(short, long)]
    addr: std::net::SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();

    let srv = rpcbench::Server;
    tonic::transport::Server::builder()
        .add_service(rpcbench::PingServer::new(srv))
        .serve(opt.addr)
        .await?;

    Ok(())
}
