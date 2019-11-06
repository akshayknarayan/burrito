use std::pin::Pin;
use std::future::Future;
use hyper::{service::{make_service_fn, service_fn}, Client, Request, Body};
use hyper_unix_connector::UnixClient;
use structopt::{StructOpt};
use futures_util::future::FutureExt;
use futures_util::try_stream::TryStreamExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "dump-docker-proxy")]
struct Opt {
    #[structopt(short, long)]
    in_addr: std::path::PathBuf,
    
    #[structopt(short, long)]
    out_addr: std::path::PathBuf,
}

fn logger() -> slog::Logger {
    use slog::Drain;
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();
    let log = logger();

    let client_main = Client::builder().build(UnixClient);
    let out_addr = opt.out_addr.clone();
    let l = log.clone();

    let make_service = make_service_fn(move |_| {
        let client = client_main.clone();
        let out_addr_clone = out_addr.clone();
        let l = l.clone();

        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {

                lazy_static::lazy_static! {
                    //static ref CONTAINER_NAME_RE: regex::Regex = Regex::new(r"/?[a-zA-Z0-9][a-zA-Z0-9_.-]+").unwrap();
                    static ref DOCKER_CREATE_CONTAINER_RE: regex::Regex = regex::Regex::new(r".containers.create").unwrap();
                }

                // check if request is creating a container
                let req  = match (req.method(), req.uri().path()) {
                    (&hyper::Method::POST, path) if DOCKER_CREATE_CONTAINER_RE.is_match(path) => {
                        slog::info!(l, "create container request";
                            "path" => ?req.uri().path_and_query(),
                            "headers" => ?req.headers(),
                            "body" => ?req.body(),
                        );

                        // log the body somehow
                        let (parts, body) = req.into_parts();
                        let l2 = l.clone();
                        Box::pin(async move {
                            let payload: hyper::Chunk = body.try_concat().await?;
                            let payload_str = String::from_utf8(payload.to_vec()).expect("body utf8");
                            slog::info!(l2, "create container payload";
                                "body" => payload_str,
                            );

                            Ok::<_, hyper::Error>(Request::from_parts(parts, payload.into()))
                        }) as Pin<Box<dyn Future<Output = _> + Send>>
                    }
                    _ => {
                        // log the request
                        slog::trace!(l, "got request"; 
                            "path" => ?req.uri().path_and_query(),
                            "headers" => ?req.headers(),
                            "body" => ?req.body(),
                        );

                        Box::pin(async move {Ok(req)}) as Pin<Box<dyn Future<Output = _> + Send>>
                    }
                };

                // forward the request
                let a = out_addr_clone.clone();
                let l2 = l.clone();
                let client = client.clone();
                async move {
                    let mut req: Request<Body> = req.await?;
                    let uri = hyper_unix_connector::Uri::new(a, req.uri().path_and_query().map(|x| x.as_str()).unwrap_or_else(|| ""));
                    *req.uri_mut() = uri.into();
                    client.request(req).map(move |resp| {
                        match resp {
                            Ok(ref r) => {
                                // log the response
                                slog::trace!(l2, "got response";
                                    "headers" => ?r.headers(),
                                    "body" => ?r.body(),
                                );
                            }
                            _ => {},
                        };

                        resp
                    }).await
                }
            }))
        }
    });

    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&opt.in_addr)?.into();
    let server = hyper::server::Server::builder(uc).serve(make_service);

    slog::info!(log, "starting"; "listening at" => ?&opt.in_addr, "proxying to" => ?&opt.out_addr);
    if let Err(e) = server.await {
        slog::crit!(log, "server crashed"; "err" => ?e);
    }

    Ok(())
}
