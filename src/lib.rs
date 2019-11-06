//! Container manager

#![feature(type_alias_impl_trait)]

use bollard::{container::Config};
use failure::Error;
use hyper::{
    Body, Client, Request, Response, StatusCode,
};
use hyper_unix_connector::{UnixClient};
use std::future::Future;
use std::collections::HashMap;
use std::pin::Pin;
use core::task::{Poll, Context};
        use futures_util::future::FutureExt;

pub fn logger() -> slog::Logger {
    use slog::Drain;
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

pub struct MakeDockerProxy {
    pub out_addr: std::path::PathBuf,
    pub log: slog::Logger,
}

impl<T> tower::Service<T> for MakeDockerProxy {
    type Response = DockerProxy;
    type Error = failure::Error;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>> + Sync + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        let out_addr = self.out_addr.clone();
        let log = self.log.clone();
        Box::pin(async {
            Ok(DockerProxy {
                out_addr,
                client: Client::builder().build(UnixClient),
                log,
            })
        })
    }
}

/// Proxy for connections to the docker api.
/// Adds a burrito mountpoint.
#[derive(Debug)]
pub struct DockerProxy{
    out_addr: std::path::PathBuf,
    client: hyper::client::Client<UnixClient, Body>,
    log: slog::Logger,
}

impl tower::Service<Request<Body>> for DockerProxy {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = impl Future<Output=Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        lazy_static::lazy_static! {
            //static ref CONTAINER_NAME_RE: regex::Regex = Regex::new(r"/?[a-zA-Z0-9][a-zA-Z0-9_.-]+").unwrap();
            static ref DOCKER_CREATE_CONTAINER_RE: regex::Regex = regex::Regex::new(r".containers.create").unwrap();
        }

        // check if request is creating a container
        let req = match (req.method(), req.uri().path()) {
            (&hyper::Method::POST, path) if DOCKER_CREATE_CONTAINER_RE.is_match(path) => {
                slog::info!(&self.log, "create container request";
                    "path" => ?req.uri().path_and_query(),
                    "headers" => ?req.headers(),
                    "body" => ?req.body(),
                );

                Box::pin(DockerProxy::rewrite_create_container_request(req, self.log.clone()))
                    as Pin<Box<dyn Future<Output = _> + Send>>
            }
            _ => {
                // log the request
                slog::trace!(&self.log, "got request";
                    "path" => ?req.uri().path_and_query(),
                    "headers" => ?req.headers(),
                    "body" => ?req.body(),
                );

                Box::pin(async move { Ok(req) })
                    as Pin<Box<dyn Future<Output = _> + Send>>
            }
        };

        // forward the request
        let a = self.out_addr.clone();
        let l2 = self.log.clone();
        let client = self.client.clone();
        async move {
            match req.await {
                Ok(req) => {
                    let mut req: Request<Body> = req;
                    let uri = hyper_unix_connector::Uri::new(
                        a,
                        req.uri()
                        .path_and_query()
                        .map(|x| x.as_str())
                        .unwrap_or_else(|| ""),
                    );
                    *req.uri_mut() = uri.into();
                    client
                        .request(req)
                        .map(move |resp| {
                            if let Ok(ref r) = resp {
                                // log the response
                                slog::trace!(l2, "got response";
                                    "headers" => ?r.headers(),
                                    "body" => ?r.body(),
                                );
                            }

                            resp
                        })
                    .await
                }
                Err(e) => {
                    let e = format!("{:?}", e);
                    let st =
                        futures_util::stream::once(async move { Ok::<_, Error>(e) });
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::wrap_stream(st))
                        .unwrap())
                }
            }
        }
    }
}

impl DockerProxy {
    async fn rewrite_create_container_request(
        req: hyper::Request<Body>,
        log: slog::Logger,
    ) -> Result<hyper::Request<Body>, Error> {
        use futures_util::try_stream::TryStreamExt;

        let (parts, body) = req.into_parts();
        let payload: hyper::Chunk = body.try_concat().await?;
        let mut cfg: Config<String> = serde_json::from_slice(&payload).map_err(|e| {
            let payload_str = String::from_utf8(payload.to_vec()).expect("body utf8");
            slog::warn!(log, "payload parse failed";
                "err" => ?e,
                "raw" => payload_str,
            );

            e
        })?;

        slog::info!(log, "create container payload";
            "body" => ?cfg,
        );

        // add our custom mountpoint
        let new_bind = "/tmp/burrito:/burrito".to_string();
        match cfg.host_config {
            Some(bollard::container::HostConfig {
                binds: Some(ref mut c),
                ..
            }) => {
                c.push(new_bind);
            }
            Some(ref mut hc) => hc.binds = Some(vec![new_bind]),
            _ => {
                cfg.host_config = Some(bollard::container::HostConfig {
                    binds: Some(vec![new_bind]),
                    ..Default::default()
                });
            }
        };

        let cfg_body = serde_json::to_vec(&cfg)?;
        let body = hyper::Chunk::from(cfg_body);

        Ok::<_, Error>(Request::from_parts(parts, body.into()))
    }
}

pub mod burritonet {
    tonic::include_proto!("burrito");
}

/// Manages the inter-container network.
///
/// Jobs:
/// 1. Maintain service addresses
/// 2. if local, return remote pipe address
/// 3. TODO if remote, establish a connection with the right machine and splice the pipe with that
///
/// Services register with the listen() RPC.
/// Clients call the open() RPC with a service-level address to get the address to connect to.
#[derive(Default)]
pub struct BurritoNet {
    root: std::path::PathBuf,
    route_table: std::sync::Mutex<HashMap<String, String>>,
}

impl BurritoNet {
    fn listen_path(&self) -> std::path::PathBuf {
        self.root.join("controller")
    }
}

use burritonet::server::{Connection, ConnectionServer};
use burritonet::{ListenReply, ListenRequest, OpenReply, OpenRequest};

#[tonic::async_trait]
impl Connection for BurritoNet {
    async fn listen(
        &self,
        request: tonic::Request<ListenRequest>,
    ) -> Result<tonic::Response<ListenReply>, tonic::Status> {
        // service_addr is what other services will call open() with
        let service_addr = request.into_inner().service_addr;

        use rand::Rng;
        let rng = rand::thread_rng();

        let p: String = rng
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .collect();
        let listen_addr = self
            .root
            .join(p)
            .into_os_string()
            .into_string()
            .expect("Generating pipe address failed");

        {
            self.route_table
                .lock()
                .expect("Route table lock poisoned")
                .insert(service_addr.clone(), listen_addr.clone())
                .ok_or_else(|| {
                    tonic::Status::new(
                        tonic::Code::AlreadyExists,
                        format!("Service address {} already in use", &service_addr),
                    )
                })
        }?; // release route_table lock

        let reply = burritonet::ListenReply {
            listen_addr: listen_addr.clone(),
        };
        Ok(tonic::Response::new(reply))
    }

    async fn open(
        &self,
        request: tonic::Request<OpenRequest>,
    ) -> Result<tonic::Response<OpenReply>, tonic::Status> {
        let dst_addr = request.into_inner().dst_addr;

        // look up the service addr to translate
        let send_addr = {
            self.route_table
                .lock()
                .expect("Route table lock poisoned")
                .get(&dst_addr)
                .ok_or_else(|| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Unknown service address {}", dst_addr),
                    )
                })?
                .to_string()
        }; // release route_table lock

        let reply = burritonet::OpenReply { send_addr };
        Ok(tonic::Response::new(reply))
    }
}
