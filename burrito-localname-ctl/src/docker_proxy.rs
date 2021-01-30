use bollard::container::Config;
use color_eyre::eyre::{Error, WrapErr};
use core::task::{Context, Poll};
use futures_util::future::FutureExt;
use hyper::{Body, Client, Request, Response, StatusCode};
use hyper_unix_connector::UnixClient;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use tower_service as tower;
use tracing::{info, trace, warn};

pub async fn serve(in_addr_docker: PathBuf, out_addr_docker: PathBuf) -> Result<(), Error> {
    // get docker-proxy serving future
    std::fs::remove_file(&in_addr_docker).unwrap_or_default(); // ignore error if file was not present
    use hyper_unix_connector::UnixConnector;
    let uc: UnixConnector = tokio::net::UnixListener::bind(&in_addr_docker)
        .context("Could not bind to docker proxy address")?
        .into();
    let make_service = MakeDockerProxy {
        out_addr: out_addr_docker.clone(),
    };
    let docker_proxy_server = hyper::server::Server::builder(uc).serve(make_service);

    info!(listening_at = ?&in_addr_docker, proxying_to = ?&out_addr_docker, "docker proxy starting" );
    Ok(docker_proxy_server.await?)
}

pub struct MakeDockerProxy {
    pub out_addr: std::path::PathBuf,
}

impl<T> tower::Service<T> for MakeDockerProxy {
    type Response = DockerProxy;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Sync + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        let out_addr = self.out_addr.clone();
        Box::pin(async {
            Ok(DockerProxy {
                out_addr,
                client: Client::builder().build(UnixClient),
            })
        })
    }
}

/// Proxy for connections to the docker api.
/// Adds a burrito mountpoint.
#[derive(Debug)]
pub struct DockerProxy {
    out_addr: std::path::PathBuf,
    client: hyper::client::Client<UnixClient, Body>,
}

impl tower::Service<Request<Body>> for DockerProxy {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

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
                info!(
                    path = ?req.uri().path_and_query(),
                    headers = ?req.headers(),
                    body = ?req.body(),
                    "create container request"
                );

                Box::pin(DockerProxy::rewrite_create_container_request(req))
                    as Pin<Box<dyn Future<Output = _> + Send>>
            }
            _ => {
                // log the request
                trace!(
                    path = ?req.uri().path_and_query(),
                    headers = ?req.headers(),
                    body = ?req.body(),
                    "got request"
                );

                Box::pin(async move { Ok(req) }) as Pin<Box<dyn Future<Output = _> + Send>>
            }
        };

        let oa = self.out_addr.clone();
        let cl = self.client.clone();

        Box::pin(async move {
            let req: Result<Request<Body>, Error> = req.await;
            match req {
                Ok(req) => {
                    if req.headers().contains_key(hyper::header::UPGRADE) {
                        // if req contains an upgrade header, handle the upgrade
                        DockerProxy::handle_upgrade(req, oa, cl).await
                    } else {
                        // else just forward the request
                        DockerProxy::forward_request(req, oa, cl).await
                    }
                }
                Err(e) => error_response(e),
            }
        }) as Pin<_>
    }
}

impl DockerProxy {
    async fn rewrite_create_container_request(
        req: hyper::Request<Body>,
    ) -> Result<hyper::Request<Body>, Error> {
        use futures_util::stream::TryStreamExt;

        let (mut parts, body) = req.into_parts();
        let payload: Vec<u8> = body.map_ok(|x| x.to_vec()).try_concat().await?;
        let mut cfg: Config<String> = serde_json::from_slice(&payload).map_err(|e| {
            let payload_str = String::from_utf8(payload.to_vec()).expect("body utf8");
            warn!(
                err = ?e,
                raw = ?payload_str,
                "payload parse failed"
            );

            e
        })?;

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

        info!(
            parts = ?parts,
            body = ?cfg,
            "create container payload"
        );

        let cfg_body = serde_json::to_vec(&cfg)?;
        let body_len = cfg_body.len();
        let body = hyper::body::Bytes::from(cfg_body);

        if let Some(v) = parts.headers.get_mut("content-length") {
            *v = body_len.into();
        }

        Ok::<_, Error>(Request::from_parts(parts, body.into()))
    }

    async fn forward_request(
        req: Request<Body>,
        addr: std::path::PathBuf,
        client: hyper::client::Client<UnixClient, Body>,
    ) -> Result<Response<Body>, hyper::Error> {
        let mut req: Request<Body> = req;
        let uri = hyper_unix_connector::Uri::new(
            addr,
            req.uri()
                .path_and_query()
                .map(|x| x.as_str())
                .unwrap_or_else(|| ""),
        );
        *req.uri_mut() = uri.into();

        trace!(
            path = ?req.uri().path_and_query(),
            "forwarding request"
        );

        client
            .request(req)
            .map(move |resp| {
                if let Ok(ref r) = resp {
                    // log the response
                    trace!(
                        code = ?r.status(),
                        headers = ?r.headers(),
                        body = ?r.body(),
                        "got response"
                    );
                }

                resp
            })
            .await
    }

    async fn handle_upgrade(
        req: Request<Body>,
        addr: std::path::PathBuf,
        client: hyper::client::Client<UnixClient, Body>,
    ) -> Result<Response<Body>, hyper::Error> {
        use futures_util::stream::{StreamExt, TryStreamExt};

        let (parts, mut body) = req.into_parts();
        let payload: Vec<u8> = body.by_ref().map_ok(|x| x.to_vec()).try_concat().await?;
        let payload_str = std::str::from_utf8(&payload).expect("body utf8");
        trace!(body = ?payload_str, "upgrade request body");
        // reassemble request
        let req = Request::from_parts(parts, Body::from(payload));
        let request_upgrade_fut = body.on_upgrade();

        // forward request
        let resp = DockerProxy::forward_request(req, addr, client).await;

        let (parts, mut body) = resp?.into_parts();
        let payload: Vec<u8> = body.by_ref().map_ok(|x| x.to_vec()).try_concat().await?;
        let payload_str = std::str::from_utf8(&payload).expect("body utf8");
        trace!(body = ?payload_str, "upgrade response body");
        // reassemble response
        let resp = Response::from_parts(parts, Body::from(payload));
        let response_upgrade_fut = body.on_upgrade();

        // copy between request_upgrade_fut <--> response_upgrade_fut until both are EOFed
        tokio::spawn(async move {
            let (req_upgrade, resp_upgrade) =
                futures_util::future::join(request_upgrade_fut, response_upgrade_fut).await;

            let req_upgrade = match req_upgrade {
                Ok(r) => r,
                Err(e) => {
                    warn!(on = "request upgrade", err = ?e, "Upgrade error");
                    return;
                }
            };
            let resp_upgrade = match resp_upgrade {
                Ok(r) => r,
                Err(e) => {
                    warn!(on = "response upgrade", err = ?e, "Upgrade error");
                    return;
                }
            };

            let (mut req_read, mut req_write) = tokio::io::split(req_upgrade);
            let (mut resp_read, mut resp_write) = tokio::io::split(resp_upgrade);

            tokio::spawn(async move {
                if let Err(e) = tokio::io::copy(&mut req_read, &mut resp_write).await {
                    warn!(on = "copy req -> resp", err = ?e, "Upgrade error");
                };
            });

            tokio::spawn(async move {
                if let Err(e) = tokio::io::copy(&mut resp_read, &mut req_write).await {
                    warn!(on = "copy resp -> req", err = ?e,  "Upgrade error");
                };
            });
        });

        Ok(resp)
    }
}

fn error_response(e: Error) -> Result<Response<Body>, hyper::Error> {
    let e = format!("{:?}", e);
    let st = futures_util::stream::once(async move { Ok::<_, Error>(e) });
    Ok(Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::wrap_stream(st))
        .unwrap())
}
