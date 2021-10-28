//! Spawn processes for ghostunnel client and server, to create a TLS tunnel.

use color_eyre::eyre::{eyre, Report, WrapErr};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, Command};
use tokio::sync::oneshot;
use tracing::{debug, debug_span, trace};
use tracing_futures::Instrument;

#[derive(Debug)]
pub struct GhostTunnel {
    _process_handle: Child,
    is_up: Option<oneshot::Receiver<Result<(), Report>>>,
}

impl Drop for GhostTunnel {
    fn drop(&mut self) {
        debug!("dropping ghostunnel prcess");
    }
}

impl GhostTunnel {
    pub(crate) async fn wait_up(&mut self) -> Result<(), Report> {
        if let Some(r) = self.is_up.take() {
            r.await?
        } else {
            Ok(())
        }
    }

    /// The end of the tunnel that serves https and forwards to a local path.
    ///
    /// Serves TLS on `external_addr`, and forwards requests to `local_addr`.
    /// Read certificate files from directory `certs_location`. It will look at files
    /// `{certs_location}/server-combined.pem` and `{certs_location}/cacert.pem`
    pub async fn start_server(
        external_addr: std::net::SocketAddr,
        local_addr: impl AsRef<Path>,
        ghostunnel_path: impl AsRef<Path>,
        certs_location: impl AsRef<Path>,
    ) -> Result<Self, Report> {
        let certs_location = certs_location.as_ref();
        let keystore_arg = certs_location.join("server-combined.pem");
        let local_addr_arg = unix_addr_arg(local_addr);
        let external_addr_arg = external_addr.to_string();
        debug!(mode = "tunnel-exit", external_addr = ?&external_addr_arg, local_addr = ?&local_addr_arg, "starting ghostunnel");
        let mut child = Command::new(ghostunnel_path.as_ref())
            .kill_on_drop(true)
            .arg("--keystore")
            .arg(keystore_arg)
            .arg("--quiet=conns")
            .arg("server")
            .arg("--listen")
            .arg(external_addr_arg)
            .arg("--target")
            .arg(local_addr_arg)
            .arg("--disable-authentication")
            // TODO use a tempfile instead, which would remove the need for polling the fd and
            // dumping it
            .stderr(std::process::Stdio::piped())
            .spawn()
            .wrap_err("spawn ghostunnel server process")?;
        let (s, r) = oneshot::channel();
        tokio::spawn(
            read_stderr(child.stderr.take().unwrap(), s)
                .instrument(debug_span!("server ghostunnel stderr")),
        );
        Ok(Self {
            _process_handle: child,
            is_up: Some(r),
        })
    }

    /// The end of the tunnel that serves unix and forwards to a TLS endpoint.
    ///
    /// Forwards to TLS on `external_addr`, and listens on `local_addr`.
    /// Read certificate files from directory `certs_location`. It will look at files
    /// `{certs_location}/client-keystore.p12` and `{certs_location}/cacert.pem`
    pub async fn start_client(
        external_addr: std::net::SocketAddr,
        local_addr: impl AsRef<Path>,
        ghostunnel_path: impl AsRef<Path>,
        _certs_location: impl AsRef<Path>,
    ) -> Result<Self, Report> {
        let local_addr_arg = unix_addr_arg(local_addr);
        let external_addr_arg = external_addr.to_string();
        debug!(mode = "tunnel-entry", external_addr = ?&external_addr_arg, local_addr = ?&local_addr_arg, "starting ghostunnel");
        let mut child = Command::new(ghostunnel_path.as_ref())
            .kill_on_drop(true)
            .arg("--quiet=conns")
            .arg("client")
            .arg("--target")
            .arg(external_addr_arg)
            .arg("--listen")
            .arg(local_addr_arg)
            .arg("--disable-authentication")
            .stderr(std::process::Stdio::piped())
            .spawn()
            .wrap_err("spawn ghostunnel client process")?;
        let (s, r) = oneshot::channel();
        tokio::spawn(
            read_stderr(child.stderr.take().unwrap(), s)
                .instrument(debug_span!("client ghostunnel stderr")),
        );
        Ok(Self {
            _process_handle: child,
            is_up: Some(r),
        })
    }
}

async fn read_stderr(stderr: ChildStderr, notify_up: oneshot::Sender<Result<(), Report>>) {
    const SEARCH_FOR: &str = "listening for connections";
    let mut buf = String::with_capacity(80);
    let mut rd = BufReader::new(stderr);
    let mut s = Some(notify_up);
    loop {
        match rd.read_line(&mut buf).await {
            Ok(x) if x == 0 => {
                if let Some(n) = s.take() { n.send(Err(eyre!("Ghostunnel child process EOF'ed stderr")))
                        .unwrap() }
                break;
            }
            Ok(_) => {
                if buf.contains(SEARCH_FOR) {
                    debug!(?buf, "matched line");
                    if let Some(n) = s.take() { n.send(Ok(())).unwrap() }
                } else {
                    trace!(?buf, "read line");
                }
            }
            Err(e) => {
                let r: Report = e.into();
                if let Some(n) = s.take() { n.send(Err(
                        r.wrap_err("Error reading Ghostunnel child process stderr")
                    ))
                    .unwrap() }
                break;
            }
        }

        buf.clear();
    }
}

fn unix_addr_arg(a: impl AsRef<Path>) -> String {
    let p = a.as_ref();
    let s = p.to_str().expect("utf8 path");
    "unix:".chars().chain(s.chars()).collect()
}
