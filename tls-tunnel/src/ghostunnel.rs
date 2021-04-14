//! Spawn processes for ghostunnel client and server, to create a TLS tunnel.

use color_eyre::eyre::{Report, WrapErr};
use std::path::Path;
use std::process::{Child, Command};
use tracing::{debug, warn};

#[derive(Debug)]
pub struct GhostTunnel {
    pub(crate) process_handle: Child,
    is_up: bool,
}

impl Drop for GhostTunnel {
    fn drop(&mut self) {
        if let Err(err) = self.process_handle.kill() {
            warn!(?err, "Failed to kill ghostunnel process");
        } else {
            debug!("killed ghostunnel process");
        }
    }
}

impl GhostTunnel {
    pub(crate) fn wait_up(&mut self) -> bool {
        if self.is_up {
            true
        } else {
            // TODO switch to tokio::process so we don't have to issue blocking read
            // calls here.
            self.is_up = if let Some(ref mut stderr) = self.process_handle.stderr {
                const SEARCH_FOR: &str = "listening for connections";
                let mut buf = String::with_capacity(80);
                use std::io::BufRead;
                let mut rd = std::io::BufReader::new(stderr);
                loop {
                    match rd.read_line(&mut buf) {
                        Ok(x) if x == 0 => break false,
                        Ok(_) => {
                            if buf.contains(SEARCH_FOR) {
                                debug!(?buf, "read line");
                                break true;
                            }
                        }
                        Err(_) => break false,
                    }

                    buf.clear();
                }
            } else {
                false
            };

            self.is_up
        }
    }

    /// The end of the tunnel that serves https and forwards to a local path.
    ///
    /// Serves TLS on `external_addr`, and forwards requests to `local_addr`.
    /// Read certificate files from directory `certs_location`. It will look at files
    /// `{certs_location}/server-combined.pem` and `{certs_location}/cacert.pem`
    pub fn start_server(
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
        let child = Command::new(ghostunnel_path.as_ref())
            .arg("--keystore")
            .arg(keystore_arg)
            .arg("server")
            .arg("--listen")
            .arg(external_addr_arg)
            .arg("--target")
            .arg(local_addr_arg)
            .arg("--disable-authentication")
            .stderr(std::process::Stdio::piped())
            .spawn()
            .wrap_err("spawn ghostunnel server process")?;
        Ok(Self {
            process_handle: child,
            is_up: false,
        })
    }

    /// The end of the tunnel that serves unix and forwards to a TLS endpoint.
    ///
    /// Forwards to TLS on `external_addr`, and listens on `local_addr`.
    /// Read certificate files from directory `certs_location`. It will look at files
    /// `{certs_location}/client-keystore.p12` and `{certs_location}/cacert.pem`
    pub fn start_client(
        external_addr: std::net::SocketAddr,
        local_addr: impl AsRef<Path>,
        ghostunnel_path: impl AsRef<Path>,
        _certs_location: impl AsRef<Path>,
    ) -> Result<Self, Report> {
        let local_addr_arg = unix_addr_arg(local_addr);
        let external_addr_arg = external_addr.to_string();
        debug!(mode = "tunnel-entry", external_addr = ?&external_addr_arg, local_addr = ?&local_addr_arg, "starting ghostunnel");
        let child = Command::new(ghostunnel_path.as_ref())
            .arg("client")
            .arg("--target")
            .arg(external_addr_arg)
            .arg("--listen")
            .arg(local_addr_arg)
            .arg("--disable-authentication")
            .stderr(std::process::Stdio::piped())
            .spawn()
            .wrap_err("spawn ghostunnel client process")?;
        Ok(Self {
            process_handle: child,
            is_up: false,
        })
    }
}

fn unix_addr_arg(a: impl AsRef<Path>) -> String {
    let p = a.as_ref();
    let s = p.to_str().expect("utf8 path");
    "unix:".chars().chain(s.chars()).collect()
}
