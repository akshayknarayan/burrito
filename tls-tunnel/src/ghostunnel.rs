//! Spawn processes for ghostunnel client and server, to create a TLS tunnel.

use color_eyre::eyre::Report;
use std::path::Path;
use std::process::{Child, Command};
use tracing::warn;

#[derive(Debug)]
pub struct GhostTunnel {
    process_handle: Child,
}

impl Drop for GhostTunnel {
    fn drop(&mut self) {
        if let Err(err) = self.process_handle.kill() {
            warn!(?err, "Failed to kill ghostunnel process");
        }
    }
}

impl GhostTunnel {
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
        let cacert_arg = certs_location.join("cacert.pem");
        let local_addr_arg = format!("unix:{:?}", local_addr.as_ref());
        let external_addr_arg = external_addr.to_string();
        let child = Command::new(ghostunnel_path.as_ref())
            .arg("--keystore")
            .arg(keystore_arg)
            .arg("--cacert")
            .arg(cacert_arg)
            .arg("server")
            .arg("--listen")
            .arg(external_addr_arg)
            .arg("--target")
            .arg(local_addr_arg)
            .arg("--allow-all")
            .spawn()?;
        Ok(Self {
            process_handle: child,
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
        certs_location: impl AsRef<Path>,
    ) -> Result<Self, Report> {
        let certs_location = certs_location.as_ref();
        let keystore_arg = certs_location.join("client-keystore.p12");
        let cacert_arg = certs_location.join("cacert.pem");
        let local_addr_arg = format!("unix:{:?}", local_addr.as_ref());
        let external_addr_arg = external_addr.to_string();
        let child = Command::new(ghostunnel_path.as_ref())
            .arg("--keystore")
            .arg(keystore_arg)
            .arg("--cacert")
            .arg(cacert_arg)
            .arg("client")
            .arg("--target")
            .arg(local_addr_arg)
            .arg("--listen")
            .arg(external_addr_arg)
            .arg("--allow-all")
            .spawn()?;
        Ok(Self {
            process_handle: child,
        })
    }
}
