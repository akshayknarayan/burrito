use std::io::Write;

use eyre::{ensure, Error, WrapErr};
use tempfile::NamedTempFile;
use tracing::{debug, info};

pub fn reset_root_dir(path: &std::path::Path) {
    debug!(dir = ?&path, "removing");
    std::fs::remove_dir_all(path).unwrap_or_default();
    debug!(dir = ?&path, "creating");
    std::fs::create_dir_all(path).unwrap();
}

pub fn start_redis(port: u16) -> Redis {
    Redis::start_k8s(port).expect("starting redis")
}

#[must_use]
pub struct Redis {
    port: u16,
    tempfile: NamedTempFile,
    port_forward_proc: std::process::Child,
}

impl Redis {
    pub fn start_k8s(port: u16) -> Result<Self, Error> {
        // write yaml
        let yaml = format!(
            "apiVersion: v1
kind: Pod
metadata:
  name: redis-{port}
  labels:
    app: redis-{port}
spec:
  containers:
  - name: redis
    image: redis:latest
    ports:
    - containerPort: {port}
      name: redis-port
",
            port = port,
        );
        let mut tempfile = NamedTempFile::new()?;
        tempfile.write_all(yaml.as_bytes())?;

        ensure!(
            std::process::Command::new("microk8s")
                .args([
                    "kubectl",
                    "apply",
                    "--wait=true",
                    "-f",
                    tempfile.path().to_str().unwrap()
                ])
                .status()?
                .success(),
            "failed to apply redis yaml: {:?}",
            yaml
        );

        let child = std::process::Command::new("microk8s")
            .args([
                "kubectl",
                "port-forward",
                &format!("redis-{}", port),
                &format!("{port}:{port}", port = port),
            ])
            .spawn()?;

        let s = Self {
            port,
            tempfile,
            port_forward_proc: child,
        };

        s.config()?;
        debug!(url = ?s.get_addr(), "started redis");
        Ok(s)
    }

    fn config(&self) -> Result<(), Error> {
        let red_conn_string = format!("redis://localhost:{}", self.port);
        info!(?red_conn_string, "connecting to redis");
        let cl = redis::Client::open(red_conn_string.as_str()).wrap_err("Connect to redis")?;
        loop {
            match cl.get_connection() {
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
                Ok(mut c) => {
                    redis::cmd("CONFIG")
                        .arg("SET")
                        .arg("notify-keyspace-events")
                        .arg("KEA")
                        .query::<()>(&mut c)?;
                    return Ok(());
                }
            }
        }
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }

    pub fn get_addr(&self) -> String {
        format!("redis://127.0.0.1:{}", self.get_port())
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        debug!(port = ?self.port, "dropped redis handle");
        self.port_forward_proc.kill().unwrap();
        assert!(std::process::Command::new("microk8s")
            .args([
                "kubectl",
                "delete",
                "-f",
                self.tempfile.path().to_str().unwrap()
            ])
            .status()
            .unwrap()
            .success());
    }
}
