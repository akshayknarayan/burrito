use eyre::{eyre, Error};
use tracing::debug;

pub fn reset_root_dir(path: &std::path::Path) {
    debug!(dir = ?&path, "removing");
    std::fs::remove_dir_all(&path).unwrap_or_default();
    debug!(dir = ?&path, "creating");
    std::fs::create_dir_all(&path).unwrap();
}

pub fn start_redis(port: u16) -> Redis {
    Redis::start(port).expect("starting redis")
}

#[must_use]
pub struct Redis {
    port: u16,
}

impl Redis {
    pub fn start(port: u16) -> Result<Self, Error> {
        let name = format!("test-burritoctl-redis-{:?}", port);
        kill_redis(port);

        let mut redis = std::process::Command::new("sudo")
            .args(&[
                "docker",
                "run",
                "--name",
                &name,
                "-d",
                "-p",
                &format!("{}:6379", port),
                "redis:5",
            ])
            .spawn()?;

        std::thread::sleep(std::time::Duration::from_millis(100));

        if let Ok(Some(_)) = redis.try_wait() {
            return Err(eyre!("Could not start redis"));
        }

        let red_conn_string = format!("redis://localhost:{}", port);
        let cl = redis::Client::open(red_conn_string.as_str())?;
        while cl.get_connection().is_err() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let s = Self { port };
        debug!(url = ?s.get_addr(), "started redis");
        Ok(s)
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }

    pub fn get_addr(&self) -> String {
        format!("redis://localhost:{}", self.get_port())
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        kill_redis(self.port);
    }
}

fn kill_redis(port: u16) {
    let name = format!("test-burritoctl-redis-{:?}", port);
    let mut kill = std::process::Command::new("sudo")
        .args(&["docker", "rm", "-f", &name])
        .spawn()
        .expect("Could not spawn docker rm");

    kill.wait().expect("Error waiting on docker rm");
}
