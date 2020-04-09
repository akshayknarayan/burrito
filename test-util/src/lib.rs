use anyhow::Error;
use slog::debug;

pub fn test_logger() -> slog::Logger {
    use slog::Drain;
    let plain = slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
    let drain = slog_term::FullFormat::new(plain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

pub async fn block_for(d: std::time::Duration) {
    tokio::time::delay_for(d).await;
}

pub fn reset_root_dir(path: &std::path::Path, log: &slog::Logger) {
    debug!(log, "removing"; "dir" => ?&path);
    std::fs::remove_dir_all(&path).unwrap_or_default();
    debug!(log, "creating"; "dir" => ?&path);
    std::fs::create_dir_all(&path).unwrap();
}

pub fn start_redis(log: &slog::Logger, port: u16) -> Redis {
    Redis::start(log, port).expect("starting redis")
}

#[must_use]
pub struct Redis {
    port: u16,
}

impl Redis {
    pub fn start(log: &slog::Logger, port: u16) -> Result<Self, Error> {
        let name = format!("test-burritoctl-redis-{:?}", port);
        kill_redis(port);

        let mut redis = std::process::Command::new("sudo")
            .args(&[
                "DOCKER_HOST=unix:///var/run/burrito-docker.sock",
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
            Err(anyhow::anyhow!("Could not start redis"))?;
        }

        let red_conn_string = format!("redis://localhost:{}", port);
        let cl = redis::Client::open(red_conn_string.as_str())?;
        while let Err(_) = cl.get_connection() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let s = Self { port };
        debug!(&log, "started redis"; "url" => s.get_addr());
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
        .args(&[
            "DOCKER_HOST=unix:///var/run/burrito-docker.sock",
            "docker",
            "rm",
            "-f",
            &name,
        ])
        .spawn()
        .expect("Could not spawn docker rm");

    kill.wait().expect("Error waiting on docker rm");
}
