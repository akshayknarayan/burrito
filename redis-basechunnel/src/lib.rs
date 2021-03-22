// Pin<Box<...>> is necessary and not worth breaking up
#![allow(clippy::type_complexity)]

use bertha::negotiate::Rendezvous;
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use std::sync::Arc;
use std::{future::Future, pin::Pin};
use tokio::sync::Mutex;

pub struct RedisBase {
    redis_conn: Arc<Mutex<redis::aio::Connection>>,
}

impl RedisBase {
    pub async fn new(redis_addr: &str) -> Result<Self, Report> {
        let redis_client = redis::Client::open(redis_addr)
            .wrap_err(eyre!("Opening redis connection: {:?}", redis_addr))?;
        let redis_conn = Arc::new(Mutex::new(
            redis_client
                .get_async_connection()
                .await
                .wrap_err("Connecting to redis")?,
        ));

        Ok(RedisBase { redis_conn })
    }
}

impl<'a> Rendezvous<'a> for RedisBase {
    type Error = Report;
    type Future = Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, Self::Error>> + 'a>>;

    /// Establish semantics via nonce in redis. If semantics already established, return Some(nonce) for
    /// comparison. Else `None` to indicate no one else was there first.
    fn init_or_fetch(&'a mut self, queue_name: String, neg_nonce: Vec<u8>) -> Self::Future {
        Box::pin(async move {
            let mut r = redis::pipe();
            let (set_res, nonce_val): (isize, Vec<u8>) = r
                .atomic()
                .set_nx(&queue_name, &neg_nonce[..])
                .get(queue_name)
                .query_async(&mut *self.redis_conn.lock().await)
                .await?;
            if set_res == 1 {
                // was set
                ensure!(nonce_val == neg_nonce, "Nonce values mismatched");
                Ok(None)
            } else if set_res == 0 {
                // was not set
                Ok(Some(nonce_val))
            } else {
                unreachable!();
            }
        })
    }
}
