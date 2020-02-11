use failure::Error;
use futures_util::stream::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tracing::trace;

#[derive(Debug, Clone)]
pub struct StaticResolver {
    root: std::path::PathBuf,
    response: String,
}

impl StaticResolver {
    pub fn new(root: std::path::PathBuf, val: &str) -> Self {
        StaticResolver {
            root,
            response: val.into(),
        }
    }

    pub fn listen_path(&self) -> std::path::PathBuf {
        self.root.join(crate::burrito_net::CONTROLLER_ADDRESS)
    }

    #[tracing::instrument(level = "debug")]
    pub async fn start(self) -> Result<(), Error> {
        // bind
        let mut sk = UnixListener::bind(&self.root)?;
        let r = self.response;
        // listen
        sk.incoming()
            .for_each_concurrent(None, |st| {
                async {
                    trace!("new incoming conn");
                    st.unwrap().write_all(r.as_bytes()).await.unwrap();
                    trace!("wrote to conn");
                }
            })
            .await;
        Ok(())
    }
}
