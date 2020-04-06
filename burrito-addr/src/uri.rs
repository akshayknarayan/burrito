use failure::{ensure, format_err, Error};

/// Burrito-aware URI type.
pub struct Uri<'a> {
    /// path + query string
    inner: std::borrow::Cow<'a, str>,
}

impl<'a> Into<hyper::Uri> for Uri<'a> {
    fn into(self) -> hyper::Uri {
        self.inner.as_ref().parse().unwrap()
    }
}

impl<'a> Uri<'a> {
    pub fn new(addr: &str) -> Self {
        Self::new_with_path(addr, "/")
    }

    pub fn new_with_path(addr: &str, path: &'a str) -> Self {
        let host = hex::encode(addr.as_bytes());
        let s = format!("burrito://{}:0{}", host, path);
        Uri {
            inner: std::borrow::Cow::Owned(s),
        }
    }

    pub fn socket_path(uri: &hyper::Uri) -> Result<String, Error> {
        ensure!(
            uri.scheme_str().map(|s| s == "burrito").is_some(),
            "URL scheme does not match: {:?}",
            uri.scheme_str()
        );

        use hex::FromHex;
        uri.host()
            .iter()
            .filter_map(|host| {
                Vec::from_hex(host)
                    .ok()
                    .map(|raw| String::from_utf8_lossy(&raw).into_owned())
            })
            .next()
            .ok_or_else(|| format_err!("Could not get socket path for Destination"))
    }

    pub fn burrito_addr(uri: &hyper::Uri) -> Result<burrito_localname_ctl::proto::Addr, Error> {
        let s = Uri::socket_path(uri)?;
        Ok(burrito_localname_ctl::proto::Addr::Burrito(s))
    }
}
