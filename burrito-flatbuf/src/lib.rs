pub use flatbuffers::FlatBufferBuilder;

#[allow(unused_imports, unused)]
mod serialize {
    include!(concat!(env!("OUT_DIR"), "/burrito_generated.rs"));
}

burrito_util::assign_const_vals!(LISTEN_REPLY, LISTEN_REQUEST, OPEN_REPLY, OPEN_REQUEST);

#[derive(Debug, Clone)]
pub struct ListenReply {
    pub addr: String,
}

impl ListenReply {
    pub fn onto(&self, msg: &mut flatbuffers::FlatBufferBuilder) {
        let addr_field = msg.create_string(&self.addr);
        let req = serialize::ListenReply::create(
            msg,
            &serialize::ListenReplyArgs {
                listen_addr: Some(addr_field),
            },
        );

        msg.finish(req, None);
    }
}

impl From<&'_ [u8]> for ListenReply {
    fn from(msg: &[u8]) -> Self {
        let m = flatbuffers::get_root::<serialize::ListenReply>(msg);
        Self {
            addr: m.listen_addr().unwrap().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListenRequest {
    pub service_addr: String,
    pub port: u16,
}

impl ListenRequest {
    pub fn onto(&self, msg: &mut flatbuffers::FlatBufferBuilder) {
        let service_addr_field = msg.create_string(&self.service_addr);
        let req = serialize::ListenRequest::create(
            msg,
            &serialize::ListenRequestArgs {
                service_addr: Some(service_addr_field),
                listen_port: self.port,
            },
        );

        msg.finish(req, None);
    }
}

impl From<&'_ [u8]> for ListenRequest {
    fn from(msg: &[u8]) -> Self {
        let m = flatbuffers::get_root::<serialize::ListenRequest>(msg);
        Self {
            service_addr: m.service_addr().unwrap().to_string(),
            port: m.listen_port(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OpenReply {
    Unix(String),
    Tcp(String),
}

impl OpenReply {
    pub fn onto(&self, msg: &mut flatbuffers::FlatBufferBuilder) {
        let addr_field = match self {
            OpenReply::Unix(s) | OpenReply::Tcp(s) => msg.create_string(s),
        };

        let req = serialize::OpenReply::create(
            msg,
            &serialize::OpenReplyArgs {
                send_addr: Some(addr_field),
                addr_type: match self {
                    OpenReply::Unix(_) => serialize::AddrType::Unix,
                    OpenReply::Tcp(_) => serialize::AddrType::Tcp,
                },
            },
        );

        msg.finish(req, None);
    }
}

impl From<&'_ [u8]> for OpenReply {
    fn from(msg: &[u8]) -> Self {
        let open_reply_reader = flatbuffers::get_root::<serialize::OpenReply>(msg);

        match open_reply_reader.addr_type() {
            serialize::AddrType::Unix => {
                OpenReply::Unix(open_reply_reader.send_addr().unwrap().to_string())
            }
            serialize::AddrType::Tcp => {
                OpenReply::Tcp(open_reply_reader.send_addr().unwrap().to_string())
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenRequest {
    pub dst_addr: String,
}

impl OpenRequest {
    pub fn onto(&self, msg: &mut flatbuffers::FlatBufferBuilder) {
        let dst_addr_field = msg.create_string(&self.dst_addr);
        let req = serialize::OpenRequest::create(
            msg,
            &serialize::OpenRequestArgs {
                dst_addr: Some(dst_addr_field),
            },
        );

        msg.finish(req, None);
    }
}

impl From<&'_ [u8]> for OpenRequest {
    fn from(msg: &[u8]) -> Self {
        let m = flatbuffers::get_root::<serialize::OpenRequest>(msg);
        Self {
            dst_addr: m.dst_addr().unwrap().to_string(),
        }
    }
}
