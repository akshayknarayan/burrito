use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::stream::{StreamExt, TryStreamExt};
use incoming::IntoIncoming;
use kvstore_ycsb::{ops, Op};
use std::error::Error;
use std::str::FromStr;
use tower_service::Service;

type StdError = Box<dyn Error + Send + Sync + 'static>;

async fn serve_tcp() -> Result<(), StdError> {
    let l = tokio::net::TcpListener::bind("127.0.0.1:57282").await?;
    let l = l.into_incoming().map_ok(|st| {
        st.set_nodelay(true).unwrap();
        st
    });
    kvstore::shard_server(vec![l], |_| 0).await
}

fn ycsb_wrkb_ser_tcp(c: &mut Criterion) {
    c.bench_function("ycsb_wrkloadb_ser_tcp", |b| {
        let mut rt = tokio::runtime::Runtime::new().unwrap();

        let mut cl = rt.block_on(async {
            // start server
            tokio::spawn(serve_tcp());

            // client
            let mut resolver = hyper::client::connect::HttpConnector::new();
            resolver.set_nodelay(true);
            let addr: hyper::Uri = hyper::Uri::from_str("http://127.0.0.1:57282").unwrap();
            futures_util::future::poll_fn(|cx| resolver.poll_ready(cx))
                .await
                .unwrap();
            let st = resolver.call(addr).await.expect("resolver");
            let mut cl = kvstore::Client::from_stream(st);

            // don't need to time the loads.
            for o in
                ops(std::path::PathBuf::from("./ycsbc-mock/wrkloadb5000-1.load")).expect("loads")
            {
                match o {
                    Op::Get(_, k) => cl.get(k).await.unwrap(),
                    Op::Update(_, k, v) => cl.update(k, v).await.unwrap(),
                };
            }

            cl
        });

        let mut accesses = ops(std::path::PathBuf::from(
            "./ycsbc-mock/wrkloadb5000-1.access",
        ))
        .expect("accesses")
        .into_iter()
        .cycle();

        b.iter(|| {
            rt.block_on(async {
                let o = accesses.next().unwrap();
                match o {
                    Op::Get(_, k) => cl.get(k).await.unwrap(),
                    Op::Update(_, k, v) => cl.update(k, v).await.unwrap(),
                };
            });
        });
    });
}

async fn serve_unix(ready: tokio::sync::oneshot::Sender<()>) {
    std::fs::remove_file("./bench-unix").unwrap_or_default();
    let l = tokio::net::UnixListener::bind("./bench-unix")
        .expect("unix bind")
        .into_incoming();
    ready.send(()).expect("ready sender");
    kvstore::shard_server(vec![l], |_| 0).await.unwrap();
}

fn ycsb_wrkb_ser_unix(c: &mut Criterion) {
    c.bench_function("ycsb_wrkloadb_ser_unix", |b| {
        let mut rt = tokio::runtime::Runtime::new().unwrap();

        let mut cl = rt.block_on(async {
            let (s, r) = tokio::sync::oneshot::channel();

            // start server
            tokio::spawn(serve_unix(s));
            r.await.expect("recv ready sender");

            // client
            let mut resolver = hyper_unix_connector::UnixClient;
            let addr: hyper::Uri = hyper_unix_connector::Uri::new("./bench-unix", "/").into();
            futures_util::future::poll_fn(|cx| resolver.poll_ready(cx))
                .await
                .unwrap();
            let st = resolver.call(addr).await.expect("resolver");
            let mut cl = kvstore::Client::from_stream(st);

            // don't need to time the loads.
            for o in
                ops(std::path::PathBuf::from("./ycsbc-mock/wrkloadb5000-1.load")).expect("loads")
            {
                match o {
                    Op::Get(_, k) => cl.get(k).await.unwrap(),
                    Op::Update(_, k, v) => cl.update(k, v).await.unwrap(),
                };
            }

            cl
        });

        let mut accesses = ops(std::path::PathBuf::from(
            "./ycsbc-mock/wrkloadb5000-1.access",
        ))
        .expect("accesses")
        .into_iter()
        .cycle();

        b.iter(|| {
            rt.block_on(async {
                let o = accesses.next().unwrap();
                match o {
                    Op::Get(_, k) => cl.get(k).await.unwrap(),
                    Op::Update(_, k, v) => cl.update(k, v).await.unwrap(),
                };
            });
        });
    });
}

async fn serve_chan(
    inc: tokio::sync::mpsc::Receiver<kvstore::Msg>,
    out: tokio::sync::mpsc::Sender<kvstore::Msg>,
) {
    let st = kvstore::Store::default();
    inc.fold(st, |mut st, m| async {
        let mut o = out.clone();
        futures_util::future::poll_fn(|cx| st.poll_ready(cx))
            .await
            .unwrap();
        let resp = st.call(m).await.expect("serv");
        o.send(resp).await.expect("snd");
        st
    })
    .await;
}

fn ycsb_wrkb_chan(c: &mut Criterion) {
    c.bench_function("ycsb_wrkloadb_chan", |b| {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (mut s1, r1) = tokio::sync::mpsc::channel(10);
        let (s2, mut r2) = tokio::sync::mpsc::channel(10);

        rt.block_on(async {
            // start server
            tokio::spawn(serve_chan(r1, s2));

            for o in
                ops(std::path::PathBuf::from("./ycsbc-mock/wrkloadb5000-1.load")).expect("loads")
            {
                let req = match o {
                    Op::Get(_, k) => kvstore::Msg::get_req(k),
                    Op::Update(_, k, v) => kvstore::Msg::put_req(k, v),
                };

                s1.send(req).await.unwrap();
                r2.recv().await.unwrap(); // meh, toss the response
            }
        });

        let mut accesses = ops(std::path::PathBuf::from(
            "./ycsbc-mock/wrkloadb5000-1.access",
        ))
        .expect("accesses")
        .into_iter()
        .cycle();

        b.iter(|| {
            rt.block_on(async {
                let o = accesses.next().unwrap();
                let req = match o {
                    Op::Get(_, k) => kvstore::Msg::get_req(k),
                    Op::Update(_, k, v) => kvstore::Msg::put_req(k, v),
                };

                s1.send(req).await.unwrap();
                r2.recv().await.unwrap(); // meh, toss the response
            });
        });
    });
}

fn ycsb_wrkb_msgkv(c: &mut Criterion) {
    c.bench_function("ycsb_wrkb_msgkv", |b| {
        let mut kv = kvstore::Kv::default();

        for o in ops(std::path::PathBuf::from("./ycsbc-mock/wrkloadb5000-1.load")).expect("loads") {
            match o {
                Op::Get(_, k) => kv.get(&k).map(|s| s.to_owned()),
                Op::Update(_, k, v) => kv.put(&k, Some(v)),
            };
        }

        let mut store = kvstore::Store::from(kv);
        let mut rt = tokio::runtime::Runtime::new().unwrap();

        let mut accesses = ops(std::path::PathBuf::from(
            "./ycsbc-mock/wrkloadb5000-1.access",
        ))
        .expect("accesses")
        .into_iter()
        .cycle();

        b.iter(|| {
            let o = accesses.next().unwrap();
            let req = match o {
                Op::Get(_, k) => kvstore::Msg::get_req(k),
                Op::Update(_, k, v) => kvstore::Msg::put_req(k, v),
            };

            rt.block_on(async {
                futures_util::future::poll_fn(|cx| store.poll_ready(cx))
                    .await
                    .unwrap();
                store.call(req).await.expect("serv"); // throw away response
            });
        });
    });
}

fn ycsb_wrkb_kv(c: &mut Criterion) {
    c.bench_function("ycsb_wrkb_kv", |b| {
        let mut kv = kvstore::Kv::default();

        for o in ops(std::path::PathBuf::from("./ycsbc-mock/wrkloadb5000-1.load")).expect("loads") {
            match o {
                Op::Get(_, k) => kv.get(&k).map(|s| s.to_owned()),
                Op::Update(_, k, v) => kv.put(&k, Some(v)),
            };
        }

        let mut accesses = ops(std::path::PathBuf::from(
            "./ycsbc-mock/wrkloadb5000-1.access",
        ))
        .expect("accesses")
        .into_iter()
        .cycle();

        b.iter(|| {
            let o = accesses.next().unwrap();
            match o {
                Op::Get(_, k) => kv.get(&k).map(|s| s.to_owned()),
                Op::Update(_, k, v) => kv.put(&k, Some(v)),
            };
        });
    });
}

async fn serve_shards(
    num_shards: usize,
    shard_fn: impl Fn(&kvstore::Msg) -> usize + 'static,
) -> Result<(), StdError> {
    let ls: Result<Vec<_>, _> =
        futures_util::future::join_all((0..num_shards + 1).map(|i| async move {
            let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 57283 + i).parse()?;
            let l = tokio::net::TcpListener::bind(addr).await?;
            Ok::<_, StdError>(l.into_incoming().map_ok(|st| {
                st.set_nodelay(true).unwrap();
                st
            }))
        }))
        .await
        .into_iter()
        .collect();
    kvstore::shard_server(ls?, shard_fn).await
}

fn ycsb_wrkb_scaling(num_shards: usize, c: &mut Criterion) {
    c.bench_function(&format!("ycsb_wrkloadb_{}shards", num_shards), |b| {
        let mut rt = tokio::runtime::Runtime::new().unwrap();

        let mut cl = rt.block_on(async {
            let shard_fn = move |m: &kvstore::Msg| {
                use std::hash::{Hash, Hasher};
                let mut hasher = ahash::AHasher::default();
                m.key().hash(&mut hasher);
                hasher.finish() as usize % num_shards
            };
            tokio::spawn(serve_shards(num_shards, shard_fn));

            // client: connect to sharder base address
            let mut resolver = hyper::client::connect::HttpConnector::new();
            resolver.set_nodelay(true);
            let addr: hyper::Uri = hyper::Uri::from_str("http://127.0.0.1:57283").unwrap();
            futures_util::future::poll_fn(|cx| resolver.poll_ready(cx))
                .await
                .unwrap();
            let st = resolver.call(addr).await.expect("resolver");
            let mut cl = kvstore::Client::from_stream(st);

            // don't need to time the loads.
            for o in
                ops(std::path::PathBuf::from("./ycsbc-mock/wrkloadb5000-1.load")).expect("loads")
            {
                match o {
                    Op::Get(_, k) => cl.get(k).await.unwrap(),
                    Op::Update(_, k, v) => cl.update(k, v).await.unwrap(),
                };
            }

            cl
        });

        let mut accesses = ops(std::path::PathBuf::from(
            "./ycsbc-mock/wrkloadb5000-1.access",
        ))
        .expect("accesses")
        .into_iter()
        .cycle();

        b.iter(|| {
            rt.block_on(async {
                let o = accesses.next().unwrap();
                match o {
                    Op::Get(_, k) => cl.get(k).await.unwrap(),
                    Op::Update(_, k, v) => cl.update(k, v).await.unwrap(),
                };
            });
        });
    });
}

macro_rules! ycsb_wrkb_shards {
    ($name: ident, $n: expr => $($snames:ident),+) => {
        fn $name(c: &mut Criterion) {
            ycsb_wrkb_scaling($n, c)
        }

        criterion_group!(numshards, $($snames),+,$name);
    };
    ($name: ident, $n: expr; $($names: ident, $ns: expr);+ => $($snames:ident),+) => {
        fn $name(c: &mut Criterion) {
            ycsb_wrkb_scaling($n, c)
        }

        ycsb_wrkb_shards!($($names, $ns)+ => $name,$($snames),+);
    };
    ($name: ident, $n: expr; $($names: ident, $ns: expr);+) => {
        fn $name(c: &mut Criterion) {
            ycsb_wrkb_scaling($n, c)
        }

        ycsb_wrkb_shards!($($names, $ns);+ => $name);
    };
}

ycsb_wrkb_shards!(yscb_wrkb_1shard, 1; ycsb_wrkb_2shard, 2; ycsb_wrkb_3shard, 3);

criterion_group!(
    benches,
    ycsb_wrkb_ser_tcp,
    ycsb_wrkb_ser_unix,
    ycsb_wrkb_chan,
    ycsb_wrkb_msgkv,
    ycsb_wrkb_kv,
);
criterion_main!(benches, numshards);
