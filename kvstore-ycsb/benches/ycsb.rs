use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::stream::StreamExt;
use kvstore_ycsb::{ops, Op};
use std::error::Error;
use std::str::FromStr;
use tower_service::Service;

type StdError = Box<dyn Error + Send + Sync + 'static>;

async fn serve_tcp() -> Result<(), StdError> {
    let mut l = tokio::net::TcpListener::bind("127.0.0.1:57282").await?;
    l.incoming()
        .for_each_concurrent(None, |st| {
            async move {
                kvstore::server(st.unwrap()).await.unwrap();
            }
        })
        .await;

    Ok(())
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
            let mut cl = kvstore::Client::from(st);

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
    let mut l = tokio::net::UnixListener::bind("./bench-unix").expect("unix bind");
    ready.send(()).expect("ready sender");
    l.incoming()
        .for_each_concurrent(None, |st| {
            async move {
                kvstore::server(st.unwrap()).await.unwrap();
            }
        })
        .await;
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
            let mut cl = kvstore::Client::from(st);

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
    inc.fold(st, |mut st, m| {
        async {
            let mut o = out.clone();
            futures_util::future::poll_fn(|cx| st.poll_ready(cx))
                .await
                .unwrap();
            let resp = st.call(m).await.expect("serv");
            o.send(resp).await.expect("snd");
            st
        }
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

criterion_group!(
    benches,
    ycsb_wrkb_ser_tcp,
    ycsb_wrkb_ser_unix,
    ycsb_wrkb_chan
);
criterion_main!(benches);
