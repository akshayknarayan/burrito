use burrito_addr::staticnet::*;
use burrito_ctl::StaticResolver;
use criterion::{criterion_group, criterion_main, Criterion};
use std::cell::RefCell;

async fn ping() {
    // ping from a client
    let mut sc = StaticClient::new(std::path::PathBuf::from("./tmp-test-sr")).await;
    let addr: hyper::Uri = burrito_addr::Uri::new("staticping").into();
    let (addr, _) = sc.resolve(addr).await.unwrap();
    assert_eq!(addr, "127.0.0.1:4242");
}

async fn ping_reuse(sc: &RefCell<burrito_addr::staticnet::StaticClient>) {
    let mut sc = sc.borrow_mut();
    let addr: hyper::Uri = burrito_addr::Uri::new("staticping").into();
    let (addr, _) = sc.resolve(addr).await.unwrap();
    assert_eq!(addr, "127.0.0.1:4242");
}

fn unix_ping(c: &mut Criterion) {
    std::fs::remove_dir_all("./tmp-test-sr/").unwrap_or_default();
    std::fs::create_dir_all("./tmp-test-sr/").unwrap();

    // start the server
    std::thread::spawn(|| {
        let sr = StaticResolver::new(
            std::path::PathBuf::from("./tmp-test-sr"),
            "127.0.0.1:4242",
            "tcp",
        );

        tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .unwrap()
            .block_on(sr.start())
            .expect("static burrito crashed")
    });

    std::thread::sleep(std::time::Duration::from_millis(100));

    let mut group = c.benchmark_group("unix ping");

    group.bench_function("unix ping", |b| {
        tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                b.iter(|| criterion::black_box(ping()));
            })
    });

    group.bench_function("unix ping with client reuse", |b| {
        tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let sc = RefCell::new(
                    burrito_addr::staticnet::StaticClient::new(std::path::PathBuf::from(
                        "./tmp-test-sr",
                    ))
                    .await,
                );
                b.iter(|| criterion::black_box(ping_reuse(&sc)));
            });
    });

    group.finish();
}

criterion_group!(benches, unix_ping);
criterion_main!(benches);
