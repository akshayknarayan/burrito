use super::{Client, Mode};
use color_eyre::eyre::{ensure, eyre, Report, WrapErr};
use futures_util::{
    future::TryFutureExt, stream::FuturesUnordered, Stream, StreamExt, TryStreamExt,
};
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Barrier;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::Instrument;
use tracing::{debug, debug_span, info, trace, warn};

pub fn kernel_tcp_inline_raw(port: u16, mode: Mode, num_threads: usize) -> Result<(), Report> {
    if let Mode::Client(mut cl) = mode {
        let download_size = cl.download_size;
        let packet_size = cl.packet_size;
        let num_clients = cl.num_clients;
        let of = cl.out_file.take();
        let (tot_bytes, tot_pkts, elapsed) = run_clients(cl, port, num_threads)?;
        super::write_results(
            of,
            tot_bytes,
            tot_pkts,
            elapsed,
            num_clients,
            download_size,
            packet_size,
        )?;
        Ok(())
    } else {
        run_server(port, num_threads)
    }
}

fn run_clients(
    c: Client,
    port: u16,
    num_threads: usize,
) -> Result<(usize, usize, Duration), Report> {
    let addr = SocketAddrV4::new(c.addr, port);
    let start_barrier = Arc::new(Barrier::new(c.num_clients));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let joined: Vec<_> = rt.block_on(async move {
        let client_threads: FuturesUnordered<_> = if c.num_clients > num_threads {
            let clients_per_thread = c.num_clients / num_threads;
            let mut remainder = c.num_clients - (num_threads * clients_per_thread);
            (0..num_threads)
                .map(|thread| {
                    let start_barrier = start_barrier.clone();
                    let num_thread_clients = if remainder > 0 {
                        remainder -= 1;
                        clients_per_thread + 1
                    } else {
                        clients_per_thread
                    };
                    let start_barrier = start_barrier.clone();
                    tokio::task::spawn(async move {
                        let futs: FuturesUnordered<_> = (0..num_thread_clients)
                            .map(|tclient| {
                                let start_barrier = start_barrier.clone();
                                run_client(addr, c.download_size, c.packet_size, start_barrier)
                                    .instrument(debug_span!("client", ?thread, ?tclient))
                            })
                            .collect();
                        Ok::<_, Report>(futs.try_collect().await?)
                    })
                })
                .collect()
        } else {
            (0..c.num_clients)
                .map(|thread| {
                    let start_barrier = start_barrier.clone();
                    tokio::task::spawn(async move {
                        let res = run_client(addr, c.download_size, c.packet_size, start_barrier)
                            .instrument(debug_span!("client", ?thread))
                            .await?;
                        Ok(vec![res])
                    })
                })
                .collect()
        };

        client_threads.map(|f| f.unwrap()).collect().await
    });

    let (tot_bytes, tot_pkts, elapsed) = joined
        .into_iter()
        .collect::<Result<Vec<Vec<_>>, _>>()
        .wrap_err("failed running one or more clients")?
        .into_iter()
        .flatten()
        .reduce(|(b, p, d), (bi, pi, di)| (b + bi, p + pi, std::cmp::max(d, di)))
        .expect("There should be at least one client thread");
    info!(?tot_bytes, ?tot_pkts, ?elapsed, "all clients done");
    Ok((tot_bytes, tot_pkts, elapsed))
}

const MAGIC_BYTES: u64 = 0x1_02_03_04_05_06_07_08;

async fn run_client(
    addr: SocketAddrV4,
    download_size: usize,
    packet_size: usize,
    start_barrier: Arc<Barrier>,
) -> Result<(usize, usize, Duration), Report> {
    let mut cn = tokio::net::TcpStream::connect(SocketAddr::V4(addr))
        .await
        .wrap_err("connect failed")?;

    let local_addr = cn.local_addr();
    let remote_addr = addr;
    info!(
        ?remote_addr,
        ?local_addr,
        ?download_size,
        ?packet_size,
        "starting client"
    );

    let mut tot_bytes = 0;
    let mut tot_pkts = 0;

    start_barrier.wait().await;
    // send the request
    cn.write_u64(MAGIC_BYTES).await?;
    cn.write_u64(download_size as _).await?;
    cn.write_u64(packet_size as _).await?;

    let start = Instant::now();
    let mut last_recv_time;
    let mut read_buf = vec![0u8; packet_size];
    loop {
        let next_size = cn.read_u32().await?;
        let rd = cn.read_exact(&mut read_buf[..next_size as usize]).await?;
        last_recv_time = Instant::now();

        tot_bytes += rd;
        tot_pkts += 1;
        trace!(?tot_bytes, ?tot_pkts, "received part");
        if read_buf[0] == 1 {
            debug!("received fin");
            cn.write_u32(next_size).await?;
            cn.write_all(&read_buf[..rd]).await?;
            break;
        }
    }

    let elapsed = last_recv_time - start;
    info!(?tot_bytes, ?tot_pkts, ?elapsed, "done");
    Ok((tot_bytes, tot_pkts, elapsed))
}

pub fn run_server(port: u16, threads: usize) -> Result<(), Report> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    async fn server_thread(port: u16, thread: usize) -> Result<(), Report> {
        let sk = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        let a: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        sk.set_reuse_port(true)?;
        sk.set_nonblocking(true)?;
        sk.bind(&a.into())?;
        sk.listen(16)?;
        let l = TcpListener::from_std(sk.into())
            .wrap_err_with(|| eyre!("socket bind failed on {:?}", a))?;
        let st = TcpListenerStream::new(l).map_err(Into::into);
        server_thread_inner(st)
            .instrument(debug_span!("server_thread", ?thread))
            .await?;
        unreachable!()
    }

    rt.block_on(async move {
        info!(?port, ?threads, "starting server");
        for i in 1..threads {
            tokio::task::spawn(server_thread(port, i));
        }

        server_thread(port, 0).await
    })
}

async fn server_thread_inner<S: Stream<Item = Result<TcpStream, Report>> + Unpin>(
    st: S,
) -> Result<(), Report> {
    st.try_for_each_concurrent(None, |mut cn| {
        let remote_addr = cn
            .peer_addr()
            .expect("Connection was not connected to remote address");
        async move {
            let req_magic_bytes = cn.read_u64().await?;
            ensure!(req_magic_bytes == MAGIC_BYTES, "request malformed");
            let mut remaining = cn.read_u64().await? as usize;
            let pkt_size = cn.read_u64().await? as usize;
            let start = Instant::now();
            info!(?remaining, ?pkt_size, ?remote_addr, "starting send");
            while remaining > 0 {
                let this_send_size = std::cmp::min(pkt_size, remaining);
                remaining -= this_send_size;
                let write_buf = vec![3u8; this_send_size];
                cn.write_u32(this_send_size as u32).await?;
                cn.write_all(&write_buf[..]).await?;
            }
            info!(elapsed = ?start.elapsed(), ?remote_addr, "done sending");

            cn.write_u32(1).await?;
            cn.write_all(&[1]).await?;
            debug!("wait for fin ack");
            ensure!(1 == cn.read_u32().await?, "wrong fin message len");
            ensure!(1 == cn.read_u8().await?, "wrong fin message body");
            info!(elapsed = ?start.elapsed(), ?remote_addr, "exiting");
            Ok::<_, Report>(())
        }
        .instrument(debug_span!("client_conn", ?remote_addr))
        .map_err(move |err| {
            warn!(?err, ?remote_addr, "client conn errored");
            err
        })
    })
    .await
}
