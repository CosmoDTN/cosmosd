//! linksim: a TCP proxy that imitates a satellite link.
//!
//! Rate-limited, delayed by a fixed one-way latency, and cuttable like a
//! blackhole: SIGUSR1 stops all forwarding (sockets stay open, nothing is
//! reset), SIGUSR2 restores it.

use std::time::Duration;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

const CHUNK: usize = 16 * 1024;

#[derive(Parser)]
#[command(about = "Rate- and latency-limited TCP proxy that can be cut with SIGUSR1 and restored with SIGUSR2")]
struct Args {
    /// Address to listen on
    #[arg(long)]
    listen: String,
    /// Address to forward to
    #[arg(long)]
    upstream: String,
    /// Link rate in kilobits per second
    #[arg(long)]
    rate_kbps: u32,
    /// One-way latency in milliseconds
    #[arg(long)]
    latency_ms: u64,
}

// === Pacing ===

/// Time `bytes` occupy a link of `rate_kbps`.
fn send_delay(bytes: usize, rate_kbps: u32) -> Duration {
    let bits = bytes as u128 * 8;
    let bps = (rate_kbps.max(1) as u128) * 1000;
    Duration::from_nanos((bits * 1_000_000_000 / bps) as u64)
}

// === Cut flag ===

/// Block while the link is cut. Returns false if the controller is gone.
async fn wait_uncut(cut: &mut watch::Receiver<bool>) -> bool {
    while *cut.borrow_and_update() {
        if cut.changed().await.is_err() {
            return false;
        }
    }
    true
}

// === Forwarding ===

/// Read chunks, pace them, and stamp each with its delivery time.
/// Ok on EOF (dropping `tx` lets the writer drain and finish).
async fn read_side(
    mut src: OwnedReadHalf,
    tx: mpsc::UnboundedSender<(Instant, Vec<u8>)>,
    rate_kbps: u32,
    latency: Duration,
    mut cut: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let gone = || std::io::Error::other("cut controller gone");
    let mut buf = vec![0u8; CHUNK];
    loop {
        if !wait_uncut(&mut cut).await {
            return Err(gone());
        }
        let n = src.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        // A cut that began during the read holds the chunk until restore.
        if !wait_uncut(&mut cut).await {
            return Err(gone());
        }
        tokio::time::sleep(send_delay(n, rate_kbps)).await;
        if tx.send((Instant::now() + latency, buf[..n].to_vec())).is_err() {
            return Err(gone());
        }
    }
}

/// Write chunks once their delivery time has come, in order, never while cut.
/// After the queue is drained (reader hit EOF) the FIN is propagated.
async fn write_side(
    mut dst: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<(Instant, Vec<u8>)>,
    mut cut: watch::Receiver<bool>,
) -> std::io::Result<()> {
    while let Some((deliver_at, chunk)) = rx.recv().await {
        tokio::time::sleep_until(deliver_at).await;
        if !wait_uncut(&mut cut).await {
            return Err(std::io::Error::other("cut controller gone"));
        }
        dst.write_all(&chunk).await?;
    }
    dst.shutdown().await
}

/// One direction: reader and writer joined by an ordered queue. No spawned
/// tasks, so dropping this future stops both. An error in either ends it.
async fn pipe(
    src: OwnedReadHalf,
    dst: OwnedWriteHalf,
    rate_kbps: u32,
    latency: Duration,
    cut: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::try_join!(
        read_side(src, tx, rate_kbps, latency, cut.clone()),
        write_side(dst, rx, cut)
    )
    .map(|_| ())
}

async fn handle(
    client: TcpStream,
    upstream: String,
    rate_kbps: u32,
    latency: Duration,
    mut cut: watch::Receiver<bool>,
) {
    // Accepted during a cut: hold off connecting upstream until restore.
    if !wait_uncut(&mut cut).await {
        return;
    }
    let server = match TcpStream::connect(&upstream).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("linksim: upstream {upstream}: {e}");
            return;
        }
    };
    let (cr, cw) = client.into_split();
    let (sr, sw) = server.into_split();
    // Each direction ends on EOF (FIN propagated); the connection is done
    // when both have, or as soon as either fails, which drops both.
    let _ = tokio::try_join!(
        pipe(cr, sw, rate_kbps, latency, cut.clone()),
        pipe(sr, cw, rate_kbps, latency, cut)
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind(&args.listen).await?;
    eprintln!(
        "linksim: {} -> {} at {} kbps, {} ms one-way",
        args.listen, args.upstream, args.rate_kbps, args.latency_ms
    );

    let (cut_tx, cut_rx) = watch::channel(false);
    let mut usr1 = signal(SignalKind::user_defined1())?;
    let mut usr2 = signal(SignalKind::user_defined2())?;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = usr1.recv() => { let _ = cut_tx.send(true); eprintln!("linksim: cut"); }
                _ = usr2.recv() => { let _ = cut_tx.send(false); eprintln!("linksim: restored"); }
            }
        }
    });

    let latency = Duration::from_millis(args.latency_ms);
    loop {
        let (client, _) = listener.accept().await?;
        tokio::spawn(handle(client, args.upstream.clone(), args.rate_kbps, latency, cut_rx.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_delay_matches_rate() {
        assert_eq!(send_delay(16_000, 3000).as_micros(), 42_666);
        assert_eq!(send_delay(0, 3000), Duration::ZERO);
    }

    /// A client that sends a request and half-closes still gets the whole
    /// response, in order, then EOF.
    #[tokio::test]
    async fn half_close_keeps_response() {
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();

        // Upstream: read the request to EOF, then reply with it and close.
        let up = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = up.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = up.accept().await.unwrap();
            let mut req = Vec::new();
            s.read_to_end(&mut req).await.unwrap();
            s.write_all(&req).await.unwrap();
        });

        let (_cut_tx, cut_rx) = watch::channel(false);
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        tokio::spawn(async move {
            let (c, _) = proxy.accept().await.unwrap();
            handle(c, up_addr, 1_000_000, Duration::from_millis(5), cut_rx).await;
        });

        let mut c = TcpStream::connect(proxy_addr).await.unwrap();
        c.write_all(&payload).await.unwrap();
        c.shutdown().await.unwrap();
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), c.read_to_end(&mut got))
            .await
            .expect("timed out")
            .unwrap();
        assert_eq!(got, payload);
    }
}
