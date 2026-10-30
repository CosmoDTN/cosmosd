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
async fn read_side(
    mut src: OwnedReadHalf,
    tx: mpsc::UnboundedSender<(Instant, Vec<u8>)>,
    rate_kbps: u32,
    latency: Duration,
    mut cut: watch::Receiver<bool>,
) {
    let mut buf = vec![0u8; CHUNK];
    loop {
        if !wait_uncut(&mut cut).await {
            return;
        }
        let n = match src.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        // A cut that began during the read holds the chunk until restore.
        if !wait_uncut(&mut cut).await {
            return;
        }
        tokio::time::sleep(send_delay(n, rate_kbps)).await;
        if tx.send((Instant::now() + latency, buf[..n].to_vec())).is_err() {
            return;
        }
    }
}

/// Write chunks once their delivery time has come, in order, never while cut.
async fn write_side(
    mut dst: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<(Instant, Vec<u8>)>,
    mut cut: watch::Receiver<bool>,
) {
    while let Some((deliver_at, chunk)) = rx.recv().await {
        tokio::time::sleep_until(deliver_at).await;
        if !wait_uncut(&mut cut).await {
            return;
        }
        if dst.write_all(&chunk).await.is_err() {
            return;
        }
    }
    let _ = dst.shutdown().await;
}

/// One direction: reader and writer tasks joined by an ordered queue.
async fn pipe(
    src: OwnedReadHalf,
    dst: OwnedWriteHalf,
    rate_kbps: u32,
    latency: Duration,
    cut: watch::Receiver<bool>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let reader = tokio::spawn(read_side(src, tx, rate_kbps, latency, cut.clone()));
    write_side(dst, rx, cut).await;
    reader.abort();
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
    let up = pipe(cr, sw, rate_kbps, latency, cut.clone());
    let down = pipe(sr, cw, rate_kbps, latency, cut);
    // When either direction ends, drop the other so both sockets close.
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
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
}
