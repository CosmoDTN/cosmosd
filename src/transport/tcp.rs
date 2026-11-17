//! TCP convergence layer — reference implementation.
//!
//! Bundles are sent as length-prefixed RFC 9171 bytes over a TCP stream.
//! Production deployments should use Reticulum or LoRa for disconnected links;
//! TCP is for LAN demos and testing.

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

use super::{BundleListener, BundleStream, ConvergenceLayer, PeerAddr};
use crate::bundle::Bundle;
use crate::wire::{decode_bundle, encode_bundle};

// === TCP stream ===

pub struct TcpBundleStream {
    stream: TcpStream,
    peer: PeerAddr,
}

#[async_trait]
impl BundleStream for TcpBundleStream {
    async fn send(&mut self, bundle: &Bundle) -> Result<()> {
        let data = encode_bundle(bundle)?;
        let len = data.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&data).await?;
        debug!("Sent bundle {} to {}", bundle.id(), self.peer);
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Bundle>> {
        let mut len_buf = [0u8; 4];
        match self.stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        // 50 MB hard cap per bundle to prevent memory exhaustion
        const MAX_BUNDLE_BYTES: usize = 50 * 1024 * 1024;
        if len > MAX_BUNDLE_BYTES {
            anyhow::bail!("Bundle too large: {} bytes from {}", len, self.peer);
        }

        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf).await?;
        let bundle = decode_bundle(&buf)?;
        debug!("Received bundle {} from {}", bundle.id(), self.peer);
        Ok(Some(bundle))
    }

    fn peer_addr(&self) -> &PeerAddr {
        &self.peer
    }
}

// === TCP listener ===

pub struct TcpBundleListener {
    listener: TcpListener,
}

#[async_trait]
impl BundleListener for TcpBundleListener {
    async fn accept(&mut self) -> Result<Box<dyn BundleStream>> {
        let (stream, addr) = self.listener.accept().await?;
        info!("Accepted peer connection from {}", addr);
        Ok(Box::new(TcpBundleStream {
            peer: PeerAddr(addr.to_string()),
            stream,
        }))
    }
}

// === TCP convergence layer ===

pub struct TcpTransport {
    pub bind_addr: String,
}

#[async_trait]
impl ConvergenceLayer for TcpTransport {
    fn name(&self) -> &str {
        "tcp"
    }

    async fn connect(&self, peer: &PeerAddr) -> Result<Box<dyn BundleStream>> {
        let stream = TcpStream::connect(&peer.0)
            .await
            .with_context(|| format!("TCP connect failed: {}", peer))?;
        info!("Connected to peer {}", peer);
        Ok(Box::new(TcpBundleStream {
            peer: peer.clone(),
            stream,
        }))
    }

    async fn listen(&self) -> Result<Box<dyn BundleListener>> {
        let listener = TcpListener::bind(&self.bind_addr)
            .await
            .with_context(|| format!("TCP bind failed: {}", self.bind_addr))?;
        info!("TCP transport listening on {}", self.bind_addr);
        Ok(Box::new(TcpBundleListener { listener }))
    }
}
