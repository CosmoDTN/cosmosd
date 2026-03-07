//! Relay authentication — optional shared token for the relay port.
//!
//! If `--relay-token` is set, every incoming relay connection must send
//! a matching token as the first frame before the manifest exchange.
//! If no token is configured, the relay is open with a logged warning.
//!
//! This is intentionally simple — a pre-shared token, not a PKI system.
//! Proper peer authentication (e.g. node keypairs) is a future milestone.

use anyhow::{bail, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::warn;

pub struct RelayAuth {
    token: Option<String>,
}

impl RelayAuth {
    pub fn new(token: Option<String>) -> Self {
        if token.is_none() {
            warn!(
                "Relay port is open — no --relay-token set. \
                 Any device on the network can sync bundles."
            );
        }
        RelayAuth { token }
    }

    /// Called by the relay listener on each incoming connection.
    /// Reads the auth frame and validates the token.
    pub async fn verify_incoming(&self, stream: &mut TcpStream) -> Result<()> {
        let Some(expected) = &self.token else {
            return Ok(()); // open relay
        };

        let token = recv_auth_frame(stream).await?;
        if token != *expected {
            send_auth_frame(stream, "denied").await?;
            bail!("Relay auth failed: wrong token");
        }
        send_auth_frame(stream, "ok").await?;
        Ok(())
    }

    /// Called by the peer manager before sending our manifest.
    /// Sends our token and waits for acknowledgement.
    pub async fn authenticate_outgoing(&self, stream: &mut TcpStream) -> Result<()> {
        let Some(token) = &self.token else {
            return Ok(()); // open relay
        };

        send_auth_frame(stream, token).await?;
        let response = recv_auth_frame(stream).await?;
        if response != "ok" {
            bail!("Relay auth rejected by peer: {}", response);
        }
        Ok(())
    }
}

async fn send_auth_frame(stream: &mut TcpStream, data: &str) -> Result<()> {
    let bytes = data.as_bytes();
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    Ok(())
}

async fn recv_auth_frame(stream: &mut TcpStream) -> Result<String> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 512 {
        bail!("Auth frame too large");
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(String::from_utf8(buf)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_relay_has_no_token() {
        let auth = RelayAuth::new(None);
        assert!(auth.token.is_none());
    }

    #[test]
    fn token_relay_stores_token() {
        let auth = RelayAuth::new(Some("secret123".to_string()));
        assert_eq!(auth.token.as_deref(), Some("secret123"));
    }
}
