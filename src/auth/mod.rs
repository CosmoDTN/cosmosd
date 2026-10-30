//! Relay authentication — optional pre-shared tokens, one per direction.
//!
//! `--relay-token` guards the relay port: every incoming connection must send
//! a matching token as the first frame before the manifest exchange. The token
//! we present when dialing a peer is that peer's own, given as
//! `--peer host:port@token`, so a gateway can hold the ground node's token
//! without demanding it from the cabin peers that dial it.
//!
//! This is intentionally simple — a pre-shared token, not a PKI system.
//! Proper peer authentication (e.g. node keypairs) is a future milestone.

use std::net::{IpAddr, Ipv4Addr};

use anyhow::{bail, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;
use tracing::warn;

use crate::router::FRAME_TIMEOUT;

// === Incoming ===

pub struct RelayAuth {
    token: Option<String>,
}

impl RelayAuth {
    /// `listen_token` guards incoming connections only.
    pub fn new(listen_token: Option<String>) -> Self {
        if listen_token.is_none() {
            warn!(
                "Relay port is open — no --relay-token set. \
                 Any device on the network can sync bundles."
            );
        }
        RelayAuth { token: listen_token }
    }

    /// Called by the relay listener on each incoming connection.
    /// Reads the auth frame and validates the token.
    pub async fn verify_incoming(&self, stream: &mut (impl AsyncRead + AsyncWrite + Unpin)) -> Result<()> {
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
}

// === Outgoing ===

/// Called before sending our manifest to a peer: presents that peer's token
/// (if it has one) and waits for acknowledgement.
pub async fn authenticate_outgoing<S>(stream: &mut S, token: Option<&str>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(token) = token else {
        return Ok(()); // peer is open
    };

    send_auth_frame(stream, token).await?;
    let response = recv_auth_frame(stream).await?;
    if response != "ok" {
        bail!("Relay auth rejected by peer: {}", response);
    }
    Ok(())
}

// === Peer arguments ===

/// Split a `--peer` argument `host:port@token` at the first `@`. Addresses
/// never contain `@`; tokens may. A blank token (`host@`) is an error.
pub fn parse_peer(arg: &str) -> Result<(String, Option<String>)> {
    match arg.split_once('@') {
        Some((addr, token)) => {
            if token.trim().is_empty() {
                bail!("--peer {arg}: empty token after '@' (drop the '@' for a peer without one)");
            }
            Ok((addr.to_string(), Some(token.to_string())))
        }
        None => Ok((arg.to_string(), None)),
    }
}

/// clap value parser for `--relay-token`: rejects empty or whitespace-only
/// tokens, which would guard the relay with a secret anyone can guess.
pub fn non_blank_token(s: &str) -> std::result::Result<String, String> {
    if s.trim().is_empty() {
        return Err("the token must not be empty or whitespace".into());
    }
    Ok(s.to_string())
}

// === Open-relay guard ===

/// Whether `ip` is a publicly routable address: not private, loopback,
/// link-local, unspecified, or in the carrier-grade NAT range 100.64.0.0/10.
pub fn is_public_v4(ip: Ipv4Addr) -> bool {
    let cgnat = ip.octets()[0] == 100 && (ip.octets()[1] & 0xC0) == 64;
    !(ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified() || cgnat)
}

/// Whether any of `ips` is a public IPv4 address.
pub fn any_public_v4(ips: &[IpAddr]) -> bool {
    ips.iter().any(|ip| matches!(ip, IpAddr::V4(v4) if is_public_v4(*v4)))
}

/// Why `serve` must refuse to start, if it must. A token or `--open-relay`
/// always passes. Otherwise a `--no-discovery` node (the ground role, which
/// strangers dial) is refused outright, since NAT and containers can hide
/// that it is exposed; any other node is refused only on a public address.
pub fn open_relay_refusal(has_token: bool, open_relay: bool, no_discovery: bool, public: bool) -> Option<&'static str> {
    if has_token || open_relay {
        None
    } else if no_discovery {
        Some("refusing an open relay with --no-discovery (ground role): set --relay-token or pass --open-relay")
    } else if public {
        Some("refusing an open relay on a public address: set --relay-token or pass --open-relay")
    } else {
        None
    }
}

/// Apply `open_relay_refusal` to this machine's interfaces. If they can't be
/// listed, the public-address check only warns.
pub fn check_open_relay(has_token: bool, open_relay: bool, no_discovery: bool) -> Result<()> {
    if has_token || open_relay {
        return Ok(());
    }
    let public = match if_addrs::get_if_addrs() {
        Ok(ifaces) => any_public_v4(&ifaces.iter().map(|i| i.ip()).collect::<Vec<_>>()),
        Err(e) => {
            warn!("could not list network interfaces, skipping the public-address check: {}", e);
            false
        }
    };
    if let Some(msg) = open_relay_refusal(has_token, open_relay, no_discovery, public) {
        bail!(msg);
    }
    Ok(())
}

async fn send_auth_frame<S: AsyncWrite + Unpin>(stream: &mut S, data: &str) -> Result<()> {
    let bytes = data.as_bytes();
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    Ok(())
}

async fn recv_auth_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Result<String> {
    let mut len_buf = [0u8; 4];
    timeout(FRAME_TIMEOUT, stream.read_exact(&mut len_buf)).await??;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 512 {
        bail!("Auth frame too large");
    }
    let mut buf = vec![0u8; len];
    timeout(FRAME_TIMEOUT, stream.read_exact(&mut buf)).await??;
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

    #[test]
    fn parse_peer_splits_token() {
        assert_eq!(parse_peer("1.2.3.4:443@s3cr@t").unwrap(), ("1.2.3.4:443".into(), Some("s3cr@t".into())));
        assert_eq!(parse_peer("10.0.0.2:7778").unwrap(), ("10.0.0.2:7778".into(), None));
    }

    #[test]
    fn parse_peer_rejects_empty_token() {
        for arg in ["1.2.3.4:443@", "1.2.3.4:443@   "] {
            let err = parse_peer(arg).expect_err(arg);
            assert!(err.to_string().contains("empty token"), "{arg}: {err}");
        }
    }

    #[test]
    fn non_blank_token_rejects_empty_and_whitespace() {
        assert_eq!(non_blank_token("s3cret").unwrap(), "s3cret");
        assert!(non_blank_token("").is_err());
        assert!(non_blank_token(" \t ").is_err());
    }

    #[test]
    fn public_v4_detection() {
        assert!(is_public_v4("8.8.8.8".parse().unwrap()));
        for ip in ["10.0.0.1", "192.168.1.5", "172.16.0.1", "100.70.0.1", "127.0.0.1", "169.254.1.1", "0.0.0.0"] {
            assert!(!is_public_v4(ip.parse().unwrap()), "{}", ip);
        }
        assert!(is_public_v4("100.128.0.1".parse().unwrap()));
    }

    #[test]
    fn open_relay_guard_passes_with_token_or_flag() {
        assert!(check_open_relay(true, false, false).is_ok());
        assert!(check_open_relay(false, true, false).is_ok());
        assert!(check_open_relay(true, false, true).is_ok());
        assert!(check_open_relay(false, true, true).is_ok());
    }

    #[test]
    fn open_relay_refused_for_ground_role() {
        // --no-discovery without a token is refused even with no public IP:
        // NAT and containers hide public exposure.
        let msg = open_relay_refusal(false, false, true, false).expect("refused");
        assert!(msg.contains("--no-discovery") && msg.contains("--relay-token") && msg.contains("--open-relay"), "{msg}");
        assert!(check_open_relay(false, false, true).is_err());
    }

    #[test]
    fn open_relay_refused_on_public_address() {
        let msg = open_relay_refusal(false, false, false, true).expect("refused");
        assert!(msg.contains("public address"), "{msg}");
        assert!(open_relay_refusal(false, false, false, false).is_none());
        for (token, open) in [(true, false), (false, true)] {
            assert!(open_relay_refusal(token, open, true, true).is_none());
        }
    }

    #[test]
    fn any_public_v4_ignores_private_and_v6() {
        let ips = |list: &[&str]| list.iter().map(|s| s.parse().unwrap()).collect::<Vec<std::net::IpAddr>>();
        assert!(!any_public_v4(&ips(&["127.0.0.1", "192.168.1.5", "10.0.0.1", "2001:4860:4860::8888"])));
        assert!(any_public_v4(&ips(&["127.0.0.1", "8.8.8.8"])));
        assert!(!any_public_v4(&[]));
    }

    #[tokio::test]
    async fn listener_token_does_not_apply_outgoing() {
        let auth = RelayAuth::new(Some("ground-secret".into()));
        let (mut a, mut b) = tokio::io::duplex(1024);
        let (inc, out) = tokio::join!(auth.verify_incoming(&mut a), authenticate_outgoing(&mut b, Some("ground-secret")));
        assert!(inc.is_ok() && out.is_ok());
    }

    #[tokio::test]
    async fn outgoing_without_token_sends_nothing() {
        let (mut a, _b) = tokio::io::duplex(1024);
        assert!(authenticate_outgoing(&mut a, None).await.is_ok());
    }
}
