# Writing a CosmosD Transport Plugin

A transport plugin (convergence layer) teaches cosmosd how to carry bundles
over a new physical medium — LoRa, Reticulum, Bluetooth, ham radio, USB, laser.

## The interface

Implement three traits in Rust:

```rust
use async_trait::async_trait;
use anyhow::Result;
use cosmosd::transport::{BundleStream, BundleListener, ConvergenceLayer, PeerAddr};
use cosmosd::bundle::Bundle;

// 1. A bidirectional bundle stream with one peer
pub struct MyStream { /* your connection state */ }

#[async_trait]
impl BundleStream for MyStream {
    async fn send(&mut self, bundle: &Bundle) -> Result<()> {
        // serialize and send bundle over your medium
    }

    async fn recv(&mut self) -> Result<Option<Bundle>> {
        // receive and deserialize one bundle
        // return Ok(None) when connection closes
    }

    fn peer_addr(&self) -> &PeerAddr {
        &self.peer
    }
}

// 2. A listener that accepts incoming connections
pub struct MyListener { /* your server socket */ }

#[async_trait]
impl BundleListener for MyListener {
    async fn accept(&mut self) -> Result<Box<dyn BundleStream>> {
        // wait for incoming peer, return a stream
    }
}

// 3. The convergence layer — entry point cosmosd calls
pub struct MyTransport { /* config: frequency, baud rate, device path, etc. */ }

#[async_trait]
impl ConvergenceLayer for MyTransport {
    fn name(&self) -> &str {
        "my-transport"  // short unique name: "lora", "reticulum", "ble"
    }

    async fn connect(&self, peer: &PeerAddr) -> Result<Box<dyn BundleStream>> {
        // open a connection to a remote peer
    }

    async fn listen(&self) -> Result<Box<dyn BundleListener>> {
        // start listening for incoming peers
    }
}
```

## Bundle serialization

Bundles travel as RFC 9171 (BPv7) bytes, with a length prefix over your stream:

```
[ 4 bytes: bundle length (big-endian u32) ][ N bytes: RFC 9171 bundle ]
```

`cosmosd::wire::encode_bundle` and `decode_bundle` turn a `Bundle` into those
bytes and back. See `src/transport/tcp.rs` for the exact framing. Reuse it if
your medium is stream-oriented. For datagram media (LoRa, UDP), adapt as needed.

## What a transport can rely on

A transport only moves bytes. It does not need to check them. The receiving
node does that before it stores anything.

- **Signed content.** Every content item has a publisher with an Ed25519 key.
  The publisher signs a manifest that lists the BLAKE3 hash of each segment.
  A node stores a manifest only when its signature checks out, and a segment
  only when its hash matches the manifest. A corrupted or forged bundle is
  dropped on arrival, so a lossy medium can never plant bad content.
- **Pairing.** A node accepts content only from its own key and the keys it
  has paired with `cosmosd keys pair`. Pairing is done by people, out of band.
  It is never negotiated over a transport. `serve` reads the keys at startup,
  so a new pairing or a revocation (`keys unpair`) takes effect after a
  restart of a running node.
- **Age-based expiry.** A bundle carries its age in the RFC 9171 age block. A
  node adds the time it held the bundle before sending it on. The bundle
  expires when its age reaches its lifetime. No wall clock is involved, so a
  transport needs no clock sync. A slow medium only adds age. Keep the age
  block when you re-frame a bundle, and do not reset it.
- **Order.** Bundles may arrive in any order. A segment that arrives before
  its manifest waits up to 10 minutes for it.

## Naming convention

Publish your transport as a separate crate and repo:

```
Crate:  cosmosd-transport-lora
Repo:   github.com/cosmoscast/transport-lora
Binary: --transport lora (future CLI flag)
```

## Bundle size considerations

| Medium | Max practical bundle size | Notes |
|--------|--------------------------|-------|
| TCP / WiFi | 50 MB | No real limit |
| Bluetooth 5 | ~2 MB / min | Fine for low-quality segments |
| LoRa (SF7) | ~5 KB / min | Audio only, or metadata/manifests |
| Ham radio (AX.25) | ~1.2 KB / packet | Manifests only, not segments |
| USB sneakernet | Unlimited | Batch transfer, not streaming |

For low-bandwidth media, consider transmitting only **manifests** (what bundles exist)
and letting users fetch full segments when they reach a higher-bandwidth node.

## Submitting

1. Build your transport crate
2. Test it with two cosmosd instances
3. Open an issue in `cosmoscast/cosmosd` linking your repo
4. We'll add it to the README under community transports
