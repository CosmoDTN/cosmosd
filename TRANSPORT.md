# Writing a CosmosD Transport Plugin

A transport plugin (convergence layer) teaches cosmosd how to carry bundles
over a new physical medium — LoRa, Reticulum, Bluetooth, ham radio, USB, laser.

## The interface

Implement two traits in Rust:

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

Bundles are serialized as **length-prefixed JSON** over your stream:

```
[ 4 bytes: payload length (big-endian u32) ][ N bytes: serde_json bundle ]
```

See `src/transport/tcp.rs` for the exact framing. Reuse `send_frame` / `recv_frame`
if your medium is stream-oriented. For datagram media (LoRa, UDP), adapt as needed.

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
