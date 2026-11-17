# Interop with a stock BPv7 node

Status: passing against µD3TN v0.15.2.

This page records the M1 interop check. A BPv7 node that we did not write takes a bundle made by cosmosd. It decodes the bundle, stores it, and forwards it with block 192 intact. The check is `scripts/interop-ud3tn.sh`.

## Stock node

- Node: µD3TN, the BPv7 implementation by D3TN.
- Version: release v0.15.2, commit `6b18b267ce79ac0c8d8f7dbce5389f2acef0ca40`.
- Source: `https://gitlab.com/d3tn/ud3tn.git`, cloned at the tag. The script checks the commit.
- Build: native, with µD3TN's documented `make posix` target. `DISABLE_JSON=1` drops the optional JSON config support, so the Jansson library is not needed. The SQLite headers are needed. macOS ships them.
- Platform of the recorded run: macOS on Apple silicon.

The plan said to run µD3TN in Docker. No Docker daemon runs on the development machine, so µD3TN is built from source instead. The native build took about a minute and worked first time. No fallback to dtn7-rs was needed.

## Convergence layer

µD3TN v0.15.2 has no TCPCLv4. It offers MTCP, SMTCP, TCPCLv3, SPP over TCP and BIBE. The check uses MTCP. MTCP sends each bundle as one CBOR byte string over TCP. It needs no session handshake, so the sender and receiver in the script stay small.

## How the check runs

1. Build cosmosd. Clone and build µD3TN in a fresh temp dir. Set `UD3TN_BIN` to skip this and use a prebuilt `ud3tn`.
2. Write a sample bundle with `cosmosd dev-bundle --out in.bundle`. It is a segment bundle under a throwaway publisher key. The content ID is `interop-sample`. The payload is 31 bytes. The whole bundle is 291 bytes.
3. Start µD3TN as `dtn://interop-node/` with one MTCP listener on a free loopback port.
4. Start a one-shot MTCP receiver on another free loopback port. It stands in for the next node.
5. Send µD3TN a contact for the bundle's destination node, `dtn://<publisher-key-id>/`, through the receiver's port. The contact opens 3 seconds later. The config goes in as a bundle to `dtn://interop-node/config` over AAP v1, µD3TN's local application socket.
6. Send the sample bundle to µD3TN over MTCP.
7. Pass only if all of these hold:
   - µD3TN logs that it received a bundle for `dtn://<publisher-key-id>/interop-sample`.
   - µD3TN logs a successful forward to the receiver.
   - The receiver gets the bundle and `cosmosd dev-bundle --check` decodes it. That decoder refuses a bundle without a valid block 192.
   - Publisher, content ID, kind, sequence and payload hash are unchanged.
   - The age block grew by at least 1000 ms. This shows µD3TN held the bundle until the contact opened.
8. Kill µD3TN and the receiver. Remove every temp dir.

The script prints `INTEROP PASS` and exits 0 only when every check holds. Otherwise it prints `INTEROP FAIL:` with a reason and exits 1.

Run it from the repo root.

```sh
scripts/interop-ud3tn.sh
```

## What was observed

The run on 2026-11-17 printed this.

```
interop: sample bundle: 291 bytes to dtn://8ac027f3a4daf768/interop-sample
interop: µD3TN v0.15.2 (commit 6b18b267ce79ac0c8d8f7dbce5389f2acef0ca40) is up as dtn://interop-node/, MTCP in on 127.0.0.1:64741
interop: contact to dtn://8ac027f3a4daf768/ opens in 3 s
interop: sent the sample bundle to µD3TN over MTCP
interop: µD3TN log: Received new bundle 0x1052ecb10 from "dtn://cosmosd-dev/" to "dtn://8ac027f3a4daf768/interop-sample" via CLA mtcp
interop: µD3TN log: TX of bundle 0x1052ecb10 to "dtn://8ac027f3a4daf768/" via "mtcp:127.0.0.1:64742" OK
interop: receiver got 293 bytes back
interop: decoded by cosmosd, before -> after:
  publisher=8ac027f3a4daf768 -> 8ac027f3a4daf768
  content_id=interop-sample -> interop-sample
  kind=segment -> segment
  sequence=0 -> 0
  payload_blake3=626e19b68ffd60a9f1d06e2277f8fb78dc0c5830ac41789bb15cf55032eb98b1 -> 626e19b68ffd60a9f1d06e2277f8fb78dc0c5830ac41789bb15cf55032eb98b1
  payload_len=31 -> 31
  source_node=cosmosd-dev -> cosmosd-dev
  age_ms=0 -> 2816
  hop_count=0 -> 1
  previous_node=none -> none
INTEROP PASS: µD3TN v0.15.2 (commit 6b18b267ce79ac0c8d8f7dbce5389f2acef0ca40) received, stored and forwarded a cosmosd bundle with block 192 intact
```

µD3TN changed what RFC 9171 lets a forwarding node change.

- It added the 2816 ms it held the bundle to the age block. Creation time is zero, so the age block is the only clock. µD3TN used it.
- It raised the hop count from 0 to 1.
- It did not add a previous node block.
- It kept block 192 and the payload byte for byte. The bundle grew by 2 bytes because the age now needs a longer CBOR integer.

## What this proves

- A stock BPv7 node decodes a cosmosd bundle and accepts it as valid RFC 9171.
- It stores the bundle while no contact is open, and forwards it when one opens.
- It carries block 192, which it does not know, without changing it. Block control flags of 0 work as intended.
- It routes on our `dtn://<publisher-key-id>/<content-id>` destination like any other `dtn` EID.
- It keeps age-based time for a bundle with creation time zero.
- What it forwards still decodes with cosmosd's own decoder.

## What this does not prove

- It does not show a stock node taking part in cosmosd sync. Sync, manifests and admission are cosmosd's own protocol. A stock node only stores and forwards.
- It does not show interop over TCPCLv4. µD3TN has none. MTCP was the path.
- It does not cover a stock node handing a bundle back to a running cosmosd daemon. cosmosd has no MTCP or TCPCL listener yet. The check decodes the forwarded bytes with cosmosd's decoder instead.
- It covers one segment bundle. Manifest bundles use the same layout and code path but were not sent.
- It covers one other implementation, µD3TN. dtn7-rs and others are not tested here.
