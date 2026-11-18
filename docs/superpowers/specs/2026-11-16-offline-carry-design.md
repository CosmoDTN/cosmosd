# Offline carry: design

_Status: draft for review, 2026-11-16_

## Intent

The owner wants to watch movies on a device that has no internet, and to get those movies onto that device without internet being involved at the moment of transfer. The movie moves from device to device over Bluetooth, USB or a local network with no uplink. It is the same idea as passing a drive from hand to hand, but automatic.

The owner's homelab is the first copy. It has internet, downloads content, and holds the library. Every transfer after that can happen with no internet anywhere nearby.

The owner wants this to be a real step forward for delay-tolerant networking (DTN), not a re-skin of a download button. That means it has to be honest about what DTN can and cannot do, and it has to interoperate with standard Bundle Protocol software so the claim can be checked by others.

## What DTN cannot do (read this first)

- **It cannot send new content to a device that has no link at all.** On a plane with no connection, nothing new arrives. Content must already be on the plane, on some device in reach.
- **The first copy must come from the internet somewhere.** The homelab is that place. After that, copies spread with no internet.
- **Bluetooth is slow.** Expect roughly 0.1 to 1 Mbit/s in practice. A film takes minutes to hours to move. This is for on-demand video, not live.
- **Live streaming needs a real link.** The existing in-flight design (`2026-10-30-inflight-live-design.md`) covers that case when the plane provides a link. This spec does not replace it.

These limits are part of the product's honest description. The docs and demo must not imply otherwise.

## The picture

```
 [Homelab]                  [Carrier: laptop]            [Viewer: phone]
 has internet once          no internet needed           no internet needed
 downloads + signs   --->   subscribes, stores,   --->   subscribes, stores,
 the library                serves HLS to VLC            plays locally
        (any link or a USB drive)     (Bluetooth, USB, local Wi-Fi)
```

Three roles, one daemon:

1. **Publisher.** Holds the library, signs content, usually the homelab. Needs internet only to fetch new content.
2. **Carrier.** Any node that holds bundles and hands them on. A laptop, a Raspberry Pi, another phone.
3. **Viewer.** A node that wants specific content, stores it, and plays it. Plays through any HLS player such as VLC.

One device can hold several roles.

## Goals

- **G1. Standard bundles.** The wire format becomes RFC 9171 (BPv7) CBOR. Standard BPv7 software can decode and carry our bundles. Streaming metadata rides in extension blocks, not in a private envelope.
- **G2. Subscriptions.** A node tells a peer which content it wants. The peer sends only that. A phone never fills up with the whole library. A storage cap is enforced, with a clear rule for what is dropped.
- **G3. Resumable transfer.** A link that drops mid-bundle resumes where it stopped. Every bundle is hash-checked before it is stored. A bundle is not discarded by the sender until the receiver confirms it is stored.
- **G4. Trusted content.** The publisher signs a manifest for each piece of content. A viewer accepts content only if the manifest is signed by a key it has paired with. Forged or tampered content is rejected even when it arrives through strangers.
- **G5. Honest time.** Expiry and age use BPv7's bundle age mechanism, so a phone with a wrong or drifting clock does not drop or keep bundles by mistake.
- **G6. Offline proof.** A repeatable demo shows a movie reaching and playing on a device with the internet verifiably off, through a carrier that also has no internet.
- **G7. Bluetooth transport.** A convergence layer carries bundles over Bluetooth Low Energy so a phone in airplane mode can take part.
- **G8. Phone as a real node.** A phone stores subscribed content and can hand it to another device. This is what makes the carry real on a phone.

## Non-goals

- Live streaming with no link. It is impossible (see above).
- Full BPSec (RFC 9172) compliance in the first release. We sign manifests with our own scheme. BPSec integrity blocks can come later.
- LoRa, satellite messengers and ham radio as video transports. They are text and request channels at best.
- Multiple bitrates, transcoding on carriers, DRM.
- Custody transfer as defined by older drafts. We use a simpler confirm-before-delete rule between two neighbours (G3).
- Naming this "BPv8", or claiming it as a standard. It is a profile.

## Anti-goals: fails even if it technically works when

- A viewer needs internet at the moment it receives content.
- The demo only works because some device quietly had a connection.
- A viewer plays content from an unsigned or unknown publisher.
- Our bundles can't be read by any other BPv7 implementation.
- A phone fills its storage with content it never asked for.
- The docs or UI present pre-loading as live streaming, or advertise it as a way around anyone's terms or content rights.

## Constraints and facts

- **Codebase.** About 3,400 lines of Rust. Branch `feat/inflight-live` has about 30 commits not on GitHub (CBOR sync v2, live ingest, live playlist, offline phone page, per-direction tokens, link simulator, demo). This spec builds on that branch. Whether and when to push it is the owner's call.
- **Bundle today.** A private struct serialised with CBOR (ciborium). Fields include `id`, `sequence`, `priority`, `expires_at`, `payload_hash` (BLAKE3), `duration_ms`, `hop_count`. It is not RFC 9171.
- **Transport today.** TCP only. A `ConvergenceLayer` trait exists, but the router does not use it yet.
- **Playback.** `/play/{content_id}` serves HLS. VLC plays it. A phone page with embedded hls.js exists. This already works and is not part of the work here.
- **Phones.** The owner has one Mac and one phone for tests, and no second Mac. The phone is an Android (confirmed 2026-11-17). The exact model and Android version are not yet known.
- **Termux limit.** Termux on Android can run the daemon and use IP links (USB tether, local hotspot). It has no Bluetooth API for a command-line program. Bluetooth on Android therefore needs a native app. The same is true on iPhone.
- **Mac as a Bluetooth peripheral.** Rust BLE crates mostly act as the central side. Acting as a peripheral on macOS needs CoreBluetooth. A small Swift helper talking to the daemon over a local socket is a likely answer.
- **Private-use block types.** RFC 9171 reserves block type codes 192 to 255 for private or experimental use. Our extension blocks use that range.
- **Ed25519 is not in the BPSec default contexts.** RFC 9173 defaults are HMAC-based. This is why signing is done on a manifest by our own scheme for now.
- **No IPs, MACs, domains or secrets in the repo.** Keys and tokens live outside the repository.

## Approaches considered

1. **A profile on BPv7 with extension blocks (chosen).**
   Standard bundles, our metadata in private-use blocks, our own layers for sync, resume and trust. Interoperable and honest. Costs real work to move to RFC 9171 encoding first.
2. **A new protocol.** Clean for streaming. Loses all interoperability and every existing tool. Rejected.
3. **Stay BPv7-inspired.** Fastest path to a demo. A dead end for credibility. Rejected.

On transports, we also weighed one Rust core with thin native shells per platform against full rewrites per platform. We pick one Rust core. Shells only handle what Rust can't (Bluetooth on phones).

## Design

### Bundle format (G1, G5)

Each bundle is an RFC 9171 bundle: a primary block, extension blocks, and a payload block holding one HLS segment (or a manifest or playlist).

- **Primary block.** Destination is a content endpoint (a `dtn:` URI naming the content), not a single node. Lifetime is set from priority as today.
- **Creation time.** DTN time zero is allowed. Nodes without a trusted clock set the creation timestamp to zero and rely on the age block.
- **Bundle age block (type 7).** Carries elapsed time. Each node adds the time it held the bundle. Expiry is judged by age, not by comparing wall clocks.
- **Hop count block (type 10).** Replaces the private `hop_count`.
- **Previous node block (type 6).** Set at each hop.
- **Content block (private-use type, working code 192).** CBOR map with: content ID, title, sequence number, segment duration in milliseconds, MIME type, and priority. Replaces the private fields.
- **Payload block.** The segment bytes. The BLAKE3 hash is not stored in the bundle. It is listed in the signed manifest (G4).
- **Block processing flags.** The content block is marked "discard bundle if block can't be processed" off, so other BPv7 software carries the bundle even if it does not understand our block. This is the property the interop test checks.

Open detail for the builder: exact canonical CBOR form, CRC choice per block, and how legacy stores are refused (the store already refuses pre-protocol-2 data, so a protocol 3 bump follows the same pattern).

### Content manifests and trust (G4)

For each piece of content, the publisher creates a manifest bundle.

- Contents: content ID, title, total segment count, per-segment BLAKE3 hash and duration, creation time, and publisher key ID.
- It is signed with the publisher's Ed25519 key.
- A viewer stores a segment only if it is listed in a signed manifest from a paired publisher. Segments that arrive before their manifest are held briefly, then dropped if no valid manifest arrives (the hold length is a builder choice).
- **Pairing.** The publisher's public key is shown as a QR code or short string. The viewer adds it once. Pairing is a local, offline step. There is no key server.
- A manifest has a version. A newer signed version replaces an older one for the same content ID.

### Subscriptions (G2)

Subscriptions live in the session handshake, not in bundle blocks. This is a change from an earlier sketch. Reason: the interest is about a conversation between two neighbours and changes often, so it does not belong inside immutable bundles.

- At the start of a session each side sends its manifest of held bundles and its **interest list**: content IDs, or a publisher key plus a channel tag, it wants.
- A side sends only bundles that match the other side's interest list. Epidemic flooding stays available as an explicit mode for small networks, off by default.
- **Storage cap.** Each node has a byte cap. When full, it drops by this order: expired, then content with no interest and not subscribed, then the oldest unwatched VOD. Content the user pinned is never dropped. The exact policy is written in the spec for the cap, and a builder may refine it, but the order of what is protected is fixed here.
- **Carriers with no subscription.** A carrier can be configured to hold content for others. It reports what it holds and sends on request. This is how a laptop carries a movie for a phone.

### Resumable, confirmed transfer (G3)

The session protocol moves bundles in pieces.

- A bundle is sent in pieces of a fixed size (default 64 KiB, tunable per transport, smaller for Bluetooth).
- The receiver tracks how many bytes of each bundle it has. After a reconnect it tells the sender the offset. The sender continues from there.
- When a bundle is complete, the receiver checks it against the signed manifest hash, stores it, then sends a confirmation.
- The sender keeps the bundle until it has the confirmation. It may then drop it only if its own policy allows. A publisher normally keeps everything.
- Partial data is kept across reconnects for a bounded time, then freed.

This does not use BPv7 fragmentation. Fragmentation splits a bundle into new bundles. We keep one bundle per segment and resume inside it. That is simpler and fits HLS.

### Transports (G7)

The router moves onto the `ConvergenceLayer` trait so every transport uses the same session protocol.

- **TCP.** Works over any IP link: local Wi-Fi, USB tethering, a hotspot with no uplink.
- **Bluetooth Low Energy.** L2CAP connection-oriented channels where the platform offers them, and GATT as a fallback if needed. Piece size and pacing are tuned for it. Discovery uses a service UUID advertisement.
- Discovery on IP links stays mDNS. A node may also be given an address by hand.

### Phone node (G8)

A phone must store and serve bundles, and must show a player. The platform decides the shape:

The owner's phone is an Android, so Android is the target.

- **M3 on Android.** A Termux build of the daemon runs on the phone over a USB tether or a local hotspot with no uplink. This gives an early check that the phone can hold and serve bundles over IP before any Bluetooth work.
- **M4 and M5 on Android.** A native app embeds the Rust core as a library and handles Bluetooth, because Termux has no Bluetooth access. Android's Bluetooth permissions and background limits are part of the design and the builder documents them.
- The phone plays through VLC for Android, the system player, or the embedded page.
- **iPhone** is out of scope for now. It would need a native app and has stricter background Bluetooth limits.

## Milestones and acceptance

Each milestone has a check that can run without a flight.

- **M1. Standard bundles and signed manifests.** (G1, G4, G5)
  - Our bundles round-trip through the encoder and decoder.
  - An independent BPv7 implementation (µD3TN is the first candidate) accepts and stores a bundle we produce, ignoring our content block.
  - A segment whose hash is not in a signed manifest is rejected. A manifest signed by an unpaired key is rejected.
  - A bundle with a wrong wall clock but a correct age expires on age.
- **M2. Subscription sync with resume.** (G2, G3)
  - Two nodes over TCP: the viewer receives only subscribed content.
  - Cut the link mid-bundle, restore it, and the transfer resumes. The final hash matches.
  - A node at its cap drops in the specified order and never drops pinned content.
- **M3. The offline demo.** (G6)
  - A Mac (carrier, holding a movie) and a phone, joined by a USB tether or a local hotspot with no uplink.
  - The check is automated where possible. It confirms no outbound DNS or internet traffic happens on the Mac during the run. The owner turns Wi-Fi uplink off by hand for the real run.
  - The movie plays on the phone in VLC or the page, from the Mac.
  - This proves offline playback from a node. It does not yet prove phone storage.
- **M4. Bluetooth.** (G7)
  - Two devices with no IP link exchange bundles over Bluetooth, resuming after a drop.
  - Measured throughput is recorded in the docs, not assumed.
- **M5. Phone stores and relays.** (G8)
  - The phone subscribes, receives a movie over Bluetooth or the IP link, and plays it with the Mac gone.
  - With a second phone or device: the content passes from the first phone to the second with the Mac absent. This is the store-carry-forward claim.

The order is fixed because M1 changes the wire format and everything after depends on it. M3 can be done with Termux on Android before M4 exists if the owner's phone is Android.

## What the owner decides

- The name of the profile. "CosmoStream" is a working name only.
- Whether to push `feat/inflight-live` to GitHub, and when.
- The exact Android model and version, before M4, because Bluetooth behavior varies by device.
- Anything about content rights. The owner chooses what content to carry. The docs must not suggest evading anyone's terms or rights.

## Calls made on the owner's behalf (open to change)

- The profile approach over a new protocol.
- Subscriptions in the session handshake, not in a bundle block.
- Signed manifests with Ed25519 instead of BPSec integrity blocks in the first release.
- One bundle per segment with resume inside it, not BPv7 fragmentation.
- Private-use block type 192 for the content block.
- One Rust core with native shells for phones.
- Milestone order M1 to M5, with the wire change first.

## Calls settled after the builder check (open to change)

- **Interop scope.** Our sessions are ours only. Interop means a one-way handoff: a stock BPv7 node can decode, store and forward a bundle we produce. The handoff uses whichever standard path the chosen interop node supports, with TCPCLv4 preferred and a bundle file import as the fallback. A stock node does not take part in subscriptions or resume. The block flags are set so unknown blocks are carried and no status reports are requested.
- **Content IDs.** A content ID is chosen by the publisher and is unique only within that publisher. The global key is the pair of publisher key ID and content ID. The destination URI is `dtn://<publisher-key-id>/<content-id>`. A second publisher can never overwrite another publisher's content, because the key ID is part of the identity.
- **Cap policy states.** "Watched" and "pinned" exist only on viewers. A carrier has no player, so it drops by this order: expired, then no interest, then oldest. The cap counts everything stored: segments, manifests, partial transfers and held-but-unmanifested segments.
- **Carrier trust.** A carrier checks signatures for publishers it is paired with. It also holds a small quota of content from unknown publishers, marked unverified, so it can carry for others without being flooded. The quota is a fixed share of the cap and the builder picks the number.
- **Revocation.** Version one has manual revocation only: remove the publisher key on a device. There is no key rotation yet. A compromised key means re-pairing.
- **Age and sessions.** Age counts time held, including time spent holding a partial transfer. Devices use a monotonic clock and persist a hold timestamp. Time lost across a reboot is accepted and written down as a known limit.
- **No-internet check.** The automated check runs locally, not in CI. It watches every non-local network interface on the Mac during the run and fails if any outbound traffic leaves. Local mDNS on the link is expected and allowed.
- **Phone in M3.** The phone is a thin client of the Mac's HLS. A Termux-built daemon on the phone is an optional extra check in M3. The phone becomes a real node with its own store and Bluetooth in M5.

## Left to the builder

- Canonical CBOR details, CRC choices, and the protocol version bump.
- The hold time for segments that arrive before their manifest.
- Piece size per transport and the pacing on Bluetooth.
- The exact storage cap policy inside the fixed protection order.
- How to build the Bluetooth peripheral side on macOS (a Swift helper is one option).
- Which interop target to use first if µD3TN is hard to build.
- The shape of the offline demo harness and the no-internet check.
- Store performance work the above runs into.
