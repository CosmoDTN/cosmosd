# Spec 001: Bundle format

Status: in use from sync protocol 3.

Every cosmosd bundle is an RFC 9171 (BPv7) bundle. It holds one HLS segment or one signed manifest. Any BPv7 node can decode it and carry it. Only cosmosd reads the content block.

The code lives in `src/wire/mod.rs`. It uses the `bp7` crate.

## Encoding

A bundle is a CBOR indefinite-length array. It holds the primary block, then the canonical blocks, then a break byte. This is the standard RFC 9171 layout.

## Block layout

| Block | Type | Block number | Contents |
| --- | --- | --- | --- |
| Primary | none | 0 | Destination, source, creation timestamp, lifetime |
| Content | 192 | 2 | CBOR map of content info |
| Bundle age | 7 | 3 | Age in milliseconds |
| Hop count | 10 | 4 | Hop limit and hop count |
| Previous node | 6 | 5 | Node that last forwarded the bundle |
| Payload | 1 | 1 | Segment bytes or signed manifest bytes |

The payload block is always last, as RFC 9171 requires. The other blocks come in descending block number.

The previous node block is present only when a node has sent the bundle to a peer. A new bundle has no previous node block.

## Primary block

- Version is 7.
- Destination is `dtn://<publisher-key-id>/<content-id>`. See the next section.
- Source is the node ID of the node that created the bundle, `dtn://<node-name>/`.
- Report-to is `dtn:none`. No status reports are requested.
- Creation timestamp time is DTN time zero. Nodes do not need a trusted clock.
- Creation timestamp sequence number is the first 8 bytes of BLAKE3 over the cosmosd bundle ID, a zero byte and the payload. With time zero, this keeps the RFC 9171 bundle ID unique. The same bundle always gets the same number.
- Lifetime is set from the priority. See "Lifetime and age".
- The primary block has a CRC-32C.

## Destination URI

The destination is `dtn://<publisher-key-id>/<content-id>`.

- The publisher key ID is 16 lowercase hex characters. It is the first 8 bytes of BLAKE3 of the publisher's Ed25519 public key.
- The content ID is chosen by the publisher. It is unique only within that publisher.
- A content ID is 1 to 64 characters long. It uses only ASCII letters, digits, `.`, `_` and `-`. So it is always one URI segment and never needs escaping.
- Identity is the pair of publisher key ID and content ID.

A decoder takes the publisher from the node part of the destination. It takes the content ID from the service part. The content ID in block 192 must match the one in the destination. If it does not, the bundle is refused.

## Block 192: content info

Block 192 is in the private-use range. Its data is a CBOR map with these keys.

| Key | CBOR type | Meaning |
| --- | --- | --- |
| `kind` | text | `segment` or `manifest` |
| `content_id` | text | Content ID, same as in the destination |
| `title` | text | Human-readable title |
| `sequence` | unsigned | Segment index. 0 for a manifest |
| `duration_ms` | unsigned | Segment playback length in milliseconds. 0 for a manifest |
| `mime_type` | text | `video/mp2t` for segments. `application/vnd.cosmos.manifest+cbor` for manifests |
| `priority` | text | `live`, `near_live` or `vod` |
| `created_at_ms` | signed | Publisher wall-clock time in Unix milliseconds |

`created_at_ms` sets the live playlist edge. Expiry never uses it.

A bundle whose block 192 is missing or does not decode as this map is refused.

## Payload

The payload block holds the segment bytes, or the CBOR bytes of a `SignedManifest`. The payload hash is not in the bundle. The signed manifest lists the BLAKE3 hash of each segment.

## Lifetime and age

The lifetime comes from the priority.

| Priority | Lifetime |
| --- | --- |
| `live` | 30 minutes |
| `near_live` | 6 hours |
| `vod` | 30 days |

A manifest for live content gets the live lifetime. Other manifests get the VOD lifetime.

The bundle age block holds the bundle's age in milliseconds. A new bundle has age 0. A bundle is valid while its age is below its lifetime. Wall clocks are never compared.

Each node that holds a bundle adds the time it held it to the age. The store keeps this count, so a bundle leaves a node with its age brought up to date.

## Hop count

The hop count block holds a hop limit of 255 and the current count. A new bundle has count 0. Each cosmosd node raises the count by one when it sends the bundle to a peer, and puts its own node ID in the previous node block. cosmosd does not enforce the limit yet.

## Control flags

- Bundle processing control flags: only "bundle must not be fragmented" is set. cosmosd keeps one bundle per segment and never fragments.
- Block processing control flags: 0 on every block. A node that does not know block 192 keeps it and forwards the bundle. No block asks for a status report or for the bundle to be deleted.

## CRC

The primary block has a CRC-32C. Canonical blocks have no CRC. The age, hop count and previous node blocks change at each hop. The payload is checked against the signed manifest hash.

## Decoding

A decoder refuses a bundle, with an error and never a panic, when:

- the bytes are not a valid RFC 9171 bundle,
- the bundle fails RFC 9171 validation,
- the destination is not `dtn://<publisher>/<content-id>`,
- the content ID breaks the rule in "Destination URI",
- block 192 is missing or does not decode,
- the content IDs in block 192 and the destination differ,
- the bundle age block is missing,
- the source is `dtn:none`.

A missing hop count block reads as count 0.

## Interop

A stock BPv7 node, µD3TN, stores and forwards these bundles with block 192 intact. See `interop.md`.
