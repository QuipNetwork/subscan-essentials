# Quip BABE author fixtures

SDK: `845eb9986c7131056b077d14ad7249f9d65ca111`.
Native runtime: `52a9070944d4cd2319a5cddcd2da1d8eb0a63abc`.
SHA-256 digests are in `manifest.json` and checked by the Go tests.
The hybrid signing and native-transition fixtures below are controlled generated
fixtures. `aglais-591853.json` is a captured network block, described separately
below.

`generator/src/main.rs` uses the pinned SDK's real H4 pair, a public test seed,
BABE signing data, and `VrfSecret::vrf_sign`. It verifies the hybrid proof,
encodes Primary and SecondaryVRF through the SDK's SCALE `PreDigest`, then decodes
and verifies them again. Both files are 840 bytes:

| Field | Bytes |
| --- | ---: |
| variant | 1 |
| authority index | 4 |
| slot | 8 |
| sr25519 VRF pre-output and proof | 96 |
| PQ signature field (fixed SCALE array) | 731 |

The 731-byte field is not a SCALE vector or the variable-length library proof.
Its first byte is a length delta, followed by `411 + delta` signature bytes and
zero padding, as checked by the pinned SDK's `library_proof` function and
`pqhybridsign = 0.0.0-rc9`. The Go parser checks this envelope/padding and the author
index, but does not verify proofs, slot eligibility, or seals: it consumes a
finalized node header. It accepts the pinned hybrid layout and 13-byte
SecondaryPlain. Classical 109-byte VRF digests and arbitrary extensions/truncations
are unsupported, producing unknown author data. Aura's existing 8-byte layout
remains supported separately.

`session-transition.rs` executes the actual native Quip runtime hooks. It builds
the local genesis, initializes BABE/session at slot 1, and prepares a controlled
consistent parent state with reversed queued validator keys and reversed
BABE next-authority state. At the next epoch's first slot it invokes BABE and Session
`on_initialize`; it does **not** call `rotate_session` itself. Assertions require
session 0 -> 1, BABE's current authority order to equal the prepared
next order, and runtime `FindAccountFromAuthorIndex` to return the post-state
validator (different from the parent-order lookup). The Go test then checks the
same mapping against the exported JSON. This proves the normal initialization
transition for this runtime, not historic rotation at the five archived blocks
(which all have secondary-plain digests) or an end-to-end node-produced transition.
Arbitrary root storage/code changes are outside this controlled transition.

Source trace in the pinned SDK:

- `substrate/primitives/consensus/babe/src/digests.rs`: variant tags and fields.
- `quip/primitives/crypto/src/substrate/sr25519_fndsa512.rs`: `VrfSignature`,
  `library_proof`; crypto-core `suite/fndsa512.rs`: `HYBRID_SIG_LEN = 731`.
- `substrate/frame/session/src/lib.rs`: `on_initialize`, `rotate_session` installs
  queued validators then calls session handlers with the same ordered keys.
- `substrate/frame/babe/src/lib.rs`: `on_new_session` maps those keys to authority
  order and calls `enact_epoch_change`; `FindAuthor` reads the header index.
- `substrate/client/consensus/babe/src/lib.rs`: verification selects the viable
  epoch for the child's slot; `verification.rs` looks up that epoch's authorities
  by the header index (rather than blindly using the parent's current epoch).

Reproduce with the pinned runtime checkout and cached Rust dependencies:

```sh
CARGO_TARGET_DIR=/path/to/reusable/cargo-target \
  scripts/test-quip-babe-fixtures.sh /path/to/quip-validator
```

The script uses the runtime's committed Cargo.lock, creates temporary harnesses,
and compares all outputs byte-for-byte. It modifies no SDK/runtime source. Only
the native harness skips Wasm/Revive fixture builds; this is not application-image
or deployed-node acceptance evidence. The generator's transient Cargo.lock and
target directory are ignored; the checked runtime lock is the resolution source.

## Captured finalized PrimaryVRF block

`aglais-591853.json` is byte-for-byte RR's `q901/live/block_591853.json`, captured
2026-10-13, SHA-256 `029c7ba97024c438a6ecfe9031e94e667b87ea7a5b57456796a50dacab036ab6`.
Its block hash is
`0xe5e55f20230e3e8f730ef47e54cd265e1aa9b1271cd506131231108d5695caa9`.
`aglais-591853-provenance.json` records the source manifest/index hashes, captured
finalized head, and import verification. All 96 source responses from 591853
through 591948 were checked against RR's manifest; each independently encoded
SCALE header hash matched the captured index, and the parent links terminate at
captured finalized hash
`0x1f4f03e6d63a2e11e70bb1b4b7beb5b419e830842a3ef8553ce3af2cd69773fd`.
This trusts the captured node finality response; it is not an independent GRANDPA
proof verification.

The Go regression checks file integrity, independently computes the block hash,
decodes the actual PreRuntime log, verifies Primary/840 bytes/index 2, and calls
the production author parser. That test's three validator labels are synthetic;
the separate enrichment regression below supplies captured accounts. The captured sample contains 27 PrimaryVRF and
73 SecondaryPlain blocks across 591849–591948; this is not a network-wide rate.
The original five conformance fixture blocks are all SecondaryPlain and do not
exercise session rotation. This additional header supplements the same
session/author review gate. The Rust reproduction script verifies the three
generated outputs; the captured header is verified by the Go test and its
provenance, not regenerated by signing.

### Exact-hash auxiliary capture

`primary-aux/` records six read-only JSON-RPC requests and verbatim responses
from the same archive endpoint on 2026-10-15: genesis identity, canonical block
hash at 591853, runtime, metadata, Session.CurrentIndex and Session.Validators.
Every historical request uses the header's exact hash. Metadata is V16; its
response is stored as gzip (SHA in the manifest covers the decompressed bytes).
The manifest and all raw request/response hashes are checked by
`TestCapturedPrimaryBabeEnrichment`. It uses the real metadata and the actual
840-byte PrimaryVRF log through the production enrichment path, with no RPC
method aliases, and observes session 0 and author
`d88854de054c7534450cddace65332a98d12ba06ff3adb39f9ad40f129b37aa7` (index 2).
JSON serialization retains available session zero and author status.

This is node-reported historical state, not a cryptographic storage proof. It
does not demonstrate session rotation, SQL persistence, or application-image
acceptance. The test uses only committed files and a local recorder; it requires
the reviewed decoder forks and `QUIP_PRIMARY_AUXILIARY_REPLAY=1` while the module
dependency pins are still pending.

To make a new capture without overwriting the reviewed evidence:

```sh
python3 -I scripts/capture-primary-aux.py /path/to/new-empty-directory
```

The script's output directory must not already exist. Compare the new captures
before changing any pinned hashes; request IDs and raw responses are retained.
