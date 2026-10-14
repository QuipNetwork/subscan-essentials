# Quip BABE author fixtures

SDK: `845eb9986c7131056b077d14ad7249f9d65ca111`.
Native runtime: `52a9070944d4cd2319a5cddcd2da1d8eb0a63abc`.
SHA-256 digests are in `manifest.json` and checked by the Go tests.
These are controlled generated fixtures, not captured network blocks.

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
