#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
fixture_dir="$PWD/testdata/quip-babe"
validator_dir=$(cd "${1:?usage: test-quip-babe-fixtures.sh /path/to/quip-validator}" && pwd)
expected_runtime=52a9070944d4cd2319a5cddcd2da1d8eb0a63abc
if [[ $(git -C "$validator_dir" rev-parse HEAD) != "$expected_runtime" ]]; then
  echo "Requires reviewed runtime $expected_runtime" >&2
  exit 1
fi
git -C "$validator_dir" diff --quiet HEAD -- runtime Cargo.toml Cargo.lock crates pallets
scratch=$(mktemp -d "${TMPDIR:-/tmp}/quip-babe-check.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
mkdir -p "$scratch/vrf/src" "$scratch/transition/src" "$scratch/output"
cp "$fixture_dir/generator/Cargo.toml" "$scratch/vrf/Cargo.toml"
cp "$fixture_dir/generator/src/main.rs" "$scratch/vrf/src/main.rs"
cp "$validator_dir/Cargo.lock" "$scratch/vrf/Cargo.lock"
cargo run --offline --manifest-path "$scratch/vrf/Cargo.toml" -- "$scratch/output"
cmp "$fixture_dir/primary.scale" "$scratch/output/primary.scale"
cmp "$fixture_dir/secondary-vrf.scale" "$scratch/output/secondary-vrf.scale"
cp "$fixture_dir/session-transition.rs" "$scratch/transition/src/main.rs"
cp "$validator_dir/Cargo.lock" "$scratch/transition/Cargo.lock"
python3 - "$scratch/transition/Cargo.toml" "$validator_dir/runtime" <<'PY'
import json,sys
from pathlib import Path
manifest='''[package]
name="quip-session-transition-check"
version="0.1.0"
edition="2021"
[dependencies]
codec={package="parity-scale-codec",version="=3.7.5"}
serde_json="1"
'''
manifest+='quip-protocol-runtime={path='+json.dumps(sys.argv[2])+'}\n'
for name in ['sp-runtime','sp-io','sp-consensus-babe','frame-support','frame-system','pallet-session','pallet-babe']:
    manifest+=name+'={git="https://github.com/QuipNetwork/polkadot-sdk.git",rev="845eb9986c7131056b077d14ad7249f9d65ca111"}\n'
Path(sys.argv[1]).write_text(manifest)
PY
SKIP_WASM_BUILD=1 SKIP_PALLET_REVIVE_FIXTURES=1 cargo run --offline \
  --manifest-path "$scratch/transition/Cargo.toml" -- "$scratch/output/session-transition.json"
cmp "$fixture_dir/session-transition.json" "$scratch/output/session-transition.json"
echo "Pinned SDK hybrid fixtures and native runtime transition reproduced byte-for-byte"
