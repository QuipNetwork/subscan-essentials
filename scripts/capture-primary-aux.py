#!/usr/bin/env python3
"""Read-only capture of auxiliary state for the archived PrimaryVRF header.

Usage: python3 -I scripts/capture-primary-aux.py OUTPUT_DIRECTORY [RPC_URL]
Responses are preserved verbatim (metadata is gzip-compressed, mtime=0).
This records node-reported historical state, not a storage/finality proof.
"""
import datetime
import gzip
import hashlib
import json
import pathlib
import sys
import urllib.request

out = pathlib.Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=False)
url = sys.argv[2] if len(sys.argv) > 2 else "https://bootnode-2.aglais.quip.network:20049/rpc"
block_hash = "0xe5e55f20230e3e8f730ef47e54cd265e1aa9b1271cd506131231108d5695caa9"
# Twox128("Session") ++ Twox128(item); replay tests independently derive keys
# from the captured metadata through the production storage reader.
index_key = "0xcec5070d609dd3497f72bde07fc96ba072763800a36a99fdfc7c10f6415f6ee6"
validators_key = "0xcec5070d609dd3497f72bde07fc96ba088dcde934c658227ee1dfafcd6e16903"
queries = [
    ("genesis", "chain_getBlockHash", [0]),
    ("hash", "chain_getBlockHash", [591853]),
    ("runtime", "state_getRuntimeVersion", [block_hash]),
    ("metadata", "state_getMetadata", [block_hash]),
    ("session", "state_getStorageAt", [index_key, block_hash]),
    ("validators", "state_getStorageAt", [validators_key, block_hash]),
]
calls = []
for number, (label, method, params) in enumerate(queries, 1):
    name = f"{number:03d}-{label}"
    request = json.dumps(dict(jsonrpc="2.0", id=number, method=method, params=params), separators=(",", ":")).encode()
    with urllib.request.urlopen(urllib.request.Request(url, request, {"Content-Type": "application/json"}), timeout=30) as response:
        raw = response.read()
    result = json.loads(raw)
    assert result.get("id") == number and "result" in result and "error" not in result, result
    if label == "genesis":
        assert result["result"] == "0x59e064bddd49a920d1392693c728c3bf9867f2cf3e0f8fea8c2498b389b0c286"
    if label == "hash":
        assert result["result"] == block_hash
    if label == "validators":
        assert isinstance(result["result"], str) and len(result["result"]) == 196
    (out / (name + ".request.json")).write_bytes(request)
    suffix = ".response.json.gz" if label == "metadata" else ".response.json"
    (out / (name + suffix)).write_bytes(gzip.compress(raw, mtime=0) if label == "metadata" else raw)
    calls.append(dict(file=name, method=method, params=params,
                      requestSha256=hashlib.sha256(request).hexdigest(),
                      responseSha256=hashlib.sha256(raw).hexdigest()))
manifest = dict(capturedAt=datetime.datetime.now(datetime.timezone.utc).isoformat(), rpc=url, blockHash=block_hash, calls=calls)
(out / "capture-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"Captured {len(calls)} requests at {block_hash}")
