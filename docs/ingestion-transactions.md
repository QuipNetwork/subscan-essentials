# Ingestion transaction and publication contract

Block, extrinsic, event, log, account mapping, `block_publications`, and `plugin_projections` rows
commit in one SQL transaction. Begin, write, and commit errors propagate to the
caller. No ingestion counters, progress markers, or plugin work are
published before a successful commit. Partition creation remains preparatory
schema work; it is not a block commit or an ingestion progress marker.

`block_publications` retains the number of newly inserted extrinsics and signed
extrinsics per block. Conflict-skipped rows do not increment either count.
PostgreSQL uses explicit `RETURNING *` with addressable batches to avoid the
pinned Gorm fork's overcounting of partially duplicate default-column returns.
The supplied signed-batch count is no longer trusted.

After commit, a Redis WATCH/MULTI/EXEC transaction applies the unpublished count
delta, records the applied per-block counts, and advances both progress markers
monotonically. Arithmetic uses checked int64 values. A Redis error is returned
as a publication failure of an already committed block. Retrying `FillBlockData`
for that stored finalized block republishes from SQL without RPC or reinsertion.
An ambiguous SQL commit is recovered the same way if the block was committed.
An EXEC reply lost after execution is safe to retry. Progress markers retain
their existing highest-observed-block meaning, not a contiguous-range guarantee.

Plugin work is durable and at least once. Enabled subscriptions are captured in
`plugin_projections` in the source block transaction, with stable IDs for each
(plugin, kind, block/event/extrinsic identity). After commit, pending jobs are
reserved for 30 seconds and sent through the existing worker queue. `queued_at`
records submission only. It is never proof of completed processing.

Worker startup and a five-second sweep revisit due pending rows (up to 256 per
pass), including work whose enqueue failed or whose worker crashed. Concurrent
duplicate deliveries are allowed; plugin effects must be idempotent.
`completed_at` is written conditionally only after the plugin returns success.
A delayed failed duplicate cannot overwrite a completed acknowledgement. No SQL
transaction/connection is held across plugin execution and its external RPC.
Errors retain pending work and an error description. A panic/process crash or
failed acknowledgement leaves the row pending; plugin side effects may
already exist and must tolerate replay. Missing/disabled plugins stay pending.
Core counter/progress completion is independent of this processing completion.

Balance transfers are keyed by event ID and accounts by address. Their inserts
and SQL totals share a transaction under a singleton count-row lock. Initial
counts are seeded from existing SQL rows once. Redis receives absolute totals
while that lock is held, so delayed publishers and lost replies cannot inflate
or overwrite newer counts. Retrying an existing transfer still retries counter
publication and both account refreshes. Refresh errors leave the projection
pending. Periodic metadata refresh reconciles these SQL totals with row counts.
System dispatch-error persistence also propagates failures instead of falsely
acknowledging them. Existing legacy queue deliveries remain supported; new
blocks use `plugin-projection` jobs. EVM processing remains outside this recovery
acceptance (the target deployment disables it).

There is no exactly-once claim. Completed SQL receipts are retained, and pending
work is replayed after failures. Old blocks without projection rows are not
silently backfilled; explicit reingestion can seed missing work. Roll out the
new tables and worker code together before ingesting new blocks. Account data
availability/unknown semantics and historical metadata selection are separate
follow-up gates; account refresh failure currently keeps that projection pending.

Redis counters and the `NETWORK_NODE:block_publications` hash form one cache
state and must be retained/restored together. Do not delete the deduplication
hash alone or independently overwrite counters while ingesting. SQL receipts
are retained without expiry. A full cache rebuild must reconcile counters,
receipt acknowledgements, and progress together; no production repair or cache
reset is performed by this change. Existing pre-migration blocks without a SQL
receipt recover progress with zero new increments. The new tables must be
migrated before running the importer (normal automatic migration includes it).

Run the focused real PostgreSQL 17 / Redis 7 regression suite:

```sh
bash scripts/test-ingestion-transactions.sh -v
```

It creates disposable containers on random loopback ports, uses a fresh SQL
schema and Redis namespace, and removes the containers on exit. Tests cover
statement rollback, a deferred constraint failure at COMMIT, signed and unsigned
duplicate batches, Redis validation failure, a lost EXEC reply, concurrent
publication retries, monotonic progress, multi-batch partial duplicates, int64
precision and overflow. Projection tests additionally exercise the real Redis
queue and balance HTTP handler, failed enqueue, lost delivery before processing,
SQL transfer insert followed by counter failure, a lost counter reply, simulated
worker crash after effects but before acknowledgement, SQL acknowledgement
failure, concurrent duplicate workers, and convergence to one transfer/two
accounts with exact counters. The
account RPC in this test is an explicitly synthetic local responder, not archived
chain state. The existing web3 package initializer can still probe its configured
EVM endpoint before tests run; these tests do not establish EVM-disabled startup.
The suite avoids the legacy DAO package test initializer that
truncates configured tables. These are focused native storage tests; the actual
linux/amd64 image and controlled chain-fixture DB/API acceptance are separate.

## Execution runtime at upgrade boundaries

`FillBlockData` reads the body and event storage at the block hash, but selects
the execution runtime with `state_getRuntimeVersion(parentHash)`. Metadata cache
misses use that same parent hash. `ChainBlock.SpecVersion` records this execution
version, including for later plugin event/error interpretation. The runtime
installed in the block's post-state is registered separately at the block height;
it does not select this block's decoder. Runtime records refer to observed state
heights (the parent height for execution metadata), not inferred activation
heights across missing history. Existing metadata caches remain keyed by spec
version within the configured chain.

Genesis uses its own hash, never its zero parent. An empty genesis body with
absent event storage is treated as an empty event vector. Missing parent hashes,
mismatched block heights, unavailable/malformed runtime responses, and invalid
metadata fail before core block writes. There is no latest/nearby-runtime fallback.
Runtime metadata registration is preparatory state outside the core block
transaction; it may remain after a later block decode or transaction failure.

The local synthetic WebSocket tests exercise `FillBlockData` with different
parent/post call indexes and argument types, ordinary blocks, genesis, failed
runtime/metadata reads, and direct `CreateChainBlock` metadata cache misses.
The opt-in archive test uses the byte-exact shared fixture from validator commit
`52a9070944d4cd2319a5cddcd2da1d8eb0a63abc`, verifies its manifest and input hashes,
and checks that block 330109's eight extrinsics and 36 events decode under runtime
117 while post-state runtime 118 is registered separately. Run it with the
reviewed decoder forks selected by a temporary module file. The test responder
serves the captured V16 bytes through the loader's metadata request; it is a
selection/decoding regression, not evidence of metadata-version negotiation by
the deployed image.

```sh
test_mod_dir=$(mktemp -d)
cp go.mod go.sum "$test_mod_dir/"
go mod edit -modfile="$test_mod_dir/go.mod" \
  -replace="github.com/itering/scale.go=$(cd ../scale.go && pwd)" \
  -replace="github.com/itering/substrate-api-rpc=$(cd ../substrate-api-rpc && pwd)"
ETH_RPC=http://127.0.0.1:1 \
QUIP_CONFORMANCE_FIXTURES="$(cd ../scale.go/testdata/quip-metadata-conformance && pwd)" \
go test -mod=mod -modfile="$test_mod_dir/go.mod" ./internal/service \
  -run TestArchivedUpgradeExecutionRuntime -count=1
rm -r "$test_mod_dir"
```

`ETH_RPC` directs the existing import-time EVM probe to a closed loopback port;
it does not establish EVM-disabled application startup. Fee/session/account/author
availability semantics and the full-image DB/API replay remain separate gates.
