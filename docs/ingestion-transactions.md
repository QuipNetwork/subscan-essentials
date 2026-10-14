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

## Fee availability

New extrinsics persist separate availability and state-hash fields for estimated
and observed fees. Both extrinsic list and detail APIs expose:

| Field | Meaning |
| --- | --- |
| `fee` / `fee_status` | Inclusion-fee estimate excluding tip, with status `estimated`, `unknown`, or `not_applicable`. |
| `used_fee` / `used_fee_status` | Full `TransactionPayment.TransactionFeePaid.actual_fee`, including tip, with status `available`, `unknown`, or `not_applicable`. |
| `fee_state_hash` | Explicit execution parent hash used for the fee estimate. |
| `used_fee_state_hash` | Block hash whose event supplied the observed fee. |
| `fee_error` | Optional auxiliary RPC/validation failure description. |

Unknown and not-applicable amounts are JSON `null`; observed/estimated zero is
the decimal string `"0"`. API consumers must accept nullable fee fields and use
the statuses. Existing numeric SQL columns are retained; rows without availability
evidence default to `unknown`, so their old zero/nonzero values are not silently
declared verified. This change does not backfill existing rows or add automatic
fee refresh to completed blocks.

Estimates use `payment_queryFeeDetails(rawExtrinsic, parentHash)`, falling back
to `payment_queryInfo` at the same hash. A valid null `inclusionFee` means a zero
estimate; a missing/malformed response means unknown. The broken direct
`TransactionPaymentApi_query_info` JSON-RPC method path is removed. Nodes without
the payment RPC methods yield unknown estimates. JSON amounts and WeightV2
components are decoded without float64 conversion, preserving uint64 values
including proof sizes above the signed-int64 range.

An estimate failure cannot erase an observed fee, including an observed zero.
Missing, malformed, or duplicate fee-paid events leave the observed amount
unknown; it is never reconstructed by scaling one weight component. Bare V4/V5
extrinsics need no estimate query; General V5 extrinsics may pay fees even though
they are not signed. Auxiliary fee failure does not fail core ingestion.

Focused tests cover RPC failures, exact large numbers, genuine zeros, tip
preservation, malformed observations, General/bare behavior, and PostgreSQL
persistence plus the DAO presenters used by both APIs. `TestArchivedFeeEvents`
uses the same fixture environment as the upgrade test and checks all 107 events,
including 20 exact fee-paid amounts. Session, author, and account availability
remain separate follow-up changes; full-image HTTP acceptance is still pending.

### Session and author observations

Block list/detail responses add `session_index` (nullable unsigned integer),
`session_status`, `session_state_hash`, `session_error`, `author_status`,
`author_state_hash`, and `author_error`. Statuses are `available`, `absent`, or
`unknown`. A verified session zero is numeric `0`; unavailable or absent session
indices are `null`. Legacy rows with no status remain unverified (`unknown`);
existing validator strings are retained for compatibility. New unavailable authors
have an empty `validator` and an explicit status/error. Consumers must inspect
status, rather than treating an empty string or null as a real value. No automatic
historical backfill is performed.

Session index and validator order are observations of the block's **post-state**,
using metadata selected by `state_getRuntimeVersion(blockHash)`. The storage prefix,
SCALE type, modifier and default come from that metadata, never global latest
metadata. Block body/event decoding still uses parent execution metadata. At a
session boundary the author index maps through the block's new validator order;
the old session-number-only SQL cache is no longer used. This follows Quip's
BABE/session mapping (`pallet_session::FindAccountFromAuthorIndex`); BABE's session
hook installs the new epoch authorities during initialization. Only the pinned Quip
hybrid BABE (840-byte primary/secondary-VRF and
13-byte secondary-plain) and Aura pre-runtime digests are supported; classical
109-byte BABE VRF payloads are not accepted as Quip proofs. See
[testdata/quip-babe](../testdata/quip-babe/README.md) for SDK-generated proof
fixtures, parser boundaries, and the native runtime session-transition check.
Missing/ambiguous/unsupported digests and invalid
author indices produce unknown, never an inferred account or an ingestion panic.

RPC null for a metadata `Default` entry decodes its fallback (including legitimate
session zero). RPC null for `Optional` is absent. RPC errors, missing metadata,
malformed storage and missing results are unknown. Session/author queries run before
the core SQL transaction; failure does not prevent valid core rows from committing.
The loader releases its pooled RPC connection first, so enrichment cannot deadlock
by borrowing another slot while all workers hold loader connections.
Strict lengths guard the legacy SCALE decoder's zero padding of truncated integers
and AccountIds. Validator vectors are specifically checked as Quip AccountId32
vectors. This does not generalize the SDK's storage decoder or change its API.

Account state has its own contract below. UI compatibility and actual-image
acceptance remain separate gates; session observations do not establish them.

### Account observations

Balance account list/detail now expose nullable `nonce`, `balance`, `locked`,
`reserved`, plus `state_status` (`available`, `absent`, `unknown`), `state_hash`,
`state_block_num`, `state_spec_version`, and `state_error`. Available zero remains
numeric nonce `0` and amount string `"0"`; unavailable amounts and nonce are JSON
`null`. Legacy rows without provenance normalize to unknown at serialization;
there is no automatic claim that their old values describe historical state.

Balance-event projections read System.Account at their finalized block hash.
They resolve its **post-state** runtime separately from the block's execution
spec, and use that metadata's storage prefix, map hasher, value type and fallback.
A missing Default value decodes the metadata fallback; Optional null is absent.
RPC errors, missing results/metadata, unsupported schema and malformed bytes are
unknown. SCALE values must consume and re-encode the entire response exactly to
reject the legacy decoder's integer padding. Nonce is checked as u32 and each
balance as u128; both modern `frozen` and legacy `miscFrozen`/`feeFrozen` fields
are supported. No latest-state fallback or silent JSON decoding is used.

Every address/hash observation is retained in `balance_account_observations`.
Its SQL write and the current account view update are atomic with account counts.
The current view advances by block number: an older retry cannot roll it back,
a newer unknown clears stale values, and a same-hash unknown cannot downgrade an
already available/absent observation. An explicit same-hash replay can recover
unknown state later. Conflicting finalized hashes at the same height fail rather
than silently replacing a view. This is the latest *observed* account state, not
an assertion that every account has been refreshed at chain head.

RPC happens before the account transaction. Once an unknown observation is
persisted, the projection can complete; unavailable historical state does not
cause endless automatic retries. SQL/Redis failures still leave projections
pending. `InitAccount` pins the finalized height reported by the storage DAO,
requires that block to be indexed, and enumerates/reads at its hash, and `InitTransfer` passes the original finalized blocks
when replaying events. Neither command reads unpinned latest account values.
Real archived auxiliary replay, UI rendering and actual-image acceptance remain
integration work; synthetic account values in unit tests are not historical
account evidence.
