# Ingestion transaction and publication contract

Block, extrinsic, event, log, account mapping, and `block_publications` rows
commit in one SQL transaction. Begin, write, and commit errors propagate to the
caller. No ingestion counters, progress markers, or plugin notifications are
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

Plugin notifications remain best effort. Their errors are logged after commit,
do not roll back core ingestion, and do not suppress counter/progress publication.
There is no durable notification outbox or exactly-once plugin delivery claim.
Publication retries of existing blocks do not replay plugin notifications.

Redis counters and the `NETWORK_NODE:block_publications` hash form one cache
state and must be retained/restored together. Do not delete the deduplication
hash alone or independently overwrite counters while ingesting. SQL receipts
are retained without expiry. A full cache rebuild must reconcile counters,
receipt acknowledgements, and progress together; no production repair or cache
reset is performed by this change. Existing pre-migration blocks without a SQL
receipt recover progress with zero new increments. The new table must be
migrated before running the importer (normal automatic migration includes it).

Run the focused real PostgreSQL 16 / Redis 7 regression suite:

```sh
bash scripts/test-ingestion-transactions.sh -v
```

It creates disposable containers on random loopback ports, uses a fresh SQL
schema and Redis namespace, and removes the containers on exit. Tests cover
statement rollback, a deferred constraint failure at COMMIT, signed and unsigned
duplicate batches, Redis validation failure, a lost EXEC reply, concurrent
publication retries, monotonic progress, multi-batch partial duplicates, int64
precision and overflow. It avoids the legacy DAO package test initializer that
truncates configured tables. These are focused native storage tests; the actual
linux/amd64 image and controlled chain-fixture DB/API acceptance are separate.
