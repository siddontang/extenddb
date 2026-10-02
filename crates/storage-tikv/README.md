# TiKV backend

This crate implements ExtendDB's storage contracts over transactional TiKV.
Development is split into dependency-closed modules: each module documents its
invariants, exposes an injectable boundary, and tests failures as well as normal
operation. It does not depend on PostgreSQL or an embedded SQL catalog.

## Module boundaries

| Module | Responsibility | Test boundary |
|---|---|---|
| `codec` | Stable tuple framing, exact numeric ordering and prefix bounds | Pure property tests, binary/decimal edge cases |
| `kv` | Transaction lifecycle and bounded conflict replay | Injected `Store` and `Transaction` |
| `kv::memory` | Deterministic MVCC reference and commit fault injection | Same contract as the real transport; test-only feature |
| `kv::client` | Client-rust connection, read protection, scans and commit classification | Real-cluster contract suite and structural error tests |
| `config` / `engine` | Validated deployment namespace, dependencies and injectable clock | Roundtrip/invalid configuration and independent namespaces |
| `table` | Account-scoped names, immutable generations, schema fences | Duplicate creation, stale generation and account isolation |
| `index` | Sparse entries, projections and full-key tie breakers | Pure projection tests and index pagination contract |
| `data` | Shared conditional mutation/transaction pipeline | Concurrency, atomic cancellation and token replay |
| `query` | Byte-range planning, ordered cursor pagination and scan segmentation | Numeric ordering, reordered composite sort-key conditions, forward/reverse pages and segment union |
| `stream` | Atomic per-shard change log and retained generations | Atomic record counts, monotonic sequences and account isolation |

The reference store deliberately does not invent predicate locks: code relying
on the absence of a range must maintain a shared lifecycle guard. A successful
point read, including a missing key, participates in commit conflict detection.

## Transaction rules

Only a *confirmed conflict* can replay a transaction closure. An unknown commit
result is returned to the caller; replaying it can duplicate a successful write.
Every retry starts a new snapshot and recomputes conditions. A transaction body
must never send messages, alter local state, or otherwise perform external side
effects. Rollback runs on errors; the real adapter also schedules cleanup when
a request is cancelled. Process-crash cleanup still depends on TiKV lock expiry.

The first transport uses optimistic two-phase commit, with read keys explicitly
protected. Its abstraction follows the `TikvTransactionSource` / driver split in
the user's TiDB Rust reference at commit
`6a5b492097d5be084a0b1106da2c7f106c518498`, particularly
`rust/crates/tidb-txnkv/src/driver/tikv_opener.rs` and `retry.rs`.
The reference's vendored client requires Rust 1.93 and adds TiDB-specific APIs.
We pin the portable published client `0.4.0` behind a feature rather than adding
an absolute dependency on that checkout or copying its entire fork. The adapter
is the only place a future client replacement needs to change.

## Tests

```sh
cargo test -p extenddb-storage-tikv
cargo test -p extenddb-storage-tikv --features client,test-support
TIKV_PD_ENDPOINTS=127.0.0.1:2379 cargo test -p extenddb-storage-tikv \
  --features client,test-support --test data_contract -- --ignored
```

The production client is opt-in (`client`); the in-memory reference is only
available to unit tests or via `test-support`. No production config selects it.
Client-rust remains experimental upstream; passing local tests does not establish
production readiness under machine failure, network partitions, or scale.

## Data layout and initial performance choices

All keys start with the escaped tuple `(extenddb, namespace, v1)`; metadata,
items, indexes, stream generations and records have disjoint subspaces. Table
ids are UUIDs, so deletion/recreation cannot alias old data. Item values retain
the DynamoDB attribute envelope, including decimal strings and binary data.

The initial implementation updates secondary indexes synchronously. This meets
DynamoDB's eventual-read allowance and keeps base/index changes atomic, but
does not emulate PostgreSQL's configurable delayed GSI propagation. Online
build metadata carries a resumable cursor; an index remains unavailable until
the build finishes. Every write rechecks table metadata, so schema changes
fence requests holding cached key information.

The key order favors contiguous partition queries. A single hot item or stream
shard still contends, and this backend has no claim of improved throughput until
workload benchmarks establish it. Large administrative scans currently collect
bounded network pages into memory; they are not an unlimited-size backup API.

## Complete backend modules

| Module | Contract and validation |
|---|---|
| `catalog` | Atomic per-account IAM aggregate, global credential locators and 4 MiB limit; IAM/namespace contracts |
| `catalog::management` | Users, groups, roles, inline policies, boundaries, keys, sessions; ownership, dependency and rollback assertions |
| `catalog::crypto` | AES-256-GCM with access-key ID as authenticated context; tampering, wrong ID/key and truncation tests |
| `catalog::credentials` | Snapshot credential resolution and expiry; deletion/revocation and temporary credential contracts |
| `catalog::authorization` | User/group/role policies, boundaries, tags and live session data; cross-account and expired-session tests |
| `catalog::operational` | Settings, bcrypt administrators, aggregated metrics and login failures; password, retention, filtering and isolation tests |
| `ttl` / `metadata` | Numeric TTL generation, resource tags and statistics; decimal/ancient TTL, replacement and tag contracts |
| `maintenance` | Bounded, resumable lifecycle/backfill/deletion and expiry steps; 130-row multi-batch builds interleaved with mutations |
| `backup` | Atomic small-table backup/restore with explicit 4 MiB bound; oversized snapshot rollback, account-generation scoping, rebuilt indexes and duplicate-target tests |
| `bootstrap` | Namespace reservation, atomic schema seed, encryption/admin/default-account initialization; repeated bootstrap and scoped destroy tests |
| `backend` | Factory composition and CLI configuration; override/conflict test and real init/serve/verify workflow |
| `runtime` | Scheduling and graceful shutdown only; single-step engine methods hold behavior, exercised by lifecycle contracts and CLI tests |

Read dependencies are installed only when a transaction writes. Read-only
operations return one TiKV snapshot without creating lock-only writes. Data
mutations snapshot-read table metadata and protect one of 256 schema guard keys,
selected by the complete item key. Schema/lifecycle changes update every guard
in the same transaction as metadata. This prevents an old writer from committing
after a schema change without forcing all unrelated writes through one key. The
reference store models TiKV's lock-only write versions so it detects this source
of contention too. The fencing unit test stages writes explicitly around a DDL
change and checks commit conflicts.

An account also has an immutable generation. Retained streams, backups and
idempotency keys cannot become visible to a newly created account that reuses an
old numeric account ID. A role/session name cannot carry conflicting live policy
or tag profiles: the existing authorization interface identifies sessions by
role/name rather than access-key ID, so use a distinct session name for a new
profile.

Maintenance commits at most 64 source items per index/TTL build, 64 physical keys
per deletion transaction (up to 16 transactions per deleting table per tick),
and 256 records per retention pass. Retention cursors are persisted. TTL and
stream expiry are approximate background processes; deletion may take multiple
ticks. Backfill skips pre-existing values that violate a new index's key schema;
subsequent invalid writes are rejected by the shared validation layer. Those
pre-existing items can still be corrected or deleted. Tests interleave both
operations with a multi-batch backfill.

## Build and exercise

```sh
cargo build -p extenddb --no-default-features --features tikv
./target/debug/extenddb init --backend tikv \
  --tikv-pd-endpoints 127.0.0.1:2379 --tikv-namespace my_deployment \
  --config extenddb-tikv.toml
./target/debug/extenddb serve --config extenddb-tikv.toml --foreground
```

The binary selects exactly one backend; enabling `tikv` together with a different
backend or with `dev-mode` fails compilation. TiKV does not require PostgreSQL at
runtime. The existing shared CLI still links SQLx for PostgreSQL-only diagnostics;
`catalog-check` now refuses other backends explicitly instead of trying their
connection descriptor as a PostgreSQL URL. Use `verify` for TiKV health checks.

Run all contracts, including the real cluster tests (which clean up unique
namespaces even after an assertion fails):

```sh
TIKV_PD_ENDPOINTS=127.0.0.1:2379 cargo test -p extenddb-storage-tikv \
  --features client,test-support -- --include-ignored
cargo clippy -p extenddb-storage-tikv --all-targets \
  --features client,test-support -- -D warnings
```

`devtools/run-tikv-tests --pd-endpoints 127.0.0.1:2379` creates a fresh namespace,
initializes and starts an HTTPS server, provisions test IAM credentials, runs
existing SDK tests, stops the server, then destroys only its namespace. It needs
Python 3.10+ with `pytest`, `boto3` and `requests`. Pass pytest paths after `--` to
choose a suite; use `--no-build` with an already built TiKV binary. CI also starts
PD/TiKV 8.5.5 and runs this path. TLS files generated by normal init are retained.

## Scope and deployment limits

* This is an experimental backend, not a PostgreSQL-to-TiKV migration tool. An
  existing PostgreSQL catalog/data set is not automatically copied or converted.
* Vector search/indexes and point-in-time recovery are explicitly unsupported.
  On-demand backup is limited to a 4 MiB encoded snapshot; larger backups fail
  before publishing data. IAM documents have a separate 4 MiB per-account limit.
* GSIs update synchronously; `index_propagation_delay_ms` does not introduce an
  artificial delay. Reads use fresh snapshots even when eventual consistency is
  allowed. Read calls in a paginated HTTP response may use separate snapshots,
  consistent with the shared storage interface.
* Use TiKV **transactional** storage. Never write this keyspace with RawKV or
  another application. Namespaces are logical isolation, not a TiKV ACL boundary.
  Configure cluster mTLS outside local testing.
* This backend does **not** advance the cluster-wide MVCC GC safepoint. Standalone
  TiKV needs an external, coordinated GC controller that accounts for all active
  transactions. A namespace-local process must not guess a safepoint for a shared
  cluster. Without GC, old MVCC versions consume increasing disk space. Operating
  that controller and validating restart/partition/GC behavior are production
  rollout prerequisites; this backend's TTL/retention jobs are not MVCC GC.
* Administrative enumeration and statistics still scan whole logical collections
  in bounded network pages. Benchmark representative tenant/table counts before
  deployment. Hot items and stream-shard counters remain contention points.
* Stop every server instance using a namespace before `destroy`. It deletes only
  the configured namespace, in bounded transactions; it does not stop or destroy
  the TiKV cluster. Namespace destruction is not atomic.

See [deployment configuration](../../docs/local-tikv-setup.md) and
[design rationale](../../docs/design/storage-tikv.md).

Hot-key admission uses 256 fair, process-local mutex slots before taking a TiKV
snapshot. A multi-item write acquires its slots in sorted order. These gates
reduce retry storms within one server; they do not provide distributed locking.
TiKV conflict checks remain authoritative across processes, and a bounded retry
budget can still return a conflict under sustained cross-instance contention.
