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
| `query` | Byte-range planning, ordered cursor pagination and scan segmentation | Numeric ordering, forward/reverse pages and segment union |
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
