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
| `table` | Account-scoped names, immutable generations, schema fences | Duplicate creation, stale generation, account isolation, billing transitions and real catalog contention |
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

Catalog publication, IAM edits and table maintenance acquire a fair account
admission gate before opening a transaction. TiKV turns their protected account
reads into lock-only writes, which otherwise collide even when table names are
different. Bulk backup/restore copying runs outside this gate; only publication
holds it. Admission is local to an engine and reuses the bounded item-write
gates. KV account-generation and table-count guards still arbitrate other server
processes. Unknown commits are never replayed merely because admission is held.
The mixed publication contract races creation, backup, restore and reclamation,
then checks that the empty account can be deleted.

The first transport uses optimistic two-phase commit, with read keys explicitly
protected. Its abstraction follows the `TikvTransactionSource` / driver split in
the user's TiDB Rust reference at commit
`6a5b492097d5be084a0b1106da2c7f106c518498`, particularly
`rust/crates/tidb-txnkv/src/driver/tikv_opener.rs` and `retry.rs`.
The reference's vendored client requires Rust 1.93 and adds TiDB-specific APIs.
We pin published client `0.4.0` behind a feature and vendor its source with a
narrow missing-primary lock-resolution backport. See
[`vendor/tikv-client/EXTENDDB.md`](../../vendor/tikv-client/EXTENDDB.md) for the
upstream provenance, exact change and removal criteria. No absolute dependency
on the TiDB checkout is required. The adapter remains the replacement boundary.

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
workload benchmarks establish it. Large administrative scans still materialize
whole logical collections from bounded network pages. Backup and restore use
the separate streaming snapshot modules described below.

## Complete backend modules

| Module | Contract and validation |
|---|---|
| `catalog` | Account-fenced IAM transactions and global credential locators; IAM/namespace contracts |
| `catalog::records` | Independent principal/policy/key/membership/session records, snapshot projections and lazy v1 migration; >4 MiB accounts and failed-migration rollback |
| `catalog::management` | Users, groups, roles, inline policies, boundaries, keys, sessions; ownership, dependency and rollback assertions |
| `catalog::crypto` | AES-256-GCM with access-key ID as authenticated context; tampering, wrong ID/key and truncation tests |
| `catalog::credentials` | Snapshot credential resolution and expiry; deletion/revocation and temporary credential contracts |
| `catalog::authorization` | User/group/role policies, boundaries, tags and live session data; cross-account and expired-session tests |
| `catalog::operational` | Settings, bcrypt administrators, aggregated metrics and login failures; password, retention, filtering and isolation tests |
| `ttl` / `metadata` | Numeric TTL generation and resource tags; decimal/ancient TTL, replacement and tag contracts |
| `statistics` | Read-only snapshots and generation-fenced publication; no scan-plus-write commits, recreation and schema-change tests |
| `maintenance` | Bounded, resumable lifecycle/backfill/deletion and expiry steps; 130-row multi-batch builds interleaved with mutations |
| `backup` | Streaming snapshot backup and atomic restore publication; >4 MiB roundtrip, snapshot interleavings, missing-chunk failure, account scoping and rebuilt indexes |
| `pitr` | Timestamp selection, renewable history retention and historical recovery; old/current images, GC-window loss, re-enable isolation, unknown enablement and source/target IAM tests |
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

The [Alternator coverage report](../../docs/testing-alternator.md) documents a
second external API suite, its isolated adapter, failures and coverage gaps.
`tests/ttl_contract.rs` separately checks TTL renewal, attribute generation
changes, index cleanup and stream records using an injected clock, against both
the reference MVCC store and real TiKV.

## Scope and deployment limits

* This is an experimental backend, not a PostgreSQL-to-TiKV migration tool. An
  existing PostgreSQL catalog/data set is not automatically copied or converted.
* Vector indexes use exact partition scans with synchronous maintenance; no ANN
  acceleration is claimed. On-demand backups stream without a table-size cap.
  IAM has no aggregate account-size cap; each physical record is limited to 4 MiB.
* PITR restores historical data from a currently existing source table. It keeps
  up to 35 days while the retention worker is healthy. The 24-hour PD lease can
  expire during extended downtime; the reported window then narrows to surviving
  history. Deleted-table recovery, SourceTableArn and schema/capacity overrides
  are not supported. Backups/history in the same cluster do not protect against
  losing the cluster.
* Table statistics are observed in a read-only MVCC snapshot, then published in
  a small transaction fenced by table id and schema generation. They are
  approximate under concurrent item writes. This avoids locking every scanned
  row merely to update a count; an old observation cannot reach a recreated table.
* The nested `TxnNotFound` catalog contention failure is repaired by the audited
  client backport and covered by fault-specific and real-cluster tests. Unknown
  commit outcomes remain errors and are never blindly replayed.
* SSE/KMS encryption is not implemented: CreateTable and UpdateTable explicitly
  reject `SSESpecification.Enabled=true` before mutation. Accepting false does
  not assert that the underlying cluster encrypts data at rest. See the
  [test report](../../docs/testing-tikv.md) before evaluating PostgreSQL replacement.
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

## Vector module

`vector` owns physical row keys, synchronous maintenance and snapshot search.
`vector::score` owns f64 distance calculations and bounded top-k ranking.
`vector::validation` is a pure, snapshot-based validator for effective billing
mode, index counts and scalar/vector attribute redefinitions on UpdateTable;
failed validation happens before any catalog mutation.
`vector::build` implements the shared `VectorIndexBuild` primitives and a bounded
`vector_build_step` scheduler. Each step changes one phase or copies at most 64
rows per index; allocation, copying and publication cannot monopolize the common
maintenance worker. A pending online create reports the table as UPDATING until
backfill starts. Cancellation is accepted only in backfill, concurrent creates
are refused, and DeleteTable refuses unfinished indexes. CreateTable and restore
never advertise ACTIVE vector indexes before the base table accepts requests.
It persists progress per batch, serializes workers through the table document,
and protects base-row reads against concurrent updates/deletes. This provides
the shared lifecycle's write ordering without a deferred propagation queue.
CreateTable indexes start active; UpdateTable indexes progress from CREATING
through backfilling to ACTIVE. A failed batch leaves the index unpublished and
a restarted worker resumes its cursor. Dropped generations cannot be revived.

Online builds honor `vector_allocation_phase_delay_ms` (default 0),
`vector_backfill_batch_delay_ms` (default 0), and
`vector_index_min_creating_ms` (default 1000). Allocation lasts at least the
larger of the allocation setting and `control_plane_delay_seconds`; the minimum
creating duration starts when UpdateTable publishes the new index. Allocation
and publication deadlines are captured on creation. The inter-batch setting is
read for each committed batch and applies before the next batch, not after the
final batch. Deadlines are stored with the cursor, so restart preserves them.
The scheduler checks the current phase and deadline inside the same transaction
that advances it; competing workers cannot skip a delay using a stale catalog
image. It never sleeps while holding a transaction or stalls other maintenance
work. These settings make slow lifecycle observations reproducible without
changing production defaults. Clock-injected tests cover exact deadlines,
concurrent workers, malformed settings, bounded batches and ready publication
against both memory and real TiKV. Low-level `Build` primitives deliberately
remain directly callable for fault tests and recovery orchestration.

All three distance functions, HASH partitions, inline equality filters and
projection use the existing engine contracts. Search scans 64 rows at a time
in one MVCC snapshot and retains only top-k plus one page; read-only scans do
not accumulate commit dependencies. CPU/network cost grows with the selected
partition. Backups restore independent vector generations and rebuilt rows.
`vector_contract` runs identically against memory and real TiKV; scoring tests
cover zero, tiny, extreme, negative and tied scores.

## IAM record layout (catalog schema 2)

IAM management still computes a logical account transformation in memory, then
writes only changed independent records. A protected account header serializes
mutations and account deletion, keeping membership and global access-key
uniqueness atomic without storing one account-sized value. Authorization uses
principal prefixes, reverse membership and session-name indexes; normal data
writes do not acquire the IAM fence. Per-record bounds remain 4 MiB, comfortably
above public policy/tag limits. Large management operations still cost O(account
size) memory/CPU, and exceptionally large deletions can meet TiKV's transaction
limits; this is not a claim of unlimited management throughput.

Stop all servers using the namespace, run `extenddb migrate --config ...`, then
start the new binary. Schema 2 accepts legacy inline account documents and
converts each on its next successful management edit. The conversion and edit
commit together. Older binaries reject the schema version; do not bypass that
check or run mixed-version writers. Keep a deployment backup before upgrade;
downgrading requires restoring it, since old binaries cannot read normalized
records. Account generation prefixes and bounded garbage collection prevent
identity reuse from exposing retired IAM data.

## Bulk snapshot modules (catalog schema 3)

`backup::snapshot` keeps one owned MVCC snapshot, scanning eight source items
per network page. `backup` stores immutable per-item chunks and publishes a
small manifest after all chunks succeed. `backup::restore` rebuilds an
unreachable table UUID, one source item and its indexes per transaction, then
atomically installs its name and schema guards. Thus neither total table size
nor total backup size is constrained to one TiKV value or transaction. Individual
items still obey the normal DynamoDB size limit. Existing inline backups and
pre-merge array-chunk manifests remain readable. Legacy chunks are streamed one
array at a time and their total item count is checked before publication; missing
or inconsistent chunks fail without exposing a target. Deletion queues either
chunk layout for bounded cleanup. Catalog migration prevents older binaries
misreading new backups. Publication retains the normal CREATING-to-ACTIVE
transition, with all rows and indexes already complete before the name appears.

`staging` protects unpublished prefixes with five-minute leases refreshed on
every write batch. Expired/cancelled work is queued for bounded reclamation.
Publication and removal of the staging job commit together. Cleanup cannot
reclaim a successful publication even if its commit response was lost. A failed
job must be requested again; it is not resumed at a different snapshot.

`kv::gc` holds a PD service safepoint at snapshot timestamp minus one, renews
before reading further pages, and releases on drop (or expires after five
minutes after process loss). Transport failure, expired leases and already-GCed
history abort rather than substitute fresh reads. The narrow protobuf projection
implements GetMembers, GetGCSafePoint and UpdateServiceGCSafePoint with the same
TLS configuration as client-rust, validating cluster identity and PD errors.
It never advances the cluster GC safepoint. Your external GC controller must
honor PD service safepoints. These protect logical snapshots, not cluster-loss
recovery: backup data lives in the same TiKV cluster.

Contracts cover >4 MiB tables on both stores, mutation between snapshot pages,
atomic target visibility, missing chunks, expired staging leases and unknown
publication outcomes. GC tests reject expired history, renewal failures and
wrong-cluster responses. Signed HTTP tests verify the complete large-table
backup/restore workflow. Backup and restore run synchronously; very large tables
can exceed client/proxy request timeouts even though storage memory is bounded.

## Point-in-time recovery (catalog schema 4)

`pitr` separates durable enablement/retention state, pure recovery-time selection,
worker reconciliation and restore orchestration. The `Store` boundary supplies
TSO timestamps, historical snapshots and named GC barriers; its default methods
refuse unsupported capabilities. The reference store retains test-only history
when a retention barrier exists, with explicit time advancement and GC injection.
`BackupEngine::restore_table_at(RestorePoint)` exposes the time to storage;
backends without historical recovery keep the default refusal. The wire handler
validates the time choice before dispatch and invalidates the restored name's
cache only after success. No fallback to present-day data exists.

Enabling registers a provisional five-minute PD lease before committing a new
recovery generation, then extends it to 24 hours. Unknown enablement commits can
be reconciled by the worker. A separate worker renews a page of at most 64 table
leases each minute, independently of GSI/vector/TTL work. Barriers advance no
further than the enabled timestamp or the 35-day cutoff, whichever is newer.
The worker continues other entries after a renewal failure and reports that
failure. Monitor renewal errors; sufficiently many tables or slow/unavailable PD
can exhaust the lease interval. An abandoned provisional lease expires within
five minutes; a stopped server's established lease expires within 24 hours.

Disabling invalidates recovery immediately and removes its barrier, retrying
cleanup in the worker when PD is unavailable. Re-enabling starts a new generation
and window. Source deletion and namespace destruction release the corresponding
barriers. A stale in-flight renewal may outlive disable briefly; its unique
service identity cannot affect a re-enabled generation and expires within the
lease interval. The external GC controller **must honor PD service safepoints**.
These barriers hold back GC for the shared cluster, so 35-day retention can
increase cluster-wide MVCC disk usage, even for other namespaces.

Recovery reads committed historical base items in one GC-protected snapshot and
uses the source's current table/index settings. Scalar indexes are rebuilt;
pre-existing values invalid under a newer index schema are omitted from that
index using the online-build rules, while their base items are preserved. Vector
indexes use the same omission/validation helpers. TTL, streams, deletion
protection, tags and PITR are not enabled on the restored table. Publication uses
the same isolated-generation writer as on-demand restore.

`RestoreDateTime` selects the end of its physical millisecond (capped at the
current TSO for the current millisecond); `UseLatestRestorableTime=true` selects
the current TSO. Specify exactly one. Future, disabled or collected timestamps
are rejected. Only the fixed 35-day recovery period is implemented; other
`RecoveryPeriodInDays` values and restore overrides are explicitly refused.
Authorization checks restore permission on the source and read/write permissions
on the target, following the [AWS backup/restore IAM guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/backuprestore_IAM.html).

Upgrade with every namespace writer stopped, run `extenddb migrate --config ...`
to schema 4, and restart the new binary. Schemas 1–3 are accepted as migration
sources. Keep a deployment backup before upgrade; mixed-version writing and
in-place downgrade are unsupported. The retained window is operational recovery,
not an independent disaster-recovery copy or a PostgreSQL data migration.
