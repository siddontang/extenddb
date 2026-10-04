# Remaining compatibility errors: investigation and repairs

This follows [the integration checkpoint](testing-merge-20261004.md), starting
from `1fc856a` on `codex/tikv-backend`. The investigation includes all 25 Parity
failures, six Alternator failures and the main Python SSE failure, plus new
failures found during independent boundary and concurrency checks.

Product repairs are committed as `ebadca9` (shared protocol/size contracts) and
`81c7552` (TiKV catalog admission/vector scheduling). The accompanying test-tool
and report commit completes this checkpoint. All carry
`Signed-off-by: siddontang <siddontang@gmail.com>`.

**Several implementation bugs are fixed. SSE/KMS and PartiQL remain unsupported;
the complete external suites are not green. This is not a production migration
or evidence of failure tolerance across multiple servers.**

## Priority and disposition

| Priority | Finding | Disposition |
|---|---|---|
| P1 | Concurrent catalog publication occasionally exhausts TiKV lock resolution; SDK retries can hide a server error even when the suite passes | Coordinate account writes before opening a snapshot; retain distributed KV guards and unknown-commit refusal |
| P1 | PostgreSQL and SQLite factories use 400,000 bytes for post-update validation, rejecting valid items below 400 KiB | Use the shared 409,600-byte default; check acceptance, rejection and rollback on four write surfaces across all three backends |
| P2 | Numeric sizing combines integer/fraction digits before rounding, undercounting values such as `1.5` and `100.5` | Round the two significant digit groups separately; preserve normalization and the 21-byte cap |
| P2 | Get/Query/Scan/BatchGet accept duplicate legacy `AttributesToGet` | Validate in a pure shared function before data access; preserve literal dotted/bracketed names |
| P2 | DeleteTable exposes DescribeTable-only schema fields | Apply a deletion-summary projection at the shared HTTP operation layer |
| P2 | TiKV ignores vector lifecycle delay settings; short intermediate states evade a five-second external poll | Persist allocation, batch and activation deadlines, check each phase atomically, and expose the existing minimum-creating setting |
| P2 | Positive SSE/KMS and PartiQL compatibility tests reach missing features | Retain explicit failures; do not claim encryption or query support |
| P3 | Provisioned throttling test sometimes reports no throttling because SDK retries wait for refill | Disable retries for the measured reads/writes and reject unexpected error classes |
| P3 | External disconnect injection only patches a chunked-response method | Add a real signed socket-disconnect regression that works with either response framing |
| P3 | Fifteen Parity assertions pin one side of documented regional differences; two other Alternator expectations need separate treatment | Preserve raw outcomes and document evidence below |

## Design and regression boundaries

### Account publication and TiKV contention

Repeated SDK runs exposed `TiKV commit outcome unknown: Failed to resolve lock`
during CreateTable and backup publication. A nominally successful SDK run had
two such errors; another run also hit an unrelated retry-sensitive throttling
assertion. Test exit status alone was therefore insufficient evidence.

Protected account reads become lock-only writes in the optimistic TiKV adapter.
Independent table names and backup names still share that account key; table
publication/reclamation also updates the account table counter. Reuse the
engine's bounded, fair admission gates for account creation/deletion, IAM edits,
table creation/maintenance and backup/restore publication. Acquire admission
**before** starting a transaction so waiting requests do not retain old snapshots.
Bulk copying does not hold the gate. No error is newly classified as a confirmed
conflict, and no unknown commit is blindly replayed.

Admission coordinates clones of one engine, not separate server processes.
Account-generation checks, table-count guards and TiKV conflict detection remain
the correctness boundary. Cross-process contention and fault-injection testing
remain necessary before production use.

The new mixed publication contract runs 24 concurrent creates, restores and
backups, verifies 17 published tables, checks account deletion is refused, races
four cleanup workers, and finally deletes the empty account. It runs unchanged
against memory MVCC and real TiKV. Existing conditional-write, migration,
backup/PITR and unknown-commit contracts remain enabled.

### Item limits and numbers

The pure sizing function now treats integer and fractional significant digits
as separate packed groups, normalizes exponent notation for values constructed
directly in Rust, and retains zero/sign behavior and the 21-byte maximum. Tests
cover zeros, exponent extremes, trailing zeros, long negative values, sets,
documents and UTF-8 attribute names.

The public [capacity calculation guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html)
describes numeric size approximately. Independent byte-boundary probes against
the official DynamoDB Local 3.3.1 confirm costs of 3 bytes for `1.5`, 4 for
`1.234`, 5 for `3.14159`, 4 for `100.5`, and at most 21 for a negative 38-digit
value. Local is a useful independent implementation, not proof that every AWS
region has identical behavior.

Forty-four API cases use fixed observed costs, not the implementation's formula.
Each accepts exactly 400 KiB, rejects one extra byte, and checks that a rejected
Put/BatchPut/Update/TransactUpdate leaves the old item intact. These tests exposed
the separate 400,000-byte factory bug on both SQL backends. The shared default
now supplies their post-update bound. A long key keeps the final-item limit
binding even in regions with a separate UpdateItem statement-size limit.

### Legacy projections and deletion responses

`validation::legacy::validate_attributes_to_get` is synchronous and performs no
I/O. All four read handlers call it before fetching items; BatchGet validates
each table in its preflight pass. Twelve API cases cover existing/missing items,
duplicate names, Unicode and literal `a`, `a.b`, `a[0]` names. DynamoDB Local also
rejects duplicates on all four surfaces. The old projection-trie unit only
described an internal helper and was not evidence of API acceptance.

DeleteTable retains identity, capacity and deletion status but omits
CreationDateTime, KeySchema, AttributeDefinitions, GlobalSecondaryIndexes and
LocalSecondaryIndexes. Backends still return the complete catalog image for
internal use. This agrees with the independent Alternator assertions and the
[AWS DeleteTable response example](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html).
The API schema lists optional fields; that does not imply every response includes
them. Two new tests cover tables with and without secondary indexes. DynamoDB
Local differs on this response and is not used as its reference.

### Vector lifecycle observability

Allocation delay, batch delay and the minimum creating duration are explicit
settings, not hard-coded sleeps added to satisfy a test. Persisted deadlines
survive restart and use the injected engine clock. Each maintenance transaction
reloads the index, checks the deadline and advances one phase or at most 64 rows.
Concurrent workers cannot act on a stale phase decision. Missing/deleted indexes
cancel normally. Invalid delays fail before publishing metadata.

Memory and TiKV contracts advance a fake clock over a 130-row build, race workers,
check exact boundaries and verify the completed search. Public build primitives
remain clock-independent seams for fault/recovery tests. Defaults remain allocation
0 ms, batch 0 ms and minimum creating 1,000 ms. With the explicitly recorded
`vector_index_min_creating_ms=8000`, the original three external lifecycle tests
pass. The default full run still reports one missed intermediate-state assertion;
that failure is not removed from its count.

## External failures retained

### Parity: 19 failures

* **One SSE/KMS case:** TiKV refuses enabled SSE. Correct support needs a real key
  provider, key lifecycle and encryption of every persisted representation,
  including relevant indexes, streams and backups. Success metadata alone is
  insufficient. See [AWS encryption at rest](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/EncryptionAtRest.html).
* **Two PartiQL cases:** ExecuteStatement is unimplemented across this runtime.
  One case expects a vector-index rejection; another reads a base-table vector
  attribute. Returning a special error only for the first would conceal the
  missing query surface. A complete implementation needs parsing/binding,
  execution, authorization, parameters and pagination, as described by the
  [ExecuteStatement API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html).
* **Ten UpdateItem size cases:** the pinned external suite records a statement
  accounting rule in 22 regions and a flat final-item rule in ten. ExtendDB keeps
  the latter, with the repaired exact numeric costs and 400 KiB bound. The suite
  explicitly says its per-statement constants have no documented basis. See the
  [pinned regional explanation](https://github.com/paritysuite/dynamodb-conformance/blob/ec55125a7ce1867baf0d7b1348592b68f7f4a56e/tests/tier3/limits/itemSizeBySurface.test.ts#L291).
* **Five Scan message cases:** error classes and validation behavior are correct;
  exact strings differ. The suite records its newer strings in four regions and
  the older wording in 29. See its
  [pinned rollout explanation](https://github.com/paritysuite/dynamodb-conformance/blob/ec55125a7ce1867baf0d7b1348592b68f7f4a56e/tests/tier3/error-messages/scan.test.ts#L40).
  These are compatibility choices requiring a named target contract, not proof
  that one globally correct string or update-size rule exists.
* **One vector lifecycle observation:** default execution can finish between the
  external test's five-second samples. The explicit-delay comparison above
  observes the intended state with unchanged assertions.

### Alternator: three failures

* `test_17119a` uses a fixture whose index has only `x` as its HASH key but queries
  it with both `p` and `x` key conditions, describing `x` as a range key. TiKV
  correctly refuses this shape. Independent DynamoDB Local queries refuse the
  two-condition request and return the row when queried by `x` alone. See the
  [pinned fixture/test](https://github.com/scylladb/scylladb/blob/b8a9fd4e49e8923b3edffa53ffec3b6e46c50906/test/alternator/test_gsi.py#L1957).
* `test_batch_write_item_large_broken_connection` injects failure through
  `urllib3.HTTPResponse.read_chunked`, which is not called for this response's
  Content-Length path. The new socket regression closes three real connections
  before reading the large Query response body, then verifies all rows and a
  subsequent update/read. The raw upstream injection assertion still fails.
* `test_tag_resource_incorrect` expects AccessDeniedException for an unparseable
  ARN; current validation and the repository's captured expectation use
  ValidationException. The Local version does not implement TagResource, so it
  cannot adjudicate this discrepancy. Keep it unresolved pending an appropriate
  service capture instead of weakening ARN/account validation.

No external assertion, skip marker, expected failure or target-specific branch
was changed to improve these counts. The earlier report's interpretation that
duplicate AttributesToGet was accepted is superseded by this investigation.

## Validation and evidence

PD/TiKV 8.5.5, PostgreSQL 17 and DynamoDB Local 3.3.1 used private loopback ports
and task-owned data. API suites used separate temporary namespaces/databases and
IAM credentials. No tests contacted AWS. Parity remains pinned to
`ec55125a7ce1867baf0d7b1348592b68f7f4a56e`; Alternator to
`b8a9fd4e49e8923b3edffa53ffec3b6e46c50906`. Their comparison runs explicitly
disable throttling; application regressions and the SDK suite enable it.

Raw logs and JSON/JUnit reports are retained locally under `discussions/errors/`
(gitignored). Do not publish raw bootstrap/test logs: they can include temporary
credentials. The runner's retained `server.log` and `run.json` allow inspection
after namespace cleanup. Counts overlap and are not additive unique coverage.

| Suite | Passed | Failed | Skipped / other | Evidence |
|---|---:|---:|---|---|
| Rust workspace | 1,281 | 0 | 4 ignored | `workspace-verified.log` |
| TiKV units and memory/real-cluster contracts | 74 | 0 | 0 | `tikv-verified.log` |
| Real catalog creation/publication stress | 10 runs, two contracts each | 0 | 0 | `catalog-stress-verified.log` |
| Rust SDK, four threads | 514 | 0 | 0 | `rust-sdk-verified.log` |
| Final strict throttling assertions | 2 | 0 | 512 filtered | `throttling-verified.log` |
| New API regressions, TiKV | 59 | 0 | 0 | `tikv-api-verified.xml` |
| New API regressions, PostgreSQL | 59 | 0 | 0 | `postgres-verified.xml` |
| New API regressions, SQLite | 59 | 0 | 0 | `sqlite-verified.xml` |
| Main Python, full checkpoint | 1,189 | 1 | 23 skips + 1 XFAIL | `python-main.xml`; 1,214 cases, SSE is the failure |
| Comprehensive Python, full checkpoint | 331 | 0 | 0 | `comprehensive.xml` |
| Parity, full final product run | 1,032 | 19 | 215 skips | `parity-verified.json`; 1,266 cases |
| Alternator, full final product run | 652 | 3 | 29 skips + 13 XFAIL + 39 XPASS | `alternator-verified.xml`; 736 cases, ordinary passes exclude XPASS |
| External vector lifecycle, explicit 8,000 ms comparison | 3 | 0 | 0 | `vector-observed.json` and `vector-observed/run.json` |
| Documentation consistency | 161 | 0 | Informational observations retained | `docs-verified.log` |

Workspace formatting and strict Clippy pass for the workspace, TiKV binary and
TiKV storage test targets. The vendored client retains its 24 inherited compiler
warnings. Final SDK, Parity, Alternator and all three API server logs contain no
ERROR entries or lock-resolution failures. The final throttling rerun includes
the additional assertion that all 50 outcomes are success or the expected
throttling error. No fault-tolerance claim is inferred from these local runs.

Compared with the integration checkpoint, Parity loses six failures and
Alternator loses three. Remaining failures are retained exactly as categorized
above; skips, XFAIL and XPASS do not establish feature support.

The complete main Python and comprehensive runs preceded the final legacy
projection, 38-digit numeric-cap correction and account-admission edits. Their
affected paths are covered by the final cross-backend API, storage-contract and
SDK runs; they are not represented as a second complete main/comprehensive run.

Temporary API namespaces/databases were destroyed after each run. The task-owned
PostgreSQL, DynamoDB Local, TiKV and PD processes were stopped at completion.

Reproduction uses `devtools/run-tikv-tests --pd-endpoints HOST:PORT
--artifacts-dir PATH -- ...`; add `--setting KEY=VALUE` for an explicitly labeled
configuration comparison. See the [module README](../crates/storage-tikv/README.md)
for storage contracts and [admin settings](manuals/05-admin-guide.md) for the
vector lifecycle controls.
