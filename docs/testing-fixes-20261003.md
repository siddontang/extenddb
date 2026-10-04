# Failure analysis and repairs: 2026-10-03 UTC

The subsequent [branch integration report](testing-merge-20261004.md) records
Vector/IAM/streaming-backup/PITR integration, follow-up fixes and new tests.
The results and unsupported-feature statements below retain this earlier checkpoint.

This follows the [full local test run](testing-full-20261003.md), whose source
was `c7f057e`. Product fixes end at `500d538` on `codex/tikv-backend`. Failures are grouped by root
cause and impact below; a failing-case count is not a count of independent bugs.
**The backend remains experimental, and the complete compatibility matrix is
not green.** No production deployment or PostgreSQL data migration was made.

## Priorities and disposition

| Priority | Failure / consequence | Disposition |
|---|---|---|
| P1 | Concurrent CreateTable can return nested `TxnNotFound` during commit | Repaired the published client's missing-primary error-unwrapping path. The existing lock TTL / rollback algorithm now receives the error. Unknown commit outcomes are still never replayed. Fault-specific client tests and ten consecutive real catalog-contention repetitions pass. |
| P1 | BatchWriteItem may mutate earlier requests before rejecting a malformed later request | Preflight every table and request before writing. Seven new API regressions assert unchanged data, including invalid requests on a second table. This does not make valid BatchWriteItem requests transactional. |
| P1 | Large on-demand backup fails above a single 4 MiB KV value | Atomic manifest and roughly 1 MiB chunks, a documented 90 MiB encoded snapshot bound, legacy inline reads, and atomic restore publication. The original 40,000-row restore-completeness fixture passes. Missing chunks fail before publishing the restored table. |
| P1 | Background statistics can fail with `TxnLockNotFound` or an oversized Raft entry while locking an entire scanned table | Split read-only statistics capture from a small publication transaction, fenced by table id and schema generation. Tests assert zero scan-plus-write commits and reject stale observations after recreation or schema changes. |
| P1 | SSE options can appear accepted without encryption being implemented | TiKV now rejects enabled SSE/KMS requests on CreateTable and UpdateTable before mutation. This fixes false acknowledgement, **not** encryption support; positive SSE compatibility tests remain failures. |
| P1 | PITR enablement / restore capability absent | Unresolved feature gap. A durable history-retention, GC and restore design is required; returning ENABLED alone would be incorrect. Vector support and node-failure recovery also remain outside the validated scope. |
| P2 | Invalid expressions may pass on empty items or through short circuit evaluation | Added pure validation of function spelling/arity/types, document paths, duplicate clauses, query keys and legacy comparisons before evaluation. Constant operands valid for `size` / `attribute_type` remain supported. |
| P2 | List actions, nested sets and deletion-only updates produce wrong item images | SET list positions use the original image and ascending append positions; REMOVE uses descending original positions. Nested set actions work. Absent deletion-only updates stay absent on TiKV, PostgreSQL and SQLite. Legacy list ADD is desugared separately from modern ADD. |
| P2 | Invalid updated images or keys return the wrong transaction envelope | Preserve request-level validation versus per-item cancellation, rollback preceding writes, validate the resulting image's depth, and retain UpdateItem-specific size messages. |
| P2 | Invalid parallel-scan cursors silently skip data | TiKV validates cursor membership using the same stable partition assignment as its scan rows, including cursors whose item no longer exists. |
| P2 | Streams, TTL and tags have missing or inconsistent validation | Validate stream ARNs/limits/sequence bounds, required TTL fields and state transitions, tag charset/length/count/aggregate bytes, and resource scope. Tag merges validate inside storage transactions on TiKV, PostgreSQL and SQLite. |
| P2 | DescribeEndpoints advertises hard-coded localhost | Return the authenticated request authority, preserving proxy ports and IPv6. The external adapter passes its HTTPS option while retaining the strict origin fence. |
| P2 | Backup APIs return ResourceNotFound instead of TableNotFound | Added the documented backup-specific exception and an exact Rust SDK assertion. |
| P3 | Documentation checker reports 104 failures, many spurious | Replaced nonportable grep / source-string heuristics with independently tested extractors, accepted an explicit binary path, and documented the actually missing CLI/config/settings entries. The revised checker reports 160 passes and zero failures. |

## Evidence and implementation boundaries

The narrow TiKV client backport, its source revision, upstream issue and removal
criteria are documented in [the vendored-client note](../vendor/tikv-client/EXTENDDB.md).
The TiDB Rust implementation was used as a reference; there is no absolute
runtime/build dependency on the user's checkout. The client mock tests distinguish
live from expired locks and assert the exact rollback escalation. Application
retry classification was not broadened.

Pure validation lives under `core/expression/validation.rs` and
`core/validation/{legacy,query,streams,tags}.rs`. Engine handlers validate wire
input, and storage mutations validate final images inside their transaction.
The storage-independent API contracts are in
`test_batch_write_validation_atomicity.py`, `test_control_plane_validation.py`,
`test_expression_validation_contract.py`, and
`test_transaction_validation_atomicity.py`. Their assertions include unchanged
images, absent rows, correct cancellation positions and rollback, not only error
text. The TiKV reference and real-cluster contracts share the same assertions.

The [backend README](../crates/storage-tikv/README.md) documents module boundaries,
backup limits and unsupported capabilities. Backup size bounds are not a promise
that every 90 MiB source can be restored: TiKV transaction limits also include
rebuilt secondary-index entries. The on-demand snapshot path is not a PITR or
unbounded backup system.

## Test environment and scope

The versions and external revisions remain those in the preceding full-run
report: PD/TiKV 8.5.5, Rust 1.94, PostgreSQL 17.11, Parity 3.5.0 at
`ec55125a7ce1867baf0d7b1348592b68f7f4a56e`, and Alternator at
`b8a9fd4e49e8923b3edffa53ffec3b6e46c50906`. External assertions and upstream
expected-failure markers are unchanged. No test contacted AWS. Test binaries
were copied before starting each server, so later Cargo builds could not change
an ongoing run. Each run used a private namespace/database and temporary IAM
credentials. Concurrent suites share the host; durations are not benchmarks.

Raw logs and JUnit/JSON reports are retained in the ignored directory
`discussions/fixes-20261003/`. Do not publish raw server/bootstrap logs because
they can contain temporary credentials. Earlier failed checkpoints are retained
separately; focused follow-ups do not overwrite full-run counts.

Docker/MongoDB, container and other-OS jobs remain unavailable. Java/C++ suite
sources and their registry are absent from this checkout, as recorded in the
preceding report. The legacy shell CLI suite includes an AWS call and remains
outside this isolated run. MongoDB native update fast paths were not validated
by the PostgreSQL/SQLite/TiKV API regressions.

## Results and checkpoint boundaries

The following are actual complete-run counts, not inferred totals after focused
reruns. The main Python and SDK runs started before later boundary/statistics
repairs; their affected cases were rerun separately. Formatting and strict
workspace/TiKV Clippy pass. The vendored client emits 24 inherited compiler
warnings; these are not suppressed or treated as application lint failures.

| Suite / checkpoint | Passed | Failed | Skipped / ignored | Evidence |
|---|---:|---:|---|---|
| Rust workspace after repairs | 1,260 | 0 | 4 | `workspace-final.log`; 45 PG cases require the separate live run below |
| TiKV offline + all real-cluster contracts | 42 | 0 | 0 | `tikv-final.log`; includes all five real-cluster cases |
| Client-rust lock-resolution unit tests | 54 | 0 | 0 | `client-units.log`; direct vendored-client tests |
| Shared API regressions / TiKV | 74 | 0 | 0 | `regressions-stage6.xml` |
| Shared API regressions / PostgreSQL | 74 | 0 | 0 | `postgres-stage6.xml`; namespace cleanup also succeeded |
| Shared API regressions / SQLite | 74 | 0 | 0 | `sqlite-stage6.xml` |
| Live PostgreSQL collation + vector storage | 45 | 0 | 0 | `postgres-storage-stage6.log` |
| Main Python / complete earlier checkpoint | 1,104 | 3 | 35 + 1 XFAIL | `python-main-stage3.xml`; 1,143 cases collected |
| Main Python validation-precedence follow-up | 13 | 0 | 0 | `validation-precedence-final.xml`; fixes two full-run wording failures |
| Comprehensive Python / complete earlier checkpoint | 330 | 1 | 0 | `comprehensive-stage4.xml`; 331 cases |
| Comprehensive affected module follow-up | 13 | 0 | 0 | `comprehensive-followup.xml`; fixes the FilterExpression error label |
| Rust SDK / complete suite | 513 | 1 | 0 | `rust-sdk-stage4.log`; remaining PITR failure |
| Large restore / after statistics repair | 2 | 0 | 0 | `large-restore-stage7.log`; includes 40,000 rows, no statistics/lock/Raft error in server log |
| Statistics module extraction | 3 | 0 | 0 | `statistics-final.log`; behavior-preserving move after the full Rust run |
| Adapter + documentation-checker unit tests | 22 | 0 | 0 | `adapters-final.log` |
| Revised documentation consistency check | 160 | 0 | 0 | `docs-final.log`; heuristic doc/ADR observations remain informational |
| Parity / TiKV, throttling disabled | 976 | 23 | 267 | `parity-stage6.json`; all 1,266 cases collected |
| Alternator / TiKV, throttling disabled | 648 | 7 | 29 + 13 XFAIL + 39 XPASS | `alternator-stage6.xml`; all selected 736 cases collected |
| Alternator key-expression follow-up | 48 | 0 | 0 | `alternator-key-final.xml`; fixes the remaining NOT-operator diagnostic |

The comparable Parity failure count fell from **41 to 23**, and Alternator from
**97 to 7** in full runs. One of those seven was then repaired and its complete
48-case module rerun; the full 736-case count is deliberately left unchanged.
The ordinary Alternator pass column excludes upstream non-strict XPASS. Skips,
XFAIL and SDK capability-probe early returns do not establish feature support.
Counts overlap between suites and must not be summed as unique coverage.

The main suite's two new failures were the missing established prefix in legacy
Expected validation errors; the final prefix fix passes all 13 precedence cases.
The comprehensive suite's new failure was an unprefixed key-reference error in
a FilterExpression, also corrected and rerun. The remaining main-suite SSE case
now receives an explicit unsupported error instead of false acknowledgement.
It remains an unsupported positive capability test, not a passing test.

The earlier full Python and SDK server logs also exposed background statistics
failures (`TxnLockNotFound`, a heartbeat error, and an 8.9 MB Raft entry). These
are recorded even though they were not test-assertion failures. The subsequent
statistics split removes the scan-plus-write transaction entirely; three
structural/lifecycle tests and the large-restore rerun validate the repair.
This is evidence for the observed workload, not proof against every distributed
failure mode.

## Remaining failing behavior, in priority order

1. **P1 — unsupported capabilities:** SSE/KMS and PITR account for two Parity
   failures, the main Python SSE failure and the SDK PITR failure. Encryption
   key management and durable historical recovery need separate implementations
   and failure-injection acceptance tests. No success metadata was fabricated.
2. **P2 — exact item-size boundaries:** six Parity cases disagree with the
   documented approximate number-byte formula. Ten others expect empirical
   UpdateItem action overheads and key-byte exclusions. The generic stored
   400 KiB limit is enforced, but these exact service boundary models are not
   established. Retain the failures and reconcile against independent live AWS
   measurements before adopting constants such as 19 bytes per SET clause.
3. **P2 — protocol edge cases / conflicting evidence:** five Parity Scan tests
   expect exact messages that differ from existing repository ground-truth
   assertions. Alternator still rejects the accepted legacy AttributesToGet
   duplicates, expects fewer DeleteTable fields in two cases, and expects
   AccessDenied for a malformed tag ARN where repository measurements expect
   ValidationException. The current
   [DeleteTable response schema](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html)
   includes those optional fields; this does not prove which fields every live
   response emits. These differences need reconciliation, not a silent assertion
   rewrite.
4. **P3 — external fixture assumptions:** Alternator `test_17119a` supplies base
   key `p` to an index whose fixture declares only `x` as HASH, while the test's
   comments call `x` a range key. It is rejected as an invalid key condition.
   The broken-connection case monkeypatches `urllib3.HTTPResponse.read_chunked`;
   this server returns a fixed byte body and the intended failure is not
   triggered on this path. The assertion remains a recorded failure. Changing
   product semantics or weakening endpoint isolation to satisfy either fixture
   would not establish compatibility.

Existing deployment gaps also remain: GC safe-point coordination, multi-node
failure recovery, the finite backup/catalog limits, unsupported vector search,
and migration of PostgreSQL data. Passing local regression tests alone does not
justify replacing a production PostgreSQL deployment.

## Reproduce the focused checks

Use the isolated-run instructions and full-suite commands in the preceding
report. With a dedicated local PD/TiKV cluster and a pinned binary:

```sh
python devtools/run-tikv-tests --binary /path/to/extenddb-tikv --no-build \
  --pd-endpoints 127.0.0.1:2489 -- \
  tests/test_batch_write_validation_atomicity.py \
  tests/test_control_plane_validation.py \
  tests/test_expression_validation_contract.py \
  tests/test_transaction_validation_atomicity.py

TIKV_PD_ENDPOINTS=127.0.0.1:2489 cargo test -p extenddb-storage-tikv \
  --features client,test-support --locked -- --include-ignored
cargo test --workspace --locked
EXTENDDB_BINARY=/path/to/extenddb-postgres tests/cli/test-doc-consistency.sh
```

Repeat the API contracts with `--backend sqlite` and a SQLite binary. For
PostgreSQL, initialize a disposable catalog/data database, provision credentials
as documented in the project test workflow, and run the same four files.
The first temporary PostgreSQL harness used the wrong admin role for teardown;
its tests passed, its two owned databases were explicitly cleaned up, and the
corrected full 74-case run passed with successful teardown.

## References used to resolve behavior

* [BatchWriteItem API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
  distinguishes whole-request validation from unprocessed valid writes.
* [DynamoDB update expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html)
  describes list ordering and one clause of each action kind.
* [Legacy comparison operators](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html)
  define the accepted constant types and comparison arity.
* [GetShardIterator](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html),
  [tagging](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tagging.html),
  and [CreateBackup](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateBackup.html)
  supply the parameter limits and backup-specific error class.

These references and pinned external observations guide fixes; they are not a
fresh live measurement of every disputed edge case against AWS.
