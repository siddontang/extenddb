# TiKV branch integration and regression tests

This integrates `codex/tikv-capabilities` (`8f92fae`) into
`codex/tikv-backend` (`46a3b5a`). Merge commit `cfc7761` preserves both branches;
`c86a451` repairs vector/protocol issues exposed by the broader suites, and
`c0b6b41` repairs a live-lock retry boundary. Every commit carries
`Signed-off-by: siddontang <siddontang@gmail.com>`.

The earlier [repair report](testing-fixes-20261003.md) describes its historical
checkpoint. Vector, normalized IAM, streaming backups and PITR are now integrated.
**This is still an experimental backend; the complete compatibility matrix is
not green and no production PostgreSQL migration was performed.**

## Integration decisions

* Retain the audited TiKV client, expression/transaction validation, deletion-only
  update semantics, cursor checks, TTL/Streams/tag rules and snapshot statistics
  from the repair branch.
* Keep streaming backup and restore with GC-protected snapshots and leased
  staging from the capabilities branch. Read all three persisted layouts: inline
  snapshots, the repair branch's array chunks, and new per-item chunks. Legacy
  chunk counts are validated before publication; deletion reclaims either layout
  in bounded batches. No 90 MiB aggregate backup limit is reintroduced.
* Preserve restore's CREATING-to-ACTIVE lifecycle and DeleteBackup's DELETED
  response. All restored data and indexes exist before publication. Restore
  readiness never exposes an ACTIVE vector index beside an unusable base table.
* Keep timestamp-plus-eight-hex-suffix public backup ARNs, while internal chunk
  prefixes retain independent UUIDs. A publication collision is refused rather
  than overwriting a backup.
* Retain normalized IAM and schema-4 PITR. Upgrade with every namespace writer
  stopped, run `extenddb migrate --config ...`, and restart the new binaries.
  Legacy IAM conversion remains lazy and atomic; old backup formats remain
  readable. Mixed-version writes and in-place downgrade are unsupported.

## Failures found by the integrated run

The first full Parity run executed vector cases that the older backend skipped.
It reported 988 passes, 29 failures and 249 skips out of 1,266 cases. Seven new
failures exposed early index readiness and incomplete online-index lifecycle
rules; PITR enablement changed from failure to pass. The first Rust SDK run had
506 passes and eight failures, including related vector rules, backup ARN shape,
and one transient lock-recovery failure during a vector capability probe.

Vector repairs now derive allocation's advertised UPDATING status from persisted
index state. Backfill returns the table to ACTIVE while the index stays CREATING;
cancellation is allowed in that phase and table deletion refuses unfinished
indexes. Each worker step performs one phase change or at most 64 rows per
index, so large builds cannot monopolize the shared maintenance loop. The pure
`vector::validation` module validates effective billing mode, per-table count
and key/vector attribute redefinitions before catalog mutation. Tests use an
injected clock to observe allocation, cancellation, bounded progress and
publication on both memory MVCC and actual TiKV.

The remaining lock failure was a live optimistic secondary whose primary stayed
absent beyond a short status-lookup retry budget. The resolver now returns that
unresolved lock to the request plan's existing bounded live-lock retry loop.
It never treats absence as a commit/rollback decision and never forces rollback
before TTL expiry. Fault tests hold the primary absent through the inner budget,
then publish a commit and verify the exact secondary resolution. Application
unknown-commit handling is unchanged. See the
[client patch note](../vendor/tikv-client/EXTENDDB.md).

## Environment and result boundaries

PD/TiKV 8.5.5 and PostgreSQL 17 ran on private loopback ports with task-owned data
directories. Each API suite used a separate namespace/database and temporary IAM
credentials. Binaries were copied before execution. No suite contacted AWS.
Parity remains pinned to `ec55125a7ce1867baf0d7b1348592b68f7f4a56e`; Alternator
remains pinned to `b8a9fd4e49e8923b3edffa53ffec3b6e46c50906`. External assertions
and expected-failure markers were not edited. Parity and Alternator explicitly
ran with throttling disabled, matching the earlier comparison checkpoint.

Raw logs and JUnit/JSON evidence are retained in the ignored local directory
`discussions/merge-20261004/`. Raw server/bootstrap logs can contain temporary
credentials and must not be published. Suites ran concurrently on the same host;
durations are not benchmarks. Counts overlap and must not be added as unique
coverage. Skipped tests and capability-probe early returns do not prove support.

| Suite / checkpoint | Passed | Failed | Skipped / ignored | Evidence |
|---|---:|---:|---|---|
| Rust workspace, final fixes | 1,278 | 0 | 4 | `workspace-final.log`; live PG cases validated separately below |
| TiKV units + all real-cluster contracts, final fixes | 70 | 0 | 0 | `tikv-with-client-fix.log` |
| Vendored client unit tests, final fixes | 55 | 0 | 0 | `client-units-final.log` |
| Real catalog contention repetitions, final fixes | 10 runs | 0 | 0 | `catalog-stress.log` |
| Main Python, complete merge checkpoint | 1,142 | 1 | 23 + 1 XFAIL | `python-main.xml`; 1,167 cases; SSE/KMS is the remaining failure |
| Comprehensive Python, complete merge checkpoint | 331 | 0 | 0 | `comprehensive.xml` |
| Rust SDK, complete final run, four threads | 514 | 0 | 0 | `rust-sdk-final.log`; includes the 40,000-row restore fixture |
| Vector Rust SDK, focused lifecycle-fix checkpoint | 60 | 0 | 454 filtered | `vector-sdk.log`; superseded by the full final run |
| TiKV shared regressions + Vector/backup/PITR, final fixes | 93 | 0 | 0 | `regressions-final.xml` |
| PostgreSQL shared API regressions, merge checkpoint | 74 | 0 | 0 | `postgres-regressions.xml` |
| SQLite shared API regressions, merge checkpoint | 74 | 0 | 0 | `sqlite-regressions.xml` |
| Live PostgreSQL collation + vector storage | 45 | 0 | 0 | `postgres-storage.log` |
| Documentation consistency | 160 | 0 | 0 | `docs-final.log`; heuristic observations remain informational |
| Alternator, complete merge checkpoint | 649 | 6 | 29 + 13 XFAIL + 39 XPASS | `alternator.xml`; 736 cases, ordinary passes exclude XPASS |
| Parity, complete final run | 1,026 | 25 | 215 | `parity-final.json`; all 1,266 cases |

Workspace, TiKV storage and TiKV binary strict Clippy, workspace formatting and
Rust 1.88.0 locked TiKV compilation pass. The vendored client retains 24 inherited
compiler warnings. The final SDK, focused API and Parity server logs contain no
ERROR entries; expected negative-authorization and setting-change audit warnings
remain visible. No application commit-result uncertainty was hidden or replayed.

The complete main/comprehensive/Alternator runs tested `cfc7761`; the final
SDK, Parity and 93-case API run tested the product changes through `c0b6b41`.
The focused final API run covers the changed vector, backup, PITR and shared
validation paths. It is not presented as a second complete Python run.

Compared with the previous complete Parity repair checkpoint (976/23/267), the
final run has 50 more passes, two more failures and 52 fewer skips. The previously
failing PITR enablement case now passes. Of the 52 newly executed cases, 49 pass
and three fail. **No previously passing case became failing.** Two new failures
exercise the repository-wide unsupported PartiQL surface. The third samples
vector build state every five seconds and fails to observe ACTIVE table plus
CREATING index on a small build; the injected-clock contracts separately verify
that intermediate state on memory and real TiKV. This remains a recorded external
failure, not an assertion rewrite or a reason to slow production builds.

Alternator's six failures match the known remainder after the prior key-condition
fix; its earlier complete run had seven failures followed by a passing focused
rerun. This new complete run confirms that improvement. Skips/XFAIL/XPASS were
preserved.


## Remaining scope

* SSE/KMS is explicitly refused; the corresponding positive compatibility cases
  remain failures. No encryption implementation or success metadata was added.
* Precise item-size boundaries and several exact Scan error messages still need
  reconciliation with independent service measurements, as described in the
  earlier report. No empirical constants or test expectations were silently
  changed to make those cases pass.
* PartiQL remains unimplemented across the runtime and returns
  UnknownOperationException. The two newly reached vector/PartiQL failures are
  retained; implementing only a special rejection would not provide query support.
* One Parity lifecycle case does not observe a short intermediate phase at its
  five-second sampling interval. Deterministic contracts verify the state, but
  this external test is still reported as failing.
* The six Alternator failures concern duplicate AttributesToGet, response-field
  and tag-ARN expectations, an inconsistent index fixture, and a broken-connection
  fixture that does not trigger on this HTTP response path. They match the known
  remainder after the earlier key-condition diagnostic fix.
* Vector search is an exact partition scan. PITR requires a coordinated external
  GC controller that respects PD service safepoints; long outages beyond its
  24-hour lease can shrink the advertised recovery window. The 35-day retention
  barrier affects cluster-wide MVCC storage. Deleted-table recovery, independent
  cluster-loss backups, multi-node failure/partition acceptance testing, and
  PostgreSQL data migration remain outside this run.

Module contracts and operational limits are documented in the
[TiKV README](../crates/storage-tikv/README.md) and
[setup guide](local-tikv-setup.md).
