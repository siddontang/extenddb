# Dynalite compatibility run — 2026-10-03 (America/Los_Angeles)

## Source and scope

This run adds the independent [architect/dynalite](https://github.com/architect/dynalite)
test suite to the existing Parity and Alternator coverage. Dynalite is a
long-running DynamoDB emulator with approximately 1.1k GitHub stars at selection
time. Its README describes testing against live DynamoDB, including limits and
error messages. That is provenance, not a fresh certification of every assertion.

The external source is **Dynalite 4.0.0**, pinned to
[`c5e5b46ef5e51e7d907411c001db7839dd146088`](https://github.com/architect/dynalite/tree/c5e5b46ef5e51e7d907411c001db7839dd146088/test)
(September 18, 2025). Every tracked upstream file remained unchanged. All 19 test
files were loaded, including upstream-disabled cases. Slow tests were enabled.
No request contacted AWS, and no upstream assertion was changed to obtain a pass.

ExtendDB binaries were built from **`c1e4b57e769ef7f2314aac2e121772bf4242bd50`**,
with separate TiKV and SQLite Cargo features, and copied before execution.
The environment used Node 22.22.0, Mocha 11.8.0, aws4 1.13.2, should 13.2.3,
async 3.2.6, and local PD/TiKV 8.5.5. The resolved npm dependency graph is retained
in `devtools/dynalite-package-lock.json`.

Each file ran in a new temporary namespace/database with TLS and provisioned
IAM credentials. Throttling was explicitly disabled; table transition delay was
50 ms and index propagation delay was zero. These are compatibility runs, not
capacity-enforcement or performance measurements.

## Results

| Backend | Collected | Passed | Failed | Pending/skipped |
|---|---:|---:|---:|---:|
| TiKV | 1,059 | 337 | 716 | 6 |
| SQLite reference | 1,059 | 337 | 716 | 6 |

**The failing test titles are identical across both backends.** No TiKV-only
failure was identified by this suite. This does not establish full equivalence:
the suite does not validate TiKV distributed recovery, vectors, PITR or Streams.
An external case can contain many assertions and may stop at its first failure.
The 716 failing cases are not 716 independently confirmed implementation bugs.

Five pending cases come from upstream: two batch limit/throttling cases, two
benchmarks, and one billing-mode transition case. One adapter pending case,
`dynalite connections basic should connect to SSL`, starts a separate in-process
Dynalite server and cannot validate an external ExtendDB instance. It is counted
as pending, never as a successful ExtendDB test. Every actual request to ExtendDB
used verified HTTPS.

Both backends have the following per-file counts:

| Module | Passed | Failed | Pending |
|---|---:|---:|---:|
| batchGetItem | 20 | 30 | 1 |
| batchWriteItem | 19 | 35 | 1 |
| bench | 0 | 0 | 2 |
| connection | 0 | 37 | 1 |
| createTable | 14 | 103 | 0 |
| deleteItem | 20 | 44 | 0 |
| deleteTable | 1 | 7 | 0 |
| describeTable | 1 | 6 | 0 |
| describeTimeToLive | 2 | 6 | 0 |
| getItem | 24 | 27 | 0 |
| listTables | 8 | 10 | 0 |
| listTagsOfResource | 2 | 9 | 0 |
| putItem | 60 | 50 | 0 |
| query | 51 | 126 | 0 |
| scan | 73 | 82 | 0 |
| tagResource | 0 | 15 | 0 |
| untagResource | 0 | 13 | 0 |
| updateItem | 41 | 63 | 0 |
| updateTable | 1 | 53 | 1 |

The upstream failure groups are: **211 serialization, 428 validation, 40
functionality, and 37 connection/protocol cases**. Many serialization cases
expect Java-specific conversion text and capitalized `Message`; ExtendDB returns
different conversion text and lowercase `message`. Other cases pin validation
precedence, namespace, exact field sets, CORS, request-id format or presigned URL
support. Those differences must be reconciled individually, not normalized by
the adapter or all dismissed as stale tests.

## Findings to prioritize

1. **P1 — BatchGetItem response bound.** A separate fresh-table probe wrote 60
   items containing 300,000-byte strings and requested them in one batch. TiKV
   returned all 60, with **18,000,000 payload bytes and zero unprocessed keys**.
   This exceeds even 16 MiB before attribute names or JSON overhead. The shared
   handler currently has no response-size cutoff. The
   [BatchGetItem contract](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html)
   requires a partial result and retryable `UnprocessedKeys` when its size bound
   is reached. This probe confirms a common-engine gap independently of
   Dynalite's assumptions about physical partitions and throttling.
2. **P2 — Batch capacity accounting.** Four upstream cases expose uncharged
   missing BatchGetItem keys. A focused probe confirms that a single nonexistent
   key returns an empty `ConsumedCapacity` list for both read consistency modes.
   AWS documents minimum read consumption for missing keys in the same
   [BatchGetItem API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html).
   Another upstream case and probe show that deleting an existing item containing
   a 2,048-byte string via BatchWriteItem reports 1 WCU. The shared handler
   calculates the delete cost from the key rather than the deleted item; see
   [item capacity rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html).
3. **P2 — Expression and return-image candidates.** A focused replay confirms
   that `size(b) = :n` runs, whereas `(size(b)) = :n` and `((size(b)) = :n)` are
   rejected by the parser. The nested update case also returns only `[3]` for
   the changed list in `UPDATED_NEW`, while a subsequent GetItem contains the
   other changes. Its mix of nested ADD and out-of-range SET requires independent
   semantic verification before adopting all of the old test's expected values.
   Retain both returned and stored images; a response mismatch alone does not
   prove lost data.
4. **P2 — Protocol and exact validation behavior.** Presigned query-string
   authentication, CORS and negative-request handling differ. Missing fields,
   wrong types, overlapping paths, index options and malformed ARNs often fail
   on a different validation step or exact message. Prioritize accepted invalid
   requests and rejected valid requests before matching cosmetic text.
5. **P3 — Implementation-specific expectations.** Several tests demand exact
   Scan hash order/segment assignment, ordering among equal GSI keys, or numeric
   set order. AWS's [GSI documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html)
   does not promise ordering among identical index keys, and
   [sets are unordered](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html).
   Other strict object comparisons reject newer optional table fields. Keep the
   raw failures visible, but do not change storage ordering or remove valid API
   fields merely to satisfy these expectations.

This turn adds testing infrastructure and records findings; it does not change
product behavior or claim the failures are repaired. The focused probe results
are diagnostic observations, not additional passing conformance tests.

## Making the measurement reliable

The first naive run was not a valid measurement: Mocha reported more failure
events than collected tests. Upstream helpers send multiple HTTP requests in
parallel and throw assertions inside callbacks. After one fails, later callbacks
can be attributed to the next test. The adapter now retains the originating
test's async context, reports its first failure once, drains its in-flight HTTP
requests, and retains subsequent errors under that same owner. Assertions and
response values are untouched. A run with unaccounted late errors or inconsistent
event totals is an infrastructure failure, not a passing suite.

A second run in one shared namespace had valid event counts but 761 failures.
Large items left by failed cases interfered with later Scan fixtures. Giving
each module a fresh namespace reduced that to the reported 716 failures; the
45-case difference is isolation, **not a product repair**. Within-file upstream
fixture behavior is retained, so important defects still need focused replay.

Raw initial/scoped runs remain under `discussions/dynalite-20261003/`. Final
per-file reports are in `isolated/` (TiKV) and `sqlite/`, each with
`summary.json`, `exit-codes.json`, and individual JSON/NDJSON reports. Further
callback errors are in `supplementalErrors` and do not increase case counts.
All module teardowns completed. No ExtendDB server ERROR, panic or unknown-commit
event was found in those final server logs. TiKV startup separately reported
unavailable heap profiling; it stayed healthy during the run.

The adapter's ten offline Node tests pass. The Python runner tests plus existing
TiKV/Alternator runner regressions total 24 passes. An end-to-end runner check
with listTables and bench preserved the measured 8 passes, 10 failures and two
pending cases and correctly exited nonzero. Node's domain deprecation warning is
retained; this compatibility wrapper is scoped to the legacy callback suite.

Raw lifecycle logs can contain temporary credentials and stay ignored/local.
The reports contain only synthetic test data and redact provisioned credentials.
No external results were submitted upstream.

## Modules and reproduction

* `devtools/dynalite_adapter.cjs`: verifies upstream revision/cleanliness, maps
  its hard-coded hostname before signing, fences HTTP/HTTPS to the provided local
  endpoint, owns callback-error attribution, and writes case-level reports.
  Pure configuration/routing and injected transport boundaries have offline tests
  in `tests/test_dynalite_adapter.cjs`.
* `devtools/run-dynalite-tests`: invokes the existing lifecycle runner separately
  for each selected module and aggregates verified counts. Its aggregation
  contract is covered by `tests/test_dynalite_runner.py`. Exit 0 means no failing
  cases, 1 means test failures, and 2 means missing/inconsistent reports or an
  unexpected lifecycle exit. Always use a new output directory.
* `devtools/dynalite-package-lock.json`: exact dependency resolution for the
  pinned external package; no upstream source is vendored.

With Node 20+, the existing Python test environment, a TiKV binary built from
the desired revision and a dedicated local PD/TiKV cluster:

```sh
git clone https://github.com/architect/dynalite.git /tmp/dynalite-suite
git -C /tmp/dynalite-suite checkout c5e5b46ef5e51e7d907411c001db7839dd146088
cp devtools/dynalite-package-lock.json /tmp/dynalite-suite/package-lock.json
npm --prefix /tmp/dynalite-suite ci --ignore-scripts --no-audit --no-fund --package-lock=true

python devtools/run-dynalite-tests \
  --checkout /tmp/dynalite-suite \
  --binary /absolute/path/to/pinned/extenddb-tikv \
  --pd-endpoints 127.0.0.1:2379 \
  --output-dir discussions/dynalite-new-run
```

Use `--backend sqlite` and a separately built SQLite binary for the reference.
Use `--suite batchGetItem` (repeatable) for selected modules. For a single case,
invoke the adapter through `run-tikv-tests --command` and provide `--suite` plus
`--grep` with an anchored full-title regular expression. Keep HTTPS, provisioning
and cleanup in the lifecycle runner. Do not run upstream `npm test`: its lint
step uses `--fix` and can modify the source being measured.

```sh
node --test tests/test_dynalite_adapter.cjs
python -m pytest -q tests/test_dynalite_runner.py
```
