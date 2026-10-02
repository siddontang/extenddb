# Expanded DynamoDB compatibility testing

Measured on 2026-10-02 UTC against the TiKV branch based on `04212f6`, with the
fixes committed alongside this report. This is an experimental backend: the
results do **not** establish that it can replace PostgreSQL in production.
Concurrent catalog writes still expose a client/transaction integration issue,
and several DynamoDB features and validation details remain incomplete.

## External suites examined

* [Parity Suite DynamoDB conformance](https://github.com/paritysuite/dynamodb-conformance)
  was selected and run unmodified. Version **3.5.0**, commit
  `ec55125a7ce1867baf0d7b1348592b68f7f4a56e`, contains 1,266 cases in 143 files.
  It accepts an endpoint, normal AWS credentials and a TLS CA certificate, and
  covers core APIs, transactions, limits, error details and capability probes.
  Its Apache-2.0 source stays in a separate checkout; assertions are not copied
  into the product. Upstream results are evidence to investigate, not an AWS
  oracle measured by this run.
* [Scylla Alternator tests](https://github.com/scylladb/scylladb/blob/master/test/alternator/README.md)
  are another candidate: their documented targets include both Alternator and
  real DynamoDB. They were located but **not run** in this exercise.

No tests in this exercise targeted AWS or published results to an upstream
service. Every SDK suite used temporary credentials and an isolated local
namespace/database. All assertions and failure exit codes were retained.

## Environment and results

macOS arm64; PD/TiKV 8.5.5; published `tikv-client = 0.4.0`; Rust 1.94;
Python 3.12.14; Node 24.21.0; Vitest 4.1.11. Both backend comparison binaries
were built from the same modified source tree. SQLite is a reference backend,
not a claim about PostgreSQL behavior.

| Suite | Passed | Failed | Skipped/ignored | Notes |
|---|---:|---:|---:|---|
| `cargo test --workspace --locked` | 1,238 | 0 | 4 | Default workspace configuration |
| TiKV offline unit/contracts | 28 | 0 | 4 | Real-cluster tests separately invoked |
| TiKV real-store contracts | 3 | 1 | 0 | New 32-way catalog contention case failed |
| Main Python API full run | 1,024 | 3 | 35 + 1 xfail | Two GSI harness failures fixed afterward; SSE remains |
| Comprehensive Python suite | 331 | 0 | 0 | `tests/python` |
| Rust SDK full run, four threads | 510 | 2 | 0 | 512 tests; includes transport lifecycle regression |
| Extra offline harness checks | 3 | 0 | 0 | Environment isolation, concurrent name uniqueness, transport cancellation |
| Focused Python regression rerun | 43 | 0 | 0 | Final runner settings and CLI stdout fix |
| Parity Suite, TiKV, throttling enabled | 943 | 42 | 281 | Full suite, no test-name exclusions |
| Parity Suite, TiKV, throttling disabled | 958 | 41 | 267 | Explicitly labeled comparison run |
| Parity Suite, SQLite, throttling disabled | 1,019 | 32 | 215 | Same external checkout and options |

Strict Clippy passed for the TiKV binary and all TiKV storage targets; workspace
formatting passed. Counts from different suites overlap and should not be added
as unique behavioral coverage. Some existing Rust vector tests return early on
an unsupported-capability probe and are nevertheless reported as passed.

Throttling matters: enabling it caused one additional Parity assertion failure
and 14 additional capability-dependent skips. The disabled run is useful for
comparison, but is **not** a passing capacity/throttling test. Neither skipped
tests nor capability probes are counted as supported features.

The first runs found 13 main-Python failures and 68 Rust SDK failures. Several
were harness problems, not storage failures. Final results above use the fixes;
original logs were retained rather than overwritten.

The main full run finished before the final GSI setting/CLI output fix. Its
raw result remains 1,024 passed and three failed. The affected cases were then
rerun with the other changed API surfaces; only the SSE failure remains open
from that full run. A second complete 18-minute main-suite run was not used
to replace the raw count with an inferred all-pass result. The final CLI-only
change also passed all 16 application-crate unit tests.

## Remaining failures and deployment blockers

1. **Concurrent catalog transactions:** 32 simultaneous CreateTable calls in
   `real_tikv_table_contract` intermittently return a nested `TxnNotFound` while
   committing. The new contract uses both the reference MVCC store and a real
   cluster, and retains the failure rather than converting it into a skip.
   A statistics refresh also encountered `TxnLockNotFound` under the initial
   full-suite load. The adapter conservatively exposes unknown commit outcomes
   without replaying the operation. This must be resolved before production.
   Five additional isolated reproductions produced four passes and one failure;
   a passing run alone is not evidence that the issue has disappeared.
   The TiDB Rust reference's lock resolver handles nested transaction-not-found
   errors; published client 0.4.0 has different error-unwrapping paths. That is
   an investigation lead, not a demonstrated root-cause fix. Do not broadly
   classify such errors as retryable without establishing the commit phase.
2. **Rust SDK:** enabling PITR is unsupported; the large restore-completeness
   fixture exceeds the documented 4 MiB encoded backup limit. Both fail
   explicitly. The small-table restore and backup deletion cases now pass.
3. **Parity, shared observations:** 26 failing cases also fail on SQLite:
   number-size accounting, item-size/update boundaries, nesting depth, and
   exact Scan validation messages. A shared failure does not prove either
   backend correct; these need independent AWS measurements before changing
   the expected values or protocol semantics.
4. **Parity, TiKV-specific observations:** 15 failures pass on SQLite: SSE
   metadata, PITR enablement, empty secondary-index key handling in transactions,
   the UpdateItem index-key error message, and cancellation messages for invalid
   transaction keys. Six SQLite failures are outside that shared set, including
   vector operations that TiKV does not support. Different skip counts make a
   single combined compatibility percentage misleading.
5. **Other rollout gaps:** vector indexes/search, coordinated MVCC GC, durable
   large backups/PITR, PostgreSQL data migration, failure injection and recovery
   under node loss/partitions remain outside the validated scope. Single-node
   local testing cannot establish production availability or durability.

SSE requests currently do not provide a KMS integration or SSEDescription on
TiKV; do not infer storage encryption from accepting a request parameter.

## Fixes and regression coverage

* Query and Scan now apply the index's default projection when Select is
  omitted, while explicit projections and ALL_ATTRIBUTES retain LSI base-table
  reachback. Eight dual-target API cases cover both operations and four modes.
* Catalog-dependent billing validation rejects throughput on on-demand tables
  and unchanged provisioned-mode updates. Provisioned table descriptions omit
  billing summaries. Contract tests verify both mode transitions and rollback
  of other requested changes on a failed update.
* Fractional control-plane delays are accepted, with invalid/overflowing values
  rejected before publishing a table. Creating tables are unavailable to item
  operations. Restore exposes CREATING then ACTIVE only after the atomic data
  and index copy commits. Backup deletion returns DELETED; backup identifiers
  use the existing timestamp/suffix shape with a transactional collision check.
* The Rust SDK shared HTTP client now polls connector requests on a persistent
  runtime. Idle-timeout changes alone were insufficient for concurrent HTTP/2
  reuse. A network-free regression destroys four caller runtimes in sequence.
  Name suffixes are unique across threads, and index waiters include GSI status.
* The runner accepts arbitrary SDK commands, propagates endpoint/CA aliases,
  removes inherited config overrides/session tokens, and creates canonical
  account-scoped import/export roots. The GSI CLI tests use the selected binary
  instead of a hard-coded release path. CLI connection logs use stderr, preserving
  machine-readable `settings get` output, and the runner seeds the expected GSI
  delay setting. Initialization and failed-test runs both
  clean up only their own namespace or temporary SQLite database.

## Reproduce

Start a dedicated PD/TiKV cluster, activate a Python environment containing
`pytest`, `boto3`, `requests`, `pytest-xdist` and `psycopg2-binary`, and run from
the repository root. Commands preserve failures; no allow-failure flag is used.

```sh
cargo test --workspace --locked
cargo test -p extenddb-storage-tikv --features client,test-support
TIKV_PD_ENDPOINTS=127.0.0.1:2379 cargo test -p extenddb-storage-tikv \
  --features client,test-support -- --ignored

devtools/run-tikv-tests --pd-endpoints 127.0.0.1:2379 -- \
  tests --ignore=tests/python \
  --ignore=tests/test_cli_lifecycle.py \
  --ignore=tests/test_cli_container_readiness.py \
  --ignore=tests/test_cli_migrate_concurrency.py \
  --ignore=tests/test_cli_vector_catalog_migration.py \
  --ignore=tests/test_gsi_async_queue.py
devtools/run-tikv-tests --no-build -- tests/python
devtools/run-tikv-tests --no-build --command -- \
  cargo test --manifest-path tests/rust/Cargo.toml --locked -- --test-threads=4
```

The five excluded modules need a PostgreSQL service/physical catalog fixture.
This checkout has no Java/C++ suite sources or `external-suites.toml`, so those
were not run. This is broad project validation, not a claim that every backend,
platform, and external suite was exercised.

For the external suite, choose a new checkout directory. Use Node 24 or newer.
Do not update the pinned revision when comparing these results.

```sh
git clone https://github.com/paritysuite/dynamodb-conformance /tmp/dynamodb-conformance
git -C /tmp/dynamodb-conformance checkout --detach ec55125a7ce1867baf0d7b1348592b68f7f4a56e
npm --prefix /tmp/dynamodb-conformance ci --ignore-scripts --workspaces=false --no-audit --no-fund
mkdir -p discussions/tikv-expanded
devtools/run-tikv-tests --command --workdir /tmp/dynamodb-conformance -- \
  node node_modules/vitest/vitest.mjs run --reporter=verbose --reporter=json \
  --outputFile="$PWD/discussions/tikv-expanded/parity.json"
```

Repeat with `--throttling false` for the labeled comparison. Repeat with both
`--backend sqlite --throttling false` for the existing-backend reference. The
runner builds the selected backend unless `--no-build` is given. When retaining
multiple backend binaries, pass their absolute path with `--binary`; each run
pins a private executable copy before starting the server.

The local raw outputs and JUnit/JSON reports are in ignored
`discussions/tikv-expanded/`. They are not committed because logs can contain
environment-specific data. The full-run results use `python-final.xml`,
`comprehensive-final.xml`, `regressions-verified.xml`, `rust-sdk-final.log`, `parity-final.json`,
`parity-unthrottled.json` and `parity-sqlite.json`. The report records the useful
evidence without publishing temporary credentials or environment logs.
