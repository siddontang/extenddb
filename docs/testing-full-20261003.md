# Full local test run: 2026-10-03 UTC

Source tested: `c7f057e` on `codex/tikv-backend`. This run changes no product
code or assertions. It repeats the complete repository API suites against TiKV,
executes backend-specific PostgreSQL and SQLite tests, and repeats the pinned
external comparisons from [the TiKV report](testing-tikv.md) and
[the Alternator report](testing-alternator.md). **The run is not green; TiKV is
still experimental.**

## Environment and isolation

macOS 15.6 arm64, Rust 1.94.0, Python 3.12.14, PD/TiKV 8.5.5,
`tikv-client` 0.4.0, PostgreSQL 17.11, pgvector 0.8.7. The repository Python
suites use a fresh environment installed from `requirements.txt`: pytest 9.0.3,
boto3 1.43.14, botocore 1.43.108. Alternator uses pytest 9.1.1 and
boto3/botocore 1.43.107. Parity uses Node 24.21.0 and Vitest 4.1.11; the npm
launcher tests use Node 22.22.0.

All binaries were rebuilt from the same source and copied to distinct paths
before execution, so subsequent Cargo builds could not change a running suite's
backend. PD/TiKV and PostgreSQL use fresh task-owned data directories and
loopback ports; API runs use isolated namespaces/databases and temporary IAM
credentials. No tests contact AWS. Several suites ran concurrently on the same
host and TiKV cluster; elapsed times are not performance measurements.

Raw logs, JUnit XML and Parity JSON are retained locally under
`discussions/full-tests-20261003-1707/` (gitignored). Raw output can contain
temporary test credentials and is not suitable for publication. The public
results below contain no credentials.

## Repository results

| Suite | Passed | Failed | Skipped/ignored | Detail |
|---|---:|---:|---:|---|
| Rust workspace, default features | 1,241 | 0 | 4 | Includes 45 PostgreSQL tests that return early without a connection; those were then executed with a real server below |
| TiKV binary/app/server feature tests | 61 | 0 | 1 | Ignored application documentation example |
| TiKV offline unit and contract tests | 31 | 0 | 5 | Five real-cluster cases executed separately below |
| TiKV real-cluster contracts | 4 | 1 | 0 | Concurrent table-creation contract fails |
| Main Python API suite, TiKV | 1,044 | 1 | 35 + 1 XFAIL | 1,081 cases; full run, no failed-case exclusions |
| Comprehensive Python API suite, TiKV | 331 | 0 | 0 | `tests/python` |
| Rust SDK suite, TiKV, four threads | 512 | 2 | 0 | PITR and large snapshot backup |
| PostgreSQL storage integration | 45 | 0 | 0 | Three collation and 42 vector-control-plane tests, real pgvector installed |
| PostgreSQL CLI, migrations and queue recovery | 38 | 0 | 1 | Five test modules, after repairing PostgreSQL installation |
| PostgreSQL Unix socket follow-up | 1 | 0 | 0 | Previously skipped case, with the task server's `PGHOST` and `PGPORT` |
| SQLite durable GSI queue recovery | 5 | 0 | 0 | Includes forced-stop warnings during teardown |
| SQLite dev-mode authorization | 5 | 0 | 0 | Seeded dev credential with SigV4 |
| npm launcher | 4 | 0 | 0 | Real dev binary; persistence, memory mode and conflicting options |
| SQLite ignored vector timing harness | 1 | 0 | 0 | Executed explicitly; not a performance acceptance test |
| Legacy documentation checker | 109 | 104 | 0 | Checker defects and documentation findings; see below |

Workspace formatting and strict Clippy pass. Clippy covers all workspace
targets and the TiKV storage targets with `client,test-support`. Counts across
suites overlap and must not be summed as unique behavioral coverage. Ignored
documentation examples remain ignored; the SQLite timing test and all five
ignored TiKV cluster cases were explicitly executed.
Some Rust SDK vector cases return early after an unsupported-capability probe
and are nevertheless reported as passed. TiKV vector support is not established
by the SDK pass count.

The main Python run takes 1,229 seconds. Its 35 skips are 13 MongoDB-specific
cases, 12 unsupported-vector cases, five dev-mode cases and five SQLite queue
cases. The latter ten run successfully in the dedicated rows above. The one
XFAIL concerns a 15-minute stream-iterator expiration test. Most warnings in
the main run are from local HTTPS tests using an unverified self-signed
certificate; the raw warning count is 1,029.

The first PostgreSQL CLI attempt produced **34 failures, four passes and one
skip** because the Homebrew PostgreSQL installation lacked runtime file links;
connections specifying `TimeZone=UTC` failed before initialization. Its log and
JUnit remain separate as `postgres-cli.*`. After restoring missing PostgreSQL
and pgvector links, `SET TimeZone='UTC'` and `CREATE EXTENSION vector` succeeded.
The full 39-case rerun is `postgres-cli-retry.*`; the 45 storage tests also ran
against the actual server, with no missing-server early return. Homebrew's
stalled PostgreSQL postinstall process was stopped; the test server itself was
initialized and managed directly with `initdb`/`pg_ctl`.

The CLI rerun's one skip probes libpq's default `/tmp/.s.PGSQL.5432`, whereas
this cluster uses a private socket directory and port 25489. The unchanged test
passes when explicitly given `PGHOST`/`PGPORT`. This focused follow-up is
reported separately; it does not rewrite the full rerun's raw skip count.

The legacy documentation script ran unchanged with GNU grep 3.12 and the
fresh PostgreSQL debug binary temporarily linked at its hard-coded release
path. This does not claim a release build. The link was removed afterward.
Its 104 failures are not 104 demonstrated documentation defects: for example,
`grep -rq "$flag" ...` interprets `--config` as an option instead of a pattern,
and another check treats arbitrary quoted source strings as runtime settings.
The raw failure count is retained without silently repairing its assertions.

## External results

Parity Suite 3.5.0 remains pinned to
`ec55125a7ce1867baf0d7b1348592b68f7f4a56e`. Each run collects all 1,266 cases.
Alternator remains pinned to Scylla 6.2.0 commit
`b8a9fd4e49e8923b3edffa53ffec3b6e46c50906`; each run collects the adapter's
complete 25-module, 736-case selection. The nine modules outside that selection
are listed in the Alternator report and were not executed here. External test
assertions and expected-failure markers are unchanged.

| Suite/backend | Passed | Failed | Skipped | XFAIL | XPASS |
|---|---:|---:|---:|---:|---:|
| Parity / TiKV, throttling enabled | 942 | 43 | 281 | — | — |
| Parity / TiKV, throttling disabled | 958 | 41 | 267 | — | — |
| Parity / SQLite, throttling disabled | 1,019 | 32 | 215 | — | — |
| Alternator / TiKV, throttling disabled | 558 | 97 | 29 | 14 | 38 |
| Alternator / SQLite, throttling disabled | 559 | 96 | 29 | 21 | 31 |

Both Alternator runs use the final 120-second per-test budget in one complete
execution, rather than combining shorter runs with focused reruns. There are
no setup errors. Non-strict upstream XPASS is separate from ordinary passes;
neither skips nor XFAIL establish support.

Parity has 26 failing case names common to TiKV and SQLite with throttling
disabled. Its two extra failures with throttling enabled report
`ProvisionedThroughputExceededException`: the empty-set-member Query fixture
and the missing-sort-key DeleteItem error-message case. This records a
configuration-sensitive result, not proof that capacity behavior is correct.

Alternator has 94 ordinary failing case names in common. Three additional
TiKV failures concern nonexistent stream identifiers; two additional SQLite
failures concern continuous-backup metadata and fetching an unprojected LSI
attribute. Upstream XFAIL/XPASS results differ too, so comparing only the net
failure-count difference would hide behavior differences.

## Remaining failures

* `real_tikv_table_contract` again returns nested `TxnNotFound` during concurrent
  CreateTable commits. The Rust SDK server log also records an unknown commit
  outcome during CreateTable and `TxnLockNotFound` during a statistics refresh.
  Passing SDK cases do not erase those server-side errors. No retry policy was
  broadened and unknown commit outcomes were not blindly replayed.
* Rust SDK `backup_restore::enable_point_in_time_recovery` fails because TiKV
  PITR is unsupported. `restored_table_has_all_items_when_first_active` fails
  because its snapshot exceeds the encoded 4 MiB backup limit.
* Main Python `TestSSESpecification::test_sse_enabled_round_trips` fails with
  `KeyError: 'SSEDescription'` when reading the table description. This is the
  only ordinary failure in that complete 1,081-case run.
* The external failures continue to include SSE, transaction validation/error
  shape, Streams behavior, size/nesting limits and other differences detailed
  in the earlier reports. These are failing cases, not a count of independent
  root causes or a fresh measurement against AWS.

## Scope not executed

This is a broad local run, not the full OS/backend/container CI matrix.
MongoDB physical cleanup, commit-outcome injection, backup and TTL fixtures
require the Docker replica-set/test-hook harness; Docker and MongoDB are not
available on this host. Their self-skips do not establish MongoDB support.
Container smoke tests and cross-platform packaging jobs likewise remain unrun.

The generic external runner cannot execute its documented Java/C++ suites:
this checkout has neither `external-suites.toml` nor `tests/external/`, and Maven
is unavailable. Its dry-run failure is recorded as `external-registry.log`.
The separately pinned Parity and Alternator suites above did execute.

`tests/cli/test-cli-comprehensive.sh` was not executed. It needs AWS CLI and
default PostgreSQL connection assumptions and includes a real AWS request;
it has not been adapted to this isolated local run. The current PostgreSQL
Python lifecycle/readiness/migration/queue tests did execute. Coverage
instrumentation, live AWS comparisons and production deployment are outside
this run.

## Reproduction

Use freshly built, separately named binaries and a disposable local PD/TiKV
cluster. The runner creates and cleans each API namespace; it must not be
pointed at a namespace containing user data. The principal commands are:

```sh
cargo test --workspace --locked
cargo test -p extenddb -p extenddb-app -p extenddb-server \
  --no-default-features --features tikv --locked
cargo test -p extenddb-storage-tikv --features client,test-support --locked
TIKV_PD_ENDPOINTS=127.0.0.1:2489 cargo test -p extenddb-storage-tikv \
  --features client,test-support --locked --no-fail-fast -- --ignored

python devtools/run-tikv-tests --binary /path/to/extenddb-tikv --no-build \
  --pd-endpoints 127.0.0.1:2489 -- tests --ignore=tests/python \
  --ignore=tests/test_cli_lifecycle.py \
  --ignore=tests/test_cli_container_readiness.py \
  --ignore=tests/test_cli_migrate_concurrency.py \
  --ignore=tests/test_cli_vector_catalog_migration.py \
  --ignore=tests/test_gsi_async_queue.py
python devtools/run-tikv-tests --binary /path/to/extenddb-tikv --no-build \
  --pd-endpoints 127.0.0.1:2489 -- tests/python
python devtools/run-tikv-tests --binary /path/to/extenddb-tikv --no-build \
  --pd-endpoints 127.0.0.1:2489 --command -- \
  cargo test --manifest-path tests/rust/Cargo.toml --locked -- --test-threads=4

# Set a base PostgreSQL URL without a database component, for a disposable
# cluster whose test role can create/drop databases. pgvector must be available.
cargo test -p extenddb-storage-postgres --test key_collation \
  --test vector_control_plane --locked --no-fail-fast -- --nocapture
```

For the five PostgreSQL Python modules excluded from the TiKV command, set
`EXTENDDB_BINARY` to the PostgreSQL binary,
`EXTENDDB_TEST_PG_CONNECTION_STRING` to the application role's base URL, and
`EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING` to the administrator's base URL, then
run pytest on those five files. Use the existing external-suite commands in the
two linked reports, retaining explicit throttling settings and JSON/JUnit
outputs. Tests and logs, not a passing subset, determine the run outcome.
