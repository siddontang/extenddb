# Alternator coverage and TiKV TTL contracts

This extends [the first TiKV compatibility report](testing-tikv.md) with another
independent source of DynamoDB scenarios. It does not replace the earlier
results or establish production readiness. No test in this exercise contacts
AWS. External assertions are observations to investigate, not newly measured
AWS golden responses or values to copy into the implementation.

## Source and execution boundary

The source is the [Scylla Alternator API suite](https://github.com/scylladb/scylladb/blob/scylla-6.2.0/test/alternator/README.md),
release **scylla-6.2.0**, commit
`b8a9fd4e49e8923b3edffa53ffec3b6e46c50906`. This is a reproducible release
snapshot, not the latest upstream suite. Source stays in a separate checkout.
The adapter refuses another revision or modifications to tracked upstream
tests and their reporting helper. Assertions and upstream xfail markers remain
unchanged.

The 25 selected modules collect **736 cases**. Initial exploration ran 721 cases
and 15 TTL cases separately; the post-fix runs select all 736. They cover item/batch operations, numbers, nested
documents, expressions and legacy Expected, Query/Scan, pagination, ReturnValues,
GSI/LSI, tables, endpoints, backup, tags, limits, Streams and TTL. Cases overlap
with existing suites; this is not 736 distinct new product behaviors.

Nine modules are outside this selection: `authorization`, `cors`, `cql_rbac`,
`health`, `manual_requests`, `metrics`, `scylla`, `system_tables`, and `tracing`.
They need a separate audit for native Scylla services, unsigned/manual client
assumptions or non-DynamoDB APIs. Exclusion is not evidence of a pass. This
release does not add transaction API coverage.

`devtools/alternator_adapter.py` separates four responsibilities:

* **Transport:** require a loopback endpoint, temporary IAM credentials and CA;
  verify both SDK service endpoints; reject requests outside the selected
  scheme/host/port in botocore and requests, including redirects/new sessions.
* **Fixtures:** use IAM credentials (`--aws` selects the upstream credential
  path while the endpoint remains local); replace the root liveness probe with
  `/health`; disable Scylla REST service discovery. Pytest owns teardown.
* **Collection:** retain assertions and upstream markers; explicitly skip
  fixtures requiring `scylla_only`, `rest_api`, `has_tablets`, `cql`,
  `waits_for_expiration` or `check_pre_consistent_cluster_management`. The last
  two query Scylla system configuration and cannot run portably.
* **Execution:** one pytest process, 120-second per-test timeout including setup
  and teardown, upstream veryslow tests disabled. Workers are rejected because
  they do not inherit this plugin instance and its transport checks. `--suite`
  selects modules without modifying source or expectations.

The exploratory and first post-fix full runs used a 30-second timeout. This is
shorter than multiple consecutive SDK table waiters: the installed botocore
model polls both TableExists and TableNotExists every 20 seconds. Table lifecycle
cases therefore receive a separate 120-second rerun. The final adapter uses that
larger default; original results remain recorded separately.

The existing lifecycle runner owns startup, configuration, credentials and
cleanup. Failure exit codes are preserved. Cleanup affects only the run's own
namespace/database. The adapter does not start databases or change server
configuration.

## Stack overflow found and fixed

Both original backend binaries aborted during
`test_limits::test_deeply_nested_expression_5`. The macOS crash report showed
hundreds of alternating `parse_function_call` / `parse_operand` frames. Condition
parsing counted groups and NOT but failed to count nested function arguments.
This shared parser defect was independent of TiKV. Subsequent connection errors
from those interrupted runs are not independent compatibility failures.

Commit `94e7bf8` includes function calls in the existing condition recursion
budget. Update parsing checks parenthesis/function depth iteratively before
recursive descent, and the engine passes its configured `max_expression_depth`.
Independent core/engine regressions cover closed and unfinished deep input,
boundary depths, sibling calls, combined NOT/function nesting and configured
limits. The original external crash case now passes and the server stays alive.
The workspace passes 1,241 tests with four ignored; affected crates pass strict
Clippy. The adapter now stops on a failed health probe so a crash cannot produce
a long cascade of meaningless connection failures.

## Measured results (2026-10-02 UTC)

| Run | Passed | Failed | Skipped | XFAIL | XPASS | Setup errors |
|---|---:|---:|---:|---:|---:|---:|
| TiKV, all 736, 30-second budget | 553 | 102 | 26 | 14 | 38 | 3 |
| SQLite, all 736, 30-second budget | 557 | 98 | 26 | 21 | 31 | 3 |
| TiKV, table module, 120-second rerun | 22 | 4 | 4 | 0 | 2 | 0 |
| SQLite, table module, 120-second rerun | 22 | 4 | 4 | 0 | 2 | 0 |
| TiKV, stream pagination, 120-second rerun | 1 | 0 | 0 | 0 | 0 | 0 |
| SQLite, stream pagination, 120-second rerun | 1 | 0 | 0 | 0 | 0 | 0 |

Upstream non-strict XPASS cases are reported separately from ordinary passes.
XFAIL and skipped cases do not establish support. The full runs have **96
failing case names in common**. This is a count of cases, not independent bugs.

The three setup errors in each full run are the concurrent table tests' Scylla
configuration fixture. The adapter now marks them explicitly unsupported; the
32-case table reruns demonstrate this change. Four TiKV and one SQLite table
failures were 30-second timeouts; **all five pass** with the larger budget.
The remaining four table failures are unchanged in both backends. Original
full-run counts above are retained, not retroactively rewritten as passes.
The remaining stream pagination timeout passes on both backends with the
larger budget (36.49 seconds on TiKV, 35.04 seconds on SQLite).

Merging by case name and taking the **latest measured result** from the full
run, table rerun and stream pagination rerun leaves 736 cases per backend:
TiKV has 558 ordinary passes, 38 XPASS, **97 failures**, 29 skips and 14 XFAIL;
SQLite has 559 ordinary passes, 31 XPASS, **96 failures**, 29 skips and 21 XFAIL.
There are no remaining setup errors in that merged view. This is an explicitly
merged result, not a second complete suite run at the longer timeout, and its
failures include the endpoint-adapter limitation described below.

TiKV's three additional non-timeout failures are Streams validation/cursor
cases: `test_describe_nonexistent_stream`,
`test_get_shard_iterator_for_nonexistent_stream`, and
`test_list_streams_with_nonexistent_last_stream`. SQLite instead has two
additional failing cases for default continuous-backup status and LSI access
to nonprojected attributes. Different xfail/skip results remain visible; neither
backend is treated as the AWS oracle or as evidence about PostgreSQL.

Other observations worth investigating:

* TiKV's raw failures include 47 unexpectedly accepted requests and 38 error
  message/type mismatches; five timeouts and 12 other assertions/API failures
  make up the remainder. The timeout reruns are recorded separately.
* Both backends expose gaps in legacy Expected, condition/update validation,
  invalid GSI batch handling, index definitions, and tag constraints. Some
  batch cases observe a valid row present after a batch containing an invalid
  index key is rejected. These observations need independent contract/golden
  confirmation before changing protocol semantics.
* `describe_endpoints` is stopped by the origin fence when the upstream test
  attempts a different destination. This is a transport-boundary limitation,
  not evidence of an AWS call or a proven protocol defect.
* Two TTL failures remain in both backends. One concerns disabling TTL with a
  different attribute; TiKV currently exposes an InternalServerError, whereas
  SQLite accepts that operation. The other is error text. The five initial TTL
  setup errors came from a Scylla system table, and became explicit skips.

The per-module counts below are the unmodified **30-second full-run** results;
the table module also has three setup errors per backend, separate from its
failure column. A zero failure count may include skipped/xfail cases.

| Module | Cases | TiKV failures | SQLite failures |
|---|---:|---:|---:|
| item | 46 | 4 | 4 |
| batch | 30 | 2 | 2 |
| number | 12 | 2 | 2 |
| nested | 4 | 0 | 0 |
| condition_expression | 53 | 9 | 9 |
| expected | 33 | 10 | 10 |
| filter_expression | 57 | 6 | 6 |
| projection_expression | 15 | 2 | 2 |
| update_expression | 58 | 13 | 13 |
| key_condition_expression | 48 | 8 | 8 |
| key_conditions | 35 | 4 | 4 |
| query | 29 | 1 | 1 |
| query_filter | 37 | 4 | 4 |
| scan | 16 | 2 | 2 |
| returnvalues | 15 | 1 | 1 |
| gsi | 73 | 4 | 4 |
| lsi | 17 | 2 | 3 |
| table | 32 | 8 | 5 |
| describe_table | 13 | 0 | 0 |
| describe_endpoints | 1 | 1 | 1 |
| backup | 3 | 1 | 2 |
| tag | 22 | 9 | 9 |
| limits | 25 | 0 | 0 |
| streams | 47 | 7 | 4 |
| ttl | 15 | 2 | 2 |

The newly added adapter has 17 offline tests, all passing (18 including the
existing lifecycle-runner isolation check). TiKV's complete feature-enabled
storage suite passes 31 tests with five real-cluster tests ignored by default.
The new TTL target then passes all four tests with `--include-ignored`: three
memory-store cases plus the real TiKV wrapper exercising the same three
contracts. These overlapping results must not be added as unique coverage.

## Independent TTL contracts

[ttl_contract.rs](../crates/storage-tikv/tests/ttl_contract.rs) adds three
deterministic scenarios through public storage traits and an injected clock.
The same scenarios also run against real TiKV:

1. Expiration removes base/GSI/LSI entries and emits one service deletion record.
   Future, absent, nonnumeric and ancient TTL attributes preserve their rows and
   index entries. Repeated cleanup produces no duplicate deletion.
2. Extending or removing an indexed expiration preserves the item and indexes.
   Advancing the clock expires only the renewed item.
3. Disabling TTL prevents cleanup; enabling another attribute isolates old
   index generations. Only the current attribute causes a later deletion.

These are independently written storage invariants based on published
[TTL behavior](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)
and [timestamp requirements](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/time-to-live-ttl-before-you-start.html).
They do not assert AWS background timing or exact error strings, and do not
replace the external TTL cases requiring Scylla internals.

## Reproduce

Use a dedicated local PD/TiKV cluster. Environment: macOS arm64, Python 3.12.14,
PD/TiKV 8.5.5, published `tikv-client 0.4.0`. Initial exploration used `75ec99c`;
both post-fix binaries were built from the source committed as `94e7bf8`.
Both comparisons disable throttling and do
**not** establish throttling conformance.

```sh
python3.12 -m venv /tmp/extenddb-alternator-venv
/tmp/extenddb-alternator-venv/bin/pip install -r devtools/alternator-requirements.txt
git clone --depth 1 --branch scylla-6.2.0 --filter=blob:none --sparse \
  https://github.com/scylladb/scylladb /tmp/extenddb-alternator
git -C /tmp/extenddb-alternator sparse-checkout set test/alternator test/pylib
mkdir -p discussions/tikv-alternator

/tmp/extenddb-alternator-venv/bin/python devtools/run-tikv-tests \
  --pd-endpoints 127.0.0.1:2379 --throttling false --command -- \
  /tmp/extenddb-alternator-venv/bin/python "$PWD/devtools/alternator_adapter.py" \
  --checkout /tmp/extenddb-alternator -- \
  --junitxml="$PWD/discussions/tikv-alternator/all.xml"
```

Repeat with `--backend sqlite` for the reference. The runner builds unless
`--no-build --binary /absolute/path/to/extenddb` is specified. Pass `--suite ttl`
before the adapter's final `--` to run TTL only; repeat for other modules.
Arguments after that separator go to pytest, including `--collect-only`, `-k`
and `--junitxml`. Do not enable xdist. An explicit larger `--timeout` is a
separate investigation run, not a replacement for the original failure.

```sh
/tmp/extenddb-alternator-venv/bin/python -m pytest \
  tests/test_alternator_adapter.py tests/test_tikv_runner.py -q
cargo test -p extenddb-storage-tikv --features client,test-support
TIKV_PD_ENDPOINTS=127.0.0.1:2379 cargo test -p extenddb-storage-tikv \
  --features client,test-support --test ttl_contract -- --ignored
```

The adapter's 17 offline tests cover endpoint rejection, missing credentials,
same-origin forwarding, redirects, restoration after failure, health failures,
Scylla fixture selection and rejection of unguarded workers. They never execute
upstream source or require a running server. The existing lifecycle runner's
environment isolation test is also retained.
The documented dependency list was additionally installed into a fresh Python
environment, where two external item smoke cases passed without pytest-xdist.

Local logs and JUnit files remain in ignored `discussions/tikv-alternator/`.
The post-fix full runs are `tikv-final.xml` and `sqlite-final.xml`; targeted
reruns are `*-table-120.xml` and `*-stream-waiter-120.xml`. Initial `tikv.xml` /
`sqlite.xml` came from interrupted runs after process crashes and must not be
used as complete compatibility scores. `ttl-contract-final.log`, `workspace.log`
and `clippy-final.log` retain Rust validation evidence. Raw logs are not committed
because they contain local environment details. The report preserves useful
counts and reproduction steps without publishing temporary credentials.
