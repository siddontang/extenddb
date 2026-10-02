# Local TiKV backend

The TiKV backend is an experimental, standalone storage implementation. It
stores the catalog, IAM credentials, data, indexes and streams in TiKV; no
PostgreSQL service is involved. Start a dedicated PD/TiKV cluster before
initializing ExtendDB. The contract suite is exercised with PD/TiKV 8.5.5.

## Build and initialize

```sh
cargo build -p extenddb --no-default-features --features tikv
./target/debug/extenddb init --backend tikv \
  --tikv-pd-endpoints 127.0.0.1:2379 \
  --tikv-namespace extenddb_local --config extenddb-tikv.toml
./target/debug/extenddb serve --config extenddb-tikv.toml --foreground
./target/debug/extenddb verify --config extenddb-tikv.toml
```

Save the administrator password printed by `init`, or supply
`EXTENDDB_ADMIN_USER` and `EXTENDDB_ADMIN_PASSWORD` to bootstrap with known
credentials. Passwords should be supplied through the environment rather than
command arguments. HTTP TLS and SigV4 authentication remain mandatory, as with
the PostgreSQL backend. Use the normal management API to create IAM users and
keys, then use an AWS SDK against the server endpoint.

`init` reserves its namespace and refuses to overwrite an existing reservation.
Schema migration and seed operations are idempotent, but `init` itself is not an
overwrite-data operation. For an interrupted initialization, inspect the isolated
namespace before choosing to destroy and initialize it again.

## Configuration

The generated file includes this backend section:

```toml
[storage]
backend = "tikv"

[storage.tikv]
pd_endpoints = ["127.0.0.1:2379"]
namespace = "extenddb_local"
request_timeout_seconds = 10
```

A namespace must contain 1–128 ASCII letters, digits, underscores or hyphens.
It isolates deployments by key prefix. Selecting another namespace selects a
different deployment; it does not migrate data. Endpoint URLs cannot contain
user information, query strings or fragments.

For a secured TiKV cluster, supply all three TLS paths together:

```toml
[storage.tikv]
pd_endpoints = ["pd-1.example.net:2379", "pd-2.example.net:2379"]
namespace = "production_a"
request_timeout_seconds = 10
ca_path = "/etc/extenddb/tikv/ca.pem"
cert_path = "/etc/extenddb/tikv/client.pem"
key_path = "/etc/extenddb/tikv/client-key.pem"
```

For first initialization with cluster TLS, prepare the configuration file with
the storage section above and run `init --config ... --overwrite`. The
bootstrapper reads the existing backend section before generating the full
server config. Conflicting explicit TiKV CLI options are rejected. TLS paths
refer to cluster client credentials; they are separate from the HTTPS server's
certificate. Use absolute paths.

## Validation and operations

```sh
TIKV_PD_ENDPOINTS=127.0.0.1:2379 cargo test -p extenddb-storage-tikv \
  --features client,test-support -- --include-ignored

# Uses a random namespace and temporary server config, and cleans up afterward.
devtools/run-tikv-tests --pd-endpoints 127.0.0.1:2379

# Select additional existing SDK suites.
devtools/run-tikv-tests --no-build --pd-endpoints 127.0.0.1:2379 -- \
  tests/test_concurrency.py tests/test_auth_integration.py
```

The SDK runner requires Python 3.10+ and `pytest`, `boto3`, `requests`. The real
Rust tests fail if their PD endpoint is not configured; ignored tests are not
counted as cluster validation. Offline unit/contract tests need no TiKV process.

`verify` checks backend connectivity and catalog counts. `catalog-check` is a
PostgreSQL physical-table diagnostic and explicitly refuses this backend.
`migrate` upgrades catalog schemas 1–3 to 4; stop all namespace writers before
upgrading, then restart with the new binary. Inline IAM accounts convert on their
next management edit. Mixed-version writers and direct downgrade are unsupported; there is no PostgreSQL-to-TiKV conversion.

Stop all server instances before removing a deployment:

```sh
./target/debug/extenddb destroy --config extenddb-tikv.toml --yes
```

This deletes only the configured ExtendDB namespace. It does not shut down PD or
TiKV or delete other namespaces. Destruction commits in batches and can resume
after failure; it is not a single atomic cluster operation.

## Limits before production use

Read the [backend limits](../crates/storage-tikv/README.md#scope-and-deployment-limits)
before planning a rollout. Vector search uses exact partition scans. Backups
stream from a GC-protected snapshot with atomic publication. IAM has no account
size cap; individual records are bounded at 4 MiB. GSIs are synchronous. Existing
PostgreSQL data needs an explicit migration plan.

TiKV's old MVCC versions require coordinated garbage collection. ExtendDB does
not advance the cluster-wide GC safepoint because it cannot account for other
clients' active transactions. An external GC controller, restore drills, and
failure/partition tests are prerequisites for sustained production operation.
Logical TTL and stream retention do not replace MVCC GC. The namespace prefix
also does not provide isolation from a client with direct cluster access.

## Enable and use PITR

With your normal signed AWS CLI credentials and `AWS_CA_BUNDLE` configured:

```sh
aws dynamodb update-continuous-backups --table-name Music \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
  --endpoint-url https://127.0.0.1:18443
aws dynamodb describe-continuous-backups --table-name Music \
  --endpoint-url https://127.0.0.1:18443
aws dynamodb restore-table-to-point-in-time --source-table-name Music \
  --target-table-name MusicRecovered --restore-date-time '<time inside reported window>' \
  --endpoint-url https://127.0.0.1:18443
```

Use `--use-latest-restorable-time` instead of `--restore-date-time` for a current
snapshot. Restore targets must be new table names. Explicit times have
millisecond precision. The source must still exist; deleted tables, source ARNs
and restore overrides are not yet supported. Restores run synchronously.

PITR keeps up to 35 days by renewing PD service safepoints. The retention worker
runs separately from data maintenance. Its lease lasts 24 hours; longer worker
outages can allow GC to shorten the window. Always inspect the reported earliest
and latest times. PD/GC errors refuse recovery rather than returning current or
partial data. Monitor retention-renewal errors and cluster disk usage: the
barriers affect the whole shared cluster. The external GC controller must honor
these service safepoints. A backup stored in the same cluster is not protection
against cluster loss. See the [PITR module contract](../crates/storage-tikv/README.md#point-in-time-recovery-catalog-schema-4)
for upgrade, cleanup and authorization details.
