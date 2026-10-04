# Rust SDK integration tests

This standalone crate exercises the public DynamoDB API with the official AWS
Rust SDK. Run it through an isolated backend runner, for example:

```sh
devtools/run-tikv-tests --no-build --command -- \
  cargo test --manifest-path tests/rust/Cargo.toml --locked -- --test-threads=4
```

`test_base.rs` reads the endpoint, credentials, region and CA certificate from
the environment. `runtime_http.rs` keeps HTTP dispatch tasks on one persistent
Tokio runtime because the shared SDK client outlives individual `#[tokio::test]`
runtimes. Disabling idle connection reuse alone does not solve concurrent
HTTP/2 connection reuse. The wrapper preserves the SDK's TLS, timeout and retry
configuration and has a network-free lifecycle regression test.

`helpers::ts()` creates unique name suffixes across simultaneous tests; its
concurrency test needs no server. `wait_for_active` waits for both the table and
all GSIs, since an ACTIVE table may still have a backfilling index.

```sh
cargo test --manifest-path tests/rust/Cargo.toml runtime_http::
cargo test --manifest-path tests/rust/Cargo.toml helpers::tests::
```

The runner destroys only its random namespace on exit, including on test
failure. Some older vector tests return early after capability probing; an SDK
test reported as passed does not prove unsupported vector functionality works.
See `docs/testing-tikv.md` for the measured backend limitations.
