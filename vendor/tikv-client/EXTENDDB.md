# ExtendDB dependency patch

This directory contains the published Apache-2.0 `tikv-client` 0.4.0 source,
upstream commit `9d9b680fb9daaae21dfbe9c4e7bcc9e796ba59cf`. The original license,
manifest, generated protocol types and tests are retained. CI metadata, the
upstream toolchain override and registry extraction markers are omitted.
Trailing whitespace in upstream design notes and protocol source is normalized.

Production changes are confined to `src/transaction/lock.rs`:
`check_txn_status` accepts `MultipleKeyErrors` as well as `ExtractedErrors`.
The region retry plan produces the former, so previously the typed
`TxnNotFound` result never reached the existing lock-TTL recovery loop.
This fixes lock resolution at its source; it does not declare an unknown
application commit safe to replay. The original TTL checks and rollback
escalation remain intact.

References: [upstream issue 543](https://github.com/tikv/client-rust/issues/543),
[proposed upstream fix 544](https://github.com/tikv/client-rust/pull/544), and
the nested-error handling in TiDB Rust's `third_party/tikv-client-rs` lock
resolver. ExtendDB's regression exercises the real request-plan error wrapper,
both expired and live orphan locks, rather than only testing an enum match.

Remove the workspace patch after a released client incorporates the fix and
passes these regressions plus ExtendDB's real-cluster contracts. Do not edit
the Cargo registry cache: this vendored source makes builds reproducible.

The merged SDK run also exposed a live optimistic lock whose primary remained
absent after the short status-lookup retry budget. `resolve_locks` now returns
that unresolved lock to the outer request plan's existing bounded live-lock
retry loop. It does not declare the transaction committed or rolled back, does
not force rollback before TTL expiry, and does not change application commit
retry classification. A mock transport test keeps the primary absent through
the inner retry budget, verifies no rollback/resolve request, then publishes a
commit and checks the exact secondary resolution. This complements the expired
primary test above. The TiDB Rust client's retry-owner flow provided the
reference for retaining a request-level retry budget beyond a status lookup.
