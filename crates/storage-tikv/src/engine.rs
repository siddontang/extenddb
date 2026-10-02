// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared dependencies, namespace ownership and injectable wall clock.
//!
//! Engine clones share the connection factory, never an active transaction.
//! Keys are versioned and rooted in an explicitly configured namespace so
//! independent ExtendDB deployments can coexist. Backend workers and tests use
//! the same public methods; time is injected to avoid sleeps in lifecycle tests.

use crate::{
    codec,
    kv::{Database, Store},
};
use std::sync::Arc;

/// Epoch milliseconds. Implementations must be thread-safe; never used for
/// transaction ordering (TiKV timestamps and conflict checks own that).
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        time::OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .div_euclid(1_000_000) as i64
    }
}

#[derive(Clone)]
pub struct TikvEngine {
    pub(crate) db: Database,
    pub(crate) namespace: Arc<str>,
    pub(crate) region: Arc<str>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) max_item_size: usize,
}
impl TikvEngine {
    /// Construct with an injected store. Connection/auth/bootstrap are separate.
    pub fn new(
        store: Arc<dyn Store>,
        namespace: &str,
        region: &str,
    ) -> Result<Self, extenddb_storage::error::StorageError> {
        crate::config::validate_namespace(namespace)?;
        Ok(Self {
            db: Database::new(store),
            namespace: namespace.into(),
            region: region.into(),
            clock: Arc::new(SystemClock),
            max_item_size: 400 * 1024,
        })
    }
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    pub fn with_max_item_size(mut self, size: usize) -> Self {
        self.max_item_size = size;
        self
    }
    /// Namespace-scoped key. Variable components are escaped by the codec.
    pub(crate) fn key(&self, parts: &[&str]) -> Vec<u8> {
        let mut k = codec::tuple(&["extenddb", &self.namespace, "v1"]);
        k.extend(codec::tuple(parts));
        k
    }
    pub(crate) async fn setting(
        &self,
        tx: &mut dyn crate::kv::Transaction,
        name: &str,
        default: i64,
    ) -> Result<i64, extenddb_storage::error::StorageError> {
        let value: Option<String> = crate::kv::get(tx, self.key(&["setting", name])).await?;
        value
            .map(|v| {
                v.parse().map_err(|_| {
                    extenddb_storage::error::StorageError::Internal(format!(
                        "Invalid setting {name}"
                    ))
                })
            })
            .unwrap_or(Ok(default))
    }
}
