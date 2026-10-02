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
    pub(crate) write_gates: Arc<Vec<Arc<tokio::sync::Mutex<()>>>>,
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
            write_gates: Arc::new(
                (0..256)
                    .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                    .collect(),
            ),
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
    pub(crate) async fn setting<T: std::str::FromStr>(
        &self,
        tx: &mut dyn crate::kv::Transaction,
        name: &str,
        default: T,
    ) -> Result<T, extenddb_storage::error::StorageError> {
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

    /// Runtime settings permit fractional seconds (the SDK runner uses 0.05).
    /// Validate before conversion so malformed values cannot overflow deadlines.
    pub(crate) async fn control_plane_delay_ms(
        &self,
        tx: &mut dyn crate::kv::Transaction,
    ) -> Result<i64, extenddb_storage::error::StorageError> {
        let seconds = self
            .setting(tx, "control_plane_delay_seconds", 0.0_f64)
            .await?;
        std::time::Duration::try_from_secs_f64(seconds)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .filter(|d| self.clock.now_ms().checked_add(*d).is_some())
            .ok_or_else(|| {
                extenddb_storage::error::StorageError::Internal(
                    "Invalid setting control_plane_delay_seconds".into(),
                )
            })
    }
}

impl TikvEngine {
    /// Reclaim at most one page of expired documents. The scan cursor commits
    /// with deletions, survives restart, and wraps after reaching the end. This
    /// avoids unbounded cleanup transactions and starvation behind live records.
    pub(crate) async fn prune_step<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        space: &str,
        expired: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<u64, extenddb_storage::error::StorageError> {
        let prefix = self.key(&[space]);
        let cursor = self.key(&["maintenance_cursor", space]);
        let expired = Arc::new(expired);
        self.db
            .run(move |tx| {
                let prefix = prefix.clone();
                let cursor = cursor.clone();
                let expired = expired.clone();
                Box::pin(async move {
                    let start = crate::kv::get::<Vec<u8>>(tx, cursor.clone())
                        .await?
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| prefix.clone());
                    let rows = tx
                        .scan(start, crate::codec::prefix_end(&prefix), 256, false)
                        .await
                        .map_err(crate::kv::storage_error)?;
                    let mut count = 0;
                    for (k, v) in &rows {
                        if expired(&crate::kv::decode::<T>(v)?) {
                            crate::kv::delete(tx, k.clone()).await?;
                            count += 1;
                        }
                    }
                    let next = if rows.len() == 256 {
                        let mut next = rows.last().unwrap().0.clone();
                        next.push(0);
                        next
                    } else {
                        vec![]
                    };
                    crate::kv::put(tx, cursor, &next).await?;
                    Ok(count)
                })
            })
            .await
    }
}

impl TikvEngine {
    /// Fair per-process contention admission. Correctness still comes from TiKV
    /// conflict detection and schema fences; other processes need no shared RAM.
    /// Sorted slots avoid deadlocks for multi-item requests. Acquire before
    /// starting a transaction so queued requests do not hold stale snapshots.
    pub(crate) async fn admit_writes(
        &self,
        keys: impl IntoIterator<Item = Vec<u8>>,
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        let slots: std::collections::BTreeSet<_> = keys
            .into_iter()
            .map(|k| crate::query::stable_hash(&k) as usize % self.write_gates.len())
            .collect();
        let mut guards = Vec::with_capacity(slots.len());
        for slot in slots {
            guards.push(self.write_gates[slot].clone().lock_owned().await);
        }
        guards
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv;

    #[tokio::test]
    async fn retention_cursor_reaches_expired_records_after_a_live_page_and_wraps() {
        let store = Arc::new(kv::memory::MemoryStore::default());
        let engine = TikvEngine::new(store.clone(), "retention", "us-east-1").unwrap();
        engine
            .db
            .run(|tx| {
                let e = engine.clone();
                Box::pin(async move {
                    for i in 0..300 {
                        kv::put(tx, e.key(&["records", &format!("{i:04}")]), &(i < 256)).await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            engine
                .prune_step::<bool>("records", |live| !live)
                .await
                .unwrap(),
            0
        );
        // Restart with the same store: progress is durable, not process-local.
        let engine = TikvEngine::new(store, "retention", "us-east-1").unwrap();
        assert_eq!(
            engine
                .prune_step::<bool>("records", |live| !live)
                .await
                .unwrap(),
            44
        );
        assert_eq!(
            engine
                .prune_step::<bool>("records", |_| true)
                .await
                .unwrap(),
            256
        );
        assert_eq!(
            engine
                .prune_step::<bool>("records", |_| true)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn multi_item_admission_deduplicates_and_orders_slots() {
        let e = TikvEngine::new(
            Arc::new(kv::memory::MemoryStore::default()),
            "gates",
            "us-east-1",
        )
        .unwrap();
        let first = vec![b"one".to_vec(), b"two".to_vec(), b"one".to_vec()];
        let mut second = first.clone();
        second.reverse();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                async {
                    let _held = e.admit_writes(first).await;
                    tokio::task::yield_now().await;
                },
                async {
                    let _held = e.admit_writes(second).await;
                    tokio::task::yield_now().await;
                }
            );
        })
        .await
        .expect("reverse-order multi-item requests must not deadlock");
    }
}
