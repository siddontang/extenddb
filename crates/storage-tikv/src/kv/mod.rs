// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Injectable transactional KV boundary, independent of TiKV client types.
//!
//! Reads in a writing transaction protect existing AND absent keys. Scans
//! provide a stable snapshot but do not promise predicate locks: callers that
//! depend on absence of children must touch a common guard key on every child
//! mutation. Commits distinguish a confirmed conflict from an unknown result;
//! only confirmed conflicts may replay a closure. Closures must have no
//! external side effects and must recompute all decisions on every attempt.

use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub mod gc;
#[cfg(any(test, feature = "test-support"))]
pub mod memory;

/// One KV pair, ordered by its raw key bytes.
pub type Pair = (Vec<u8>, Vec<u8>);

/// Storage protocol failures. `UnknownCommit` must never be retried blindly.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("transaction conflict")]
    Conflict,
    #[error("commit outcome unknown: {0}")]
    UnknownCommit(String),
    #[error("KV transport: {0}")]
    Transport(String),
}

/// A transaction owns its snapshot and buffers mutations until commit.
pub trait Transaction: Send {
    /// Snapshot read without a commit dependency. Only use when another guard
    /// explicitly fences the decision (e.g. striped table-schema guards).
    fn get_snapshot(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>>;
    fn get(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>>;
    /// Scan `[start,end)`, in the requested direction; limit is always bounded.
    fn scan(
        &mut self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        reverse: bool,
    ) -> BoxFuture<'_, Result<Vec<Pair>, Error>>;
    /// Read-only range scan without accumulating commit dependencies. A caller
    /// that later writes must provide a separate fence for these observations.
    fn scan_snapshot(
        &mut self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        reverse: bool,
    ) -> BoxFuture<'_, Result<Vec<Pair>, Error>> {
        self.scan(start, end, limit, reverse)
    }
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> BoxFuture<'_, Result<(), Error>>;
    fn delete(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<(), Error>>;
    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>>;
    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>>;
}

/// Long-lived store; a fresh transaction is created on each retry.
pub trait Store: Send + Sync {
    fn begin(&self) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>>;
    /// Long-lived read-only snapshot, protected against concurrent MVCC GC.
    /// Non-GC reference stores can use their normal immutable snapshot.
    fn snapshot(&self) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>> {
        self.begin()
    }

    /// Current TSO (milliseconds in upper bits, 18 logical bits).
    fn timestamp(&self) -> BoxFuture<'_, Result<u64, Error>> {
        Box::pin(async { Err(Error::Transport("Historical snapshots unavailable".into())) })
    }
    fn snapshot_at(&self, _timestamp: u64) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>> {
        Box::pin(async { Err(Error::Transport("Historical snapshots unavailable".into())) })
    }
    /// Install/renew a named retention barrier; zero TTL removes it. Returns the
    /// minimum accepted service floor. This must never advance the GC safepoint.
    fn retain(&self, _id: String, _floor: u64, _ttl: i64) -> BoxFuture<'_, Result<u64, Error>> {
        Box::pin(async { Err(Error::Transport("History retention unavailable".into())) })
    }
    fn gc_floor(&self) -> BoxFuture<'_, Result<u64, Error>> {
        Box::pin(async { Err(Error::Transport("GC visibility unavailable".into())) })
    }
}

/// Per-request transaction runner. Transport-specific retries stay in the client.
#[derive(Clone)]
pub struct Database {
    store: Arc<dyn Store>,
    max_attempts: usize,
}

impl Database {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            max_attempts: 64,
        }
    }
    /// Override the bounded conflict budget (at least one attempt).
    pub fn with_max_attempts(mut self, attempts: usize) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Open one owned read-only snapshot for a streaming operation. Unlike `run`,
    /// this never retries with a newer timestamp; callers must not stage writes
    /// in this transaction. Dropping the real adapter rolls it back.
    pub async fn snapshot(&self) -> Result<Box<dyn Transaction>, StorageError> {
        self.store.snapshot().await.map_err(storage_error)
    }

    pub async fn timestamp(&self) -> Result<u64, StorageError> {
        self.store.timestamp().await.map_err(storage_error)
    }
    pub async fn snapshot_at(&self, ts: u64) -> Result<Box<dyn Transaction>, StorageError> {
        self.store.snapshot_at(ts).await.map_err(storage_error)
    }
    pub async fn retain(&self, id: String, floor: u64, ttl: i64) -> Result<u64, StorageError> {
        self.store
            .retain(id, floor, ttl)
            .await
            .map_err(storage_error)
    }
    pub async fn gc_floor(&self) -> Result<u64, StorageError> {
        self.store.gc_floor().await.map_err(storage_error)
    }

    /// Execute a replayable unit of work and commit it atomically.
    pub async fn run<T, F>(&self, mut body: F) -> Result<T, StorageError>
    where
        T: Send,
        F: for<'a> FnMut(&'a mut dyn Transaction) -> BoxFuture<'a, Result<T, StorageError>> + Send,
    {
        for attempt in 0..self.max_attempts {
            let mut tx = self.store.begin().await.map_err(storage_error)?;
            let result = body(tx.as_mut()).await;
            match result {
                Ok(value) => match tx.commit().await {
                    Ok(()) => return Ok(value),
                    Err(Error::Conflict) => {
                        tx.rollback().await.map_err(storage_error)?;
                    }
                    Err(e) => {
                        // A failed commit may already have committed. Returning the
                        // error preserves that uncertainty instead of duplicating work.
                        let _ = tx.rollback().await;
                        return Err(storage_error(e));
                    }
                },
                Err(StorageError::TransactionConflict(_)) => {
                    tx.rollback().await.map_err(storage_error)?;
                }
                Err(e) => {
                    tx.rollback().await.map_err(storage_error)?;
                    return Err(e);
                }
            }
            if attempt + 1 < self.max_attempts {
                let ceiling = 1u64 << attempt.min(8);
                let delay = rand::random::<u64>() % (ceiling + 1);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }
        Err(StorageError::TransactionConflict(
            "TiKV conflict retry budget exhausted".into(),
        ))
    }
}

pub fn storage_error(e: Error) -> StorageError {
    match e {
        Error::Conflict => StorageError::TransactionConflict("TiKV write conflict".into()),
        Error::UnknownCommit(s) => {
            StorageError::Internal(format!("TiKV commit outcome unknown: {s}"))
        }
        Error::Transport(s) => StorageError::Transient(s),
    }
}

/// Decode a document, reporting corrupt data rather than substituting defaults.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    serde_json::from_slice(bytes)
        .map_err(|e| StorageError::Internal(format!("Corrupt TiKV document: {e}")))
}
pub async fn get<T: DeserializeOwned>(
    tx: &mut dyn Transaction,
    key: Vec<u8>,
) -> Result<Option<T>, StorageError> {
    tx.get(key)
        .await
        .map_err(storage_error)?
        .map(|b| decode(&b))
        .transpose()
}
pub async fn put<T: Serialize + Sync>(
    tx: &mut dyn Transaction,
    key: Vec<u8>,
    value: &T,
) -> Result<(), StorageError> {
    let bytes = serde_json::to_vec(value).map_err(|e| StorageError::Internal(e.to_string()))?;
    tx.put(key, bytes).await.map_err(storage_error)
}
pub async fn delete(tx: &mut dyn Transaction, key: Vec<u8>) -> Result<(), StorageError> {
    tx.delete(key).await.map_err(storage_error)
}

/// Read a bounded prefix page. Callers own pagination and lifecycle guards.
pub async fn prefix(
    tx: &mut dyn Transaction,
    prefix: Vec<u8>,
    limit: u32,
) -> Result<Vec<Pair>, StorageError> {
    let end = crate::codec::prefix_end(&prefix);
    tx.scan(prefix, end, limit, false)
        .await
        .map_err(storage_error)
}

/// Iterate a prefix in bounded network pages within one snapshot.
pub async fn all(tx: &mut dyn Transaction, prefix_key: Vec<u8>) -> Result<Vec<Pair>, StorageError> {
    let end = crate::codec::prefix_end(&prefix_key);
    let mut start = prefix_key;
    let mut out = vec![];
    loop {
        let page = tx
            .scan(start.clone(), end.clone(), 256, false)
            .await
            .map_err(storage_error)?;
        if page.is_empty() {
            break;
        }
        start = page.last().unwrap().0.clone();
        start.push(0);
        let done = page.len() < 256;
        out.extend(page);
        if done {
            break;
        }
    }
    Ok(out)
}

/// Read document data without a dependency; caller must supply its own fence.
pub async fn snapshot_get<T: DeserializeOwned>(
    tx: &mut dyn Transaction,
    key: Vec<u8>,
) -> Result<Option<T>, StorageError> {
    tx.get_snapshot(key)
        .await
        .map_err(storage_error)?
        .map(|b| decode(&b))
        .transpose()
}
