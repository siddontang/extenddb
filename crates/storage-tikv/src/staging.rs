// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Leased, unpublished prefixes for bulk operations.
//!
//! A job is created before any staged writes. Every batch protects and refreshes
//! its lease; publication removes the job in the same transaction as the visible
//! manifest/name. Reaping removes the job and enqueues its prefix atomically, so
//! a delayed worker cannot write into a prefix already being reclaimed. Unknown
//! publication outcomes are safe: cleanup only acts on a still-present job.
use crate::{TikvEngine, kv};
use extenddb_storage::error::StorageError;
use serde::{Deserialize, Serialize};
const LEASE_MS: i64 = 300_000;
#[derive(Clone, Serialize, Deserialize)]
struct Job {
    prefix: Vec<u8>,
    expires: i64,
}
#[derive(Clone)]
pub(crate) struct Stage {
    engine: TikvEngine,
    id: String,
}
impl Stage {
    pub async fn begin(engine: &TikvEngine, prefix: Vec<u8>) -> Result<Self, StorageError> {
        let this = Self {
            engine: engine.clone(),
            id: uuid::Uuid::new_v4().to_string(),
        };
        engine
            .db
            .run(|tx| {
                let this = this.clone();
                let prefix = prefix.clone();
                Box::pin(async move {
                    kv::put(
                        tx,
                        this.key(),
                        &Job {
                            prefix,
                            expires: this.engine.clock.now_ms() + LEASE_MS,
                        },
                    )
                    .await
                })
            })
            .await?;
        Ok(this)
    }
    fn key(&self) -> Vec<u8> {
        self.engine.key(&["staging", &self.id])
    }
    pub async fn touch(&self, tx: &mut dyn kv::Transaction) -> Result<(), StorageError> {
        let mut job: Job = kv::get(tx, self.key()).await?.ok_or_else(|| {
            StorageError::Validation("Bulk operation lease expired; retry the request".into())
        })?;
        if job.expires <= self.engine.clock.now_ms() {
            return Err(StorageError::Validation(
                "Bulk operation lease expired; retry the request".into(),
            ));
        }
        job.expires = self.engine.clock.now_ms() + LEASE_MS;
        kv::put(tx, self.key(), &job).await
    }
    pub async fn publish(&self, tx: &mut dyn kv::Transaction) -> Result<(), StorageError> {
        self.touch(tx).await?;
        kv::delete(tx, self.key()).await
    }
    pub async fn abandon(&self) -> Result<(), StorageError> {
        self.engine
            .db
            .run(|tx| {
                let this = self.clone();
                Box::pin(async move {
                    if let Some(job) = kv::get::<Job>(tx, this.key()).await? {
                        kv::put(
                            tx,
                            this.engine.key(&["garbage", "staging", &this.id]),
                            &job.prefix,
                        )
                        .await?;
                        kv::delete(tx, this.key()).await?;
                    }
                    Ok(())
                })
            })
            .await
    }
}
impl TikvEngine {
    /// Reclaim a bounded page of expired jobs. Persistent cursor prevents live
    /// early jobs from starving later expired jobs across worker restarts.
    pub async fn reap_staging(&self) -> Result<(), StorageError> {
        self.db
            .run(|tx| {
                let e = self.clone();
                Box::pin(async move {
                    let p = e.key(&["staging"]);
                    let ck = e.key(&["maintenance_cursor", "staging"]);
                    let start = kv::get::<Vec<u8>>(tx, ck.clone())
                        .await?
                        .filter(|v| !v.is_empty())
                        .unwrap_or_else(|| p.clone());
                    let rows = tx
                        .scan(start, crate::codec::prefix_end(&p), 64, false)
                        .await
                        .map_err(kv::storage_error)?;
                    for (k, v) in &rows {
                        let j: Job = kv::decode(v)?;
                        if j.expires <= e.clock.now_ms() {
                            let mut garbage = e.key(&["garbage", "staging"]);
                            garbage.extend(k);
                            kv::put(tx, garbage, &j.prefix).await?;
                            kv::delete(tx, k.clone()).await?;
                        }
                    }
                    let next = if rows.len() == 64 {
                        let mut k = rows.last().unwrap().0.clone();
                        k.push(0);
                        k
                    } else {
                        vec![]
                    };
                    kv::put(tx, ck, &next).await
                })
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{engine::Clock, kv::memory::MemoryStore};
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    struct TestClock(AtomicI64);
    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    #[tokio::test]
    async fn expired_jobs_fence_writers_and_unknown_publication_is_not_reaped() {
        let store = Arc::new(MemoryStore::default());
        let clock = Arc::new(TestClock(AtomicI64::new(0)));
        let e = TikvEngine::new(store.clone(), "stage", "us-east-1")
            .unwrap()
            .with_clock(clock.clone());
        let stage = Stage::begin(&e, e.key(&["temporary"])).await.unwrap();
        clock.0.store(LEASE_MS, Ordering::SeqCst);
        e.reap_staging().await.unwrap();
        assert!(
            e.db.run(|tx| {
                let s = stage.clone();
                Box::pin(async move { s.touch(tx).await })
            })
            .await
            .is_err()
        );
        let stage = Stage::begin(&e, e.key(&["published-data"])).await.unwrap();
        store.inject_unknown_commits(1);
        assert!(
            e.db.run(|tx| {
                let s = stage.clone();
                let e = e.clone();
                Box::pin(async move {
                    s.publish(tx).await?;
                    kv::put(tx, e.key(&["published-data", "row"]), &42).await
                })
            })
            .await
            .is_err()
        );
        stage.abandon().await.unwrap();
        clock.0.fetch_add(LEASE_MS, Ordering::SeqCst);
        e.reap_staging().await.unwrap();
        e.lifecycle_step().await.unwrap();
        e.db.run(|tx| {
            let e = e.clone();
            Box::pin(async move {
                assert_eq!(
                    kv::get::<i32>(tx, e.key(&["published-data", "row"])).await?,
                    Some(42)
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
