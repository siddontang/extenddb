// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Thin client-rust transport adapter; all DynamoDB decisions live above it.
//!
//! Like TiDB Rust's `TikvTransactionSource`, one long-lived client opens fresh
//! transactions behind an injectable seam. We use optimistic two-phase commit
//! with explicit read locks (including missing keys). 1PC and async commit stay
//! disabled. Error classification is structural, never based on error strings.
//! Dropping an unfinished request schedules rollback on the Tokio runtime;
//! TiKV's lock TTL remains the final recovery mechanism after process death.

use super::gc::Barriers;
use super::{Error, Pair, Store, Transaction};
use futures::future::BoxFuture;
use std::sync::Arc;
use tikv_client::{BoundRange, Key, TimestampExt, TransactionClient};

pub struct TikvStore {
    client: TransactionClient,
    gc: Arc<super::gc::Pd>,
}
impl TikvStore {
    pub async fn connect(
        endpoints: Vec<String>,
        config: tikv_client::Config,
    ) -> Result<Self, Error> {
        let gc = super::gc::Pd::connect(endpoints.clone(), &config).await?;
        Ok(Self {
            gc,
            client: TransactionClient::new_with_config(endpoints, config)
                .await
                .map_err(transport)?,
        })
    }
}
impl Store for TikvStore {
    fn snapshot(&self) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>> {
        Box::pin(async move {
            let timestamp = self.client.current_timestamp().await.map_err(transport)?;
            let guard = super::gc::Guard::acquire(self.gc.clone(), timestamp.version()).await?;
            Ok(Box::new(Pinned {
                inner: self
                    .client
                    .snapshot(timestamp, tikv_client::TransactionOptions::new_optimistic()),
                guard,
            }) as Box<dyn Transaction>)
        })
    }

    fn timestamp(&self) -> BoxFuture<'_, Result<u64, Error>> {
        Box::pin(async move {
            Ok(self
                .client
                .current_timestamp()
                .await
                .map_err(transport)?
                .version())
        })
    }
    fn snapshot_at(&self, ts: u64) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>> {
        Box::pin(async move {
            let guard = super::gc::Guard::acquire(self.gc.clone(), ts).await?;
            Ok(Box::new(Pinned {
                inner: self.client.snapshot(
                    tikv_client::Timestamp::from_version(ts),
                    tikv_client::TransactionOptions::new_optimistic(),
                ),
                guard,
            }) as Box<dyn Transaction>)
        })
    }
    fn retain(&self, id: String, floor: u64, ttl: i64) -> BoxFuture<'_, Result<u64, Error>> {
        self.gc.pin(id, floor, ttl)
    }
    fn gc_floor(&self) -> BoxFuture<'_, Result<u64, Error>> {
        self.gc.safe_point()
    }
    fn begin(&self) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>> {
        Box::pin(async move {
            let tx = self.client.begin_optimistic().await.map_err(transport)?;
            Ok(Box::new(Tx {
                inner: Some(tx),
                reads: Default::default(),
                writes: false,
            }) as Box<dyn Transaction>)
        })
    }
}
struct Tx {
    inner: Option<tikv_client::Transaction>,
    reads: std::collections::BTreeSet<Vec<u8>>,
    writes: bool,
}
impl Drop for Tx {
    fn drop(&mut self) {
        if let Some(mut tx) = self.inner.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(async move {
                let _ = tx.rollback().await;
            });
        }
    }
}
fn transport(e: tikv_client::Error) -> Error {
    Error::Transport(e.to_string())
}
fn conflict(e: &tikv_client::Error) -> bool {
    match e {
        tikv_client::Error::KeyError(k) => k.conflict.is_some(),
        tikv_client::Error::MultipleKeyErrors(es) | tikv_client::Error::ExtractedErrors(es) => {
            !es.is_empty() && es.iter().all(conflict)
        }
        _ => false,
    }
}
impl Transaction for Tx {
    fn get_snapshot(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            self.inner
                .as_mut()
                .ok_or_else(|| Error::Transport("closed transaction".into()))?
                .get(key)
                .await
                .map_err(transport)
        })
    }

    fn get(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            let tx = self
                .inner
                .as_mut()
                .ok_or_else(|| Error::Transport("closed transaction".into()))?;
            self.reads.insert(key.clone());
            tx.get(key).await.map_err(transport)
        })
    }
    fn scan(
        &mut self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        reverse: bool,
    ) -> BoxFuture<'_, Result<Vec<Pair>, Error>> {
        Box::pin(async move {
            let rows = self.scan_snapshot(start, end, limit, reverse).await?;
            self.reads.extend(rows.iter().map(|(k, _)| k.clone()));
            Ok(rows)
        })
    }
    fn scan_snapshot(
        &mut self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        reverse: bool,
    ) -> BoxFuture<'_, Result<Vec<Pair>, Error>> {
        Box::pin(async move {
            let tx = self
                .inner
                .as_mut()
                .ok_or_else(|| Error::Transport("closed transaction".into()))?;
            let range: BoundRange = (Key::from(start), end.map(Key::from)).into();
            let rows: Vec<_> = if reverse {
                tx.scan_reverse(range, limit)
                    .await
                    .map_err(transport)?
                    .collect()
            } else {
                tx.scan(range, limit).await.map_err(transport)?.collect()
            };
            Ok(rows
                .into_iter()
                .map(|r| (Vec::<u8>::from(r.key().clone()), r.value().clone()))
                .collect())
        })
    }
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.writes = true;
            self.inner
                .as_mut()
                .ok_or_else(|| Error::Transport("closed transaction".into()))?
                .put(key, value)
                .await
                .map_err(transport)
        })
    }
    fn delete(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.writes = true;
            self.inner
                .as_mut()
                .ok_or_else(|| Error::Transport("closed transaction".into()))?
                .delete(key)
                .await
                .map_err(transport)
        })
    }
    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if let Some(mut tx) = self.inner.take() {
                tx.rollback().await.map_err(transport)?;
            }
            Ok(())
        })
    }
    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let tx = self
                .inner
                .as_mut()
                .ok_or_else(|| Error::Transport("closed transaction".into()))?;
            if self.writes {
                tx.lock_keys(self.reads.iter().cloned().collect::<Vec<_>>())
                    .await
                    .map_err(transport)?;
            }
            match tx.commit().await {
                Ok(_) => {
                    self.inner.take();
                    Ok(())
                }
                Err(e) if conflict(&e) => Err(Error::Conflict),
                Err(e) => Err(Error::UnknownCommit(e.to_string())),
            }
        })
    }
}

/// Read-only adapter: GC checks bracket each page, including the final empty
/// page, so a stalled reader cannot publish data after its protection expired.
struct Pinned {
    inner: tikv_client::Snapshot,
    guard: super::gc::Guard,
}
impl Transaction for Pinned {
    fn get_snapshot(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            self.guard.check().await?;
            let v = self.inner.get(key).await.map_err(transport)?;
            self.guard.check().await?;
            Ok(v)
        })
    }
    fn get(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        self.get_snapshot(key)
    }
    fn scan(
        &mut self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        reverse: bool,
    ) -> BoxFuture<'_, Result<Vec<Pair>, Error>> {
        Box::pin(async move {
            self.guard.check().await?;
            let range: BoundRange = (Key::from(start), end.map(Key::from)).into();
            let rows: Vec<_> = if reverse {
                self.inner
                    .scan_reverse(range, limit)
                    .await
                    .map_err(transport)?
                    .collect()
            } else {
                self.inner
                    .scan(range, limit)
                    .await
                    .map_err(transport)?
                    .collect()
            };
            self.guard.check().await?;
            Ok(rows
                .into_iter()
                .map(|r| (Vec::<u8>::from(r.key().clone()), r.value().clone()))
                .collect())
        })
    }
    fn put(&mut self, _: Vec<u8>, _: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Err(Error::Transport("cannot write a read-only snapshot".into())) })
    }
    fn delete(&mut self, _: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Err(Error::Transport("cannot write a read-only snapshot".into())) })
    }
    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { self.guard.check().await })
    }
    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classification_never_guesses_from_strings() {
        assert!(!conflict(&tikv_client::Error::StringError(
            "write conflict".into()
        )));
        assert!(!conflict(&tikv_client::Error::ExtractedErrors(vec![])));
        let key = tikv_client::ProtoKeyError {
            conflict: Some(Default::default()),
            ..Default::default()
        };
        assert!(conflict(&tikv_client::Error::KeyError(Box::new(key))));
    }
}
