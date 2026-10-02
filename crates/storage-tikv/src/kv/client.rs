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

use super::{Error, Pair, Store, Transaction};
use futures::future::BoxFuture;
use tikv_client::{BoundRange, Key, TransactionClient};

pub struct TikvStore {
    client: TransactionClient,
}
impl TikvStore {
    pub async fn connect(
        endpoints: Vec<String>,
        config: tikv_client::Config,
    ) -> Result<Self, Error> {
        Ok(Self {
            client: TransactionClient::new_with_config(endpoints, config)
                .await
                .map_err(transport)?,
        })
    }
}
impl Store for TikvStore {
    fn begin(&self) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>> {
        Box::pin(async move {
            let tx = self.client.begin_optimistic().await.map_err(transport)?;
            Ok(Box::new(Tx { inner: Some(tx) }) as Box<dyn Transaction>)
        })
    }
}
struct Tx {
    inner: Option<tikv_client::Transaction>,
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
    fn get(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            let tx = self
                .inner
                .as_mut()
                .ok_or_else(|| Error::Transport("closed transaction".into()))?;
            tx.lock_keys([key.clone()]).await.map_err(transport)?;
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
            let keys: Vec<_> = rows.iter().map(|r| r.key().clone()).collect();
            tx.lock_keys(keys).await.map_err(transport)?;
            Ok(rows
                .into_iter()
                .map(|r| (Vec::<u8>::from(r.key().clone()), r.value().clone()))
                .collect())
        })
    }
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
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
