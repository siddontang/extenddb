// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic MVCC reference implementation for contract and fault tests.
//!
//! Transactions read an immutable snapshot and validate every read/write key
//! at commit, including absent reads and deleted-key tombstones. Range scans
//! deliberately do not detect phantoms, matching the production contract;
//! higher layers must use lifecycle guards. This is a test double, not a
//! selectable production storage backend.

use super::{Error, Pair, Store, Transaction};
use futures::future::BoxFuture;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[derive(Default, Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<State>>,
}
#[derive(Default)]
struct State {
    clock: u64,
    data: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    conflicts: usize,
    unknown: usize,
}
struct Tx {
    owner: Arc<Mutex<State>>,
    snapshot: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    reads: BTreeSet<Vec<u8>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    closed: bool,
}
impl MemoryStore {
    /// Fail this many commits before applying any writes.
    pub fn inject_conflicts(&self, n: usize) {
        self.inner.lock().unwrap().conflicts = n;
    }
    /// Commit, then report an unknown outcome, modeling a lost response.
    pub fn inject_unknown_commits(&self, n: usize) {
        self.inner.lock().unwrap().unknown = n;
    }
}
impl Store for MemoryStore {
    fn begin(&self) -> BoxFuture<'_, Result<Box<dyn Transaction>, Error>> {
        Box::pin(async move {
            Ok(Box::new(Tx {
                owner: self.inner.clone(),
                snapshot: self.inner.lock().unwrap().data.clone(),
                reads: BTreeSet::new(),
                writes: BTreeMap::new(),
                closed: false,
            }) as Box<dyn Transaction>)
        })
    }
}
impl Tx {
    fn open(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::Transport("transaction already closed".into()))
        } else {
            Ok(())
        }
    }
}
impl Transaction for Tx {
    fn get(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            self.open()?;
            self.reads.insert(key.clone());
            Ok(self
                .writes
                .get(&key)
                .cloned()
                .unwrap_or_else(|| self.snapshot.get(&key).and_then(|x| x.1.clone())))
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
            self.open()?;
            let mut view: BTreeMap<_, _> = self
                .snapshot
                .iter()
                .filter_map(|(k, (_, v))| v.clone().map(|v| (k.clone(), v)))
                .collect();
            for (k, v) in &self.writes {
                if let Some(v) = v {
                    view.insert(k.clone(), v.clone());
                } else {
                    view.remove(k);
                }
            }
            let mut rows: Vec<_> = view
                .into_iter()
                .filter(|(k, _)| k >= &start && end.as_ref().is_none_or(|e| k < e))
                .collect();
            if reverse {
                rows.reverse();
            }
            rows.truncate(limit as usize);
            self.reads.extend(rows.iter().map(|(k, _)| k.clone()));
            Ok(rows)
        })
    }
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.open()?;
            self.writes.insert(key, Some(value));
            Ok(())
        })
    }
    fn delete(&mut self, key: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.open()?;
            self.writes.insert(key, None);
            Ok(())
        })
    }
    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.closed = true;
            self.writes.clear();
            Ok(())
        })
    }
    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.open()?;
            let mut state = self.owner.lock().unwrap();
            if state.conflicts > 0 {
                state.conflicts -= 1;
                return Err(Error::Conflict);
            }
            for key in self.reads.iter().chain(self.writes.keys()) {
                if self.snapshot.get(key).map(|x| x.0) != state.data.get(key).map(|x| x.0) {
                    return Err(Error::Conflict);
                }
            }
            state.clock += 1;
            let version = state.clock;
            for (k, v) in std::mem::take(&mut self.writes) {
                state.data.insert(k, (version, v));
            }
            self.closed = true;
            if state.unknown > 0 {
                state.unknown -= 1;
                return Err(Error::UnknownCommit("injected lost response".into()));
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{Database, get, put};
    #[tokio::test]
    async fn absent_read_and_write_skew_are_protected() {
        let s = MemoryStore::default();
        let mut a = s.begin().await.unwrap();
        let mut b = s.begin().await.unwrap();
        assert!(a.get(b"a".to_vec()).await.unwrap().is_none());
        assert!(b.get(b"b".to_vec()).await.unwrap().is_none());
        a.put(b"b".to_vec(), vec![1]).await.unwrap();
        b.put(b"a".to_vec(), vec![1]).await.unwrap();
        a.commit().await.unwrap();
        assert!(matches!(b.commit().await, Err(Error::Conflict)));
    }
    #[tokio::test]
    async fn retry_and_unknown_commit_have_different_semantics() {
        let s = Arc::new(MemoryStore::default());
        let db = Database::new(s.clone());
        s.inject_conflicts(2);
        db.run(|tx| Box::pin(async move { put(tx, b"n".to_vec(), &1u64).await }))
            .await
            .unwrap();
        s.inject_unknown_commits(1);
        assert!(
            db.run(|tx| Box::pin(async move {
                let n: u64 = get(tx, b"n".to_vec()).await?.unwrap();
                put(tx, b"n".to_vec(), &(n + 1)).await
            }))
            .await
            .is_err()
        );
        assert_eq!(
            db.run(|tx| Box::pin(get::<u64>(tx, b"n".to_vec())))
                .await
                .unwrap(),
            Some(2)
        );
    }
    #[tokio::test]
    async fn rollback_snapshot_and_reverse_bounds() {
        let s = MemoryStore::default();
        let mut a = s.begin().await.unwrap();
        let mut b = s.begin().await.unwrap();
        for k in [1, 2, 3] {
            a.put(vec![k], vec![k]).await.unwrap();
        }
        a.commit().await.unwrap();
        assert_eq!(b.get(vec![2]).await.unwrap(), None);
        b.rollback().await.unwrap();
        let mut c = s.begin().await.unwrap();
        c.delete(vec![2]).await.unwrap();
        assert_eq!(
            c.scan(vec![1], Some(vec![3]), 10, true).await.unwrap(),
            vec![(vec![1], vec![1])]
        );
        c.rollback().await.unwrap();
        assert_eq!(
            s.begin().await.unwrap().get(vec![2]).await.unwrap(),
            Some(vec![2])
        );
    }
}
