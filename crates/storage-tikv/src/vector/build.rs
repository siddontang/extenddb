// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiKV primitives for the shared vector build driver.
//!
//! A persisted cursor and protected table document serialize competing workers.
//! Batches read current base images and protect them through commit, instead of
//! replaying an old snapshot over a newer synchronous index write. A restarted
//! worker resumes from disk; already committed rows are idempotent upserts.
use crate::{TikvEngine, kv, table::Table, vector};
use extenddb_storage::{
    error::StorageError,
    vector_lifecycle::{BackfillRow, BatchOutcome, VectorIndexBuild, classify_backfill_row},
};

#[derive(Clone)]
pub struct Build {
    engine: TikvEngine,
    table_id: String,
    index_id: String,
}
impl Build {
    pub fn new(engine: TikvEngine, table_id: String, index_id: String) -> Self {
        Self {
            engine,
            table_id,
            index_id,
        }
    }
    /// Resolve a build by account-scoped name without exposing key layout to callers.
    pub async fn load(
        engine: TikvEngine,
        account: &str,
        name: &str,
        index: &str,
    ) -> Result<Self, StorageError> {
        let (account, name, index) = (account.to_owned(), name.to_owned(), index.to_owned());
        let (table_id, index_id) = engine
            .db
            .run(|tx| {
                let (e, a, n, i) = (engine.clone(), account.clone(), name.clone(), index.clone());
                Box::pin(async move {
                    let t = e.table_by_name(tx, &a, &n).await?;
                    let v = t
                        .vectors
                        .iter()
                        .find(|v| v.spec.index_name == i)
                        .ok_or(StorageError::IndexNotFound(i))?;
                    Ok((t.description.table_id.clone(), v.id.clone()))
                })
            })
            .await?;
        Ok(Self::new(engine, table_id, index_id))
    }
    async fn table(&self, tx: &mut dyn kv::Transaction) -> Result<(Table, usize), StorageError> {
        let t: Table = kv::get(tx, self.engine.key(&["table", &self.table_id]))
            .await?
            .ok_or_else(|| StorageError::TableNotFound(self.table_id.clone()))?;
        if t.description.table_status != extenddb_core::types::TableStatus::Active {
            return Err(StorageError::TableNotActive(t.description.table_name));
        }
        let pos = t
            .vectors
            .iter()
            .position(|v| v.id == self.index_id)
            .ok_or_else(|| StorageError::IndexNotFound(self.index_id.clone()))?;
        Ok((t, pos))
    }
    async fn write_batch(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &mut Table,
        pos: usize,
        limit: i64,
    ) -> Result<BatchOutcome<Vec<u8>>, StorageError> {
        if t.vectors[pos].complete {
            return Ok(BatchOutcome {
                written: 0,
                skipped: 0,
                fetched: 0,
                cursor: None,
            });
        }
        let v = t.vectors[pos].clone();
        let prefix = self.engine.item_prefix(&self.table_id);
        let start = if v.cursor.is_empty() {
            prefix.clone()
        } else {
            let mut c = v.cursor.clone();
            c.push(0);
            c
        };
        let rows = tx
            .scan(
                start,
                crate::codec::prefix_end(&prefix),
                limit as u32,
                false,
            )
            .await
            .map_err(kv::storage_error)?;
        let (mut written, mut skipped) = (0, 0);
        for (key, bytes) in &rows {
            let text =
                std::str::from_utf8(bytes).map_err(|e| StorageError::Internal(e.to_string()))?;
            match classify_backfill_row(text, &v.meta(), &format!("{} bytes", key.len())) {
                BackfillRow::Index(item) => {
                    vector::apply(&self.engine, tx, t, &v, None, Some(&item)).await?;
                    written += 1;
                }
                BackfillRow::Poison => skipped += 1,
                BackfillRow::Omit => {}
            }
        }
        let cursor = rows.last().map(|(k, _)| k.clone());
        if let Some(c) = &cursor {
            t.vectors[pos].cursor = c.clone();
        }
        t.vectors[pos].complete = rows.len() < (limit as usize);
        t.vectors[pos].skipped += skipped;
        t.vectors[pos].next_batch_after = self
            .engine
            .delay_deadline(
                tx,
                extenddb_core::settings_keys::VECTOR_BACKFILL_BATCH_DELAY_MS,
                0,
            )
            .await?;
        self.engine.save_table(tx, t).await?;
        Ok(BatchOutcome {
            written,
            skipped,
            fetched: rows.len() as i64,
            cursor,
        })
    }

    /// Decide and commit one step using the same protected table read. Two
    /// workers cannot both consume the same deadline and rush through a phase.
    async fn advance(&self) -> Result<(), StorageError> {
        let this = self.clone();
        self.engine
            .db
            .run(|tx| {
                let this = this.clone();
                Box::pin(async move {
                    let (mut t, pos) = match this.table(tx).await {
                        Ok(found) => found,
                        Err(
                            StorageError::TableNotFound(_)
                            | StorageError::IndexNotFound(_)
                            | StorageError::TableNotActive(_),
                        ) => return Ok(()),
                        Err(error) => return Err(error),
                    };
                    let v = &t.vectors[pos];
                    let now = this.engine.clock.now_ms();
                    match v.backfilling {
                        Some(false) if now >= v.allocate_after => {
                            t.vectors[pos].backfilling = Some(true);
                            this.engine.save_table(tx, &t).await?;
                        }
                        Some(true) if v.complete && now >= v.activate_after => {
                            t.vectors[pos].backfilling = None;
                            this.engine.save_table(tx, &t).await?;
                        }
                        Some(true) if !v.complete && now >= v.next_batch_after => {
                            this.write_batch(tx, &mut t, pos, 64).await?;
                        }
                        _ => {}
                    }
                    Ok(())
                })
            })
            .await
    }
}
impl VectorIndexBuild for Build {
    type Cursor = Vec<u8>;
    async fn set_backfilling(&mut self) -> Result<(), StorageError> {
        let this = self.clone();
        self.engine
            .db
            .run(|tx| {
                let this = this.clone();
                Box::pin(async move {
                    let (mut t, pos) = this.table(tx).await?;
                    if t.vectors[pos].backfilling == Some(false) {
                        t.vectors[pos].backfilling = Some(true);
                        this.engine.save_table(tx, &t).await?;
                    }
                    Ok(())
                })
            })
            .await
    }
    async fn backfill_batch(
        &mut self,
        _cursor: Option<Vec<u8>>,
        limit: i64,
    ) -> Result<BatchOutcome<Vec<u8>>, StorageError> {
        if !(1..=64).contains(&limit) {
            return Err(StorageError::Validation(
                "TiKV vector backfill batch size must be 1..=64".into(),
            ));
        }
        let this = self.clone();
        this.engine
            .db
            .run(|tx| {
                let this = this.clone();
                Box::pin(async move {
                    let (mut t, pos) = this.table(tx).await?;
                    this.write_batch(tx, &mut t, pos, limit).await
                })
            })
            .await
    }
    async fn mark_active(&mut self, _skipped: usize) -> Result<(), StorageError> {
        let this = self.clone();
        this.engine
            .db
            .run(|tx| {
                let this = this.clone();
                Box::pin(async move {
                    let (mut t, pos) = this.table(tx).await?;
                    if !t.vectors[pos].complete {
                        return Err(StorageError::Internal(
                            "Cannot publish incomplete vector index".into(),
                        ));
                    }
                    t.vectors[pos].backfilling = None;
                    this.engine.save_table(tx, &t).await
                })
            })
            .await
    }
    async fn reset_data_table(&mut self) -> Result<(), StorageError> {
        let this = self.clone();
        let id = this
            .engine
            .db
            .run(|tx| {
                let this = this.clone();
                Box::pin(async move {
                    let (mut t, pos) = this.table(tx).await?;
                    let old = t.vectors[pos].id.clone();
                    let v = &mut t.vectors[pos];
                    v.id = uuid::Uuid::new_v4().to_string();
                    v.cursor.clear();
                    v.complete = false;
                    v.skipped = 0;
                    v.backfilling = Some(true);
                    v.next_batch_after = 0;
                    v.activate_after = 0;
                    let id = v.id.clone();
                    kv::put(
                        tx,
                        this.engine.key(&["garbage", &this.table_id, &old]),
                        &vector::prefix(&this.engine, &this.table_id, &old),
                    )
                    .await?;
                    this.engine.save_table(tx, &t).await?;
                    Ok(id)
                })
            })
            .await?;
        self.index_id = id;
        Ok(())
    }
    fn notify_active(&mut self) {} // live writes already maintain the index atomically
}
impl TikvEngine {
    /// Advance each unfinished index by one persisted phase or bounded batch.
    /// Allocation, backfill, and publication occur on separate worker steps, so
    /// cancellation remains possible and large builds do not monopolize TTL or
    /// table maintenance. Explicit Build primitives remain usable in fault tests.
    pub async fn vector_build_step(&self) -> Result<(), StorageError> {
        let tables = self
            .db
            .run(|tx| {
                let e = self.clone();
                Box::pin(async move { e.tables(tx).await })
            })
            .await?;
        for t in tables {
            if t.description.table_status != extenddb_core::types::TableStatus::Active {
                continue;
            }
            for v in t.vectors.into_iter().filter(|v| v.backfilling.is_some()) {
                Build::new(self.clone(), t.description.table_id.clone(), v.id)
                    .advance()
                    .await?;
            }
        }
        Ok(())
    }
}
