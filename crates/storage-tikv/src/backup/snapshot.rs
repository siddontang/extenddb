// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A single owned, read-only MVCC snapshot streamed in bounded pages.
//! This object never retries a scan with a newer timestamp. Transport/GC failure
//! aborts the export; already staged chunks remain unpublished and are reclaimed.
use crate::{TikvEngine, catalog::Account, codec, kv, table::Table};
use extenddb_core::types::{Item, TableStatus};
use extenddb_storage::error::StorageError;
pub(crate) struct Reader {
    tx: Box<dyn kv::Transaction>,
    pub table: Table,
    pub account_generation: String,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    done: bool,
}
impl Reader {
    pub async fn open(e: &TikvEngine, account: &str, name: &str) -> Result<Self, StorageError> {
        let mut tx = e.db.snapshot().await?;
        let table = e.table_by_name(tx.as_mut(), account, name).await?;
        if table.description.table_status != TableStatus::Active
            || table.indexes.iter().any(|i| i.cursor.is_some())
            || table.vectors.iter().any(|v| v.backfilling.is_some())
        {
            return Err(StorageError::TableNotActive(name.into()));
        }
        let a: Account = kv::get(tx.as_mut(), e.key(&["account", account]))
            .await?
            .ok_or_else(super::missing)?;
        let start = e.item_prefix(&table.description.table_id);
        let end = codec::prefix_end(&start);
        Ok(Self {
            tx,
            table,
            account_generation: a.generation,
            start,
            end,
            done: false,
        })
    }
    pub async fn next(&mut self) -> Result<Vec<Item>, StorageError> {
        if self.done {
            return Ok(vec![]);
        }
        let rows = self
            .tx
            .scan_snapshot(self.start.clone(), self.end.clone(), 8, false)
            .await
            .map_err(kv::storage_error)?;
        self.done = rows.len() < 8;
        if let Some((k, _)) = rows.last() {
            self.start = k.clone();
            self.start.push(0);
        }
        rows.into_iter().map(|(_, v)| kv::decode(&v)).collect()
    }
}
