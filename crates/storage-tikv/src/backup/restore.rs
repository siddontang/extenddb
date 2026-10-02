// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk restore writes an unreachable table UUID, rebuilding every index in
//! bounded transactions. Only the final transaction installs the name, schema
//! guards and account table count. Failures/cancellation leave no partial table.
use super::*;
use crate::staging::Stage;
pub(crate) struct Writer {
    e: TikvEngine,
    stage: Stage,
    table: Table,
    generation: String,
    count: i64,
    size: i64,
}
impl Writer {
    pub async fn begin(
        e: &TikvEngine,
        source: &Table,
        name: &str,
        generation: String,
    ) -> Result<Self, StorageError> {
        let mut t = source.clone();
        let a = &t.account;
        t.description.table_id = uuid::Uuid::new_v4().to_string();
        t.description.table_name = name.into();
        t.description.table_arn = extenddb_storage::util::table_arn(&e.region, a, name);
        t.description.creation_date_time = e.clock.now_ms() as f64 / 1000.;
        t.description.table_status = TableStatus::Active;
        t.description.latest_stream_arn = None;
        t.description.latest_stream_label = None;
        t.description.stream_specification = None;
        t.description.deletion_protection_enabled = false;
        t.description.restore_summary = None;
        t.ttl_attribute = None;
        t.ttl_ready = false;
        t.ttl_cursor = None;
        t.ttl_generation.clear();
        for i in &mut t.indexes {
            i.id = uuid::Uuid::new_v4().to_string();
            i.cursor = None;
        }
        for v in &mut t.vectors {
            v.id = uuid::Uuid::new_v4().to_string();
            v.backfilling = None;
            v.complete = true;
            v.cursor.clear();
        }
        let stage = Stage::begin(e, e.key(&["data", &t.description.table_id])).await?;
        Ok(Self {
            e: e.clone(),
            stage,
            table: t,
            generation,
            count: 0,
            size: 0,
        })
    }
    pub async fn write(&mut self, items: &[Item]) -> Result<(), StorageError> {
        // One source item per transaction also bounds index write amplification.
        for item in items {
            let info = self.table.key_info();
            self.e
                .db
                .run(|tx| {
                    let e = self.e.clone();
                    let stage = self.stage.clone();
                    let t = self.table.clone();
                    let item = item.clone();
                    let info = info.clone();
                    Box::pin(async move {
                        stage.touch(tx).await?;
                        kv::put(tx, e.item_key(&info, &item)?, &item).await?;
                        for i in &t.indexes {
                            // Historical items can predate a current GSI schema.
                            // Preserve base data and apply the same poison-row
                            // omission used by online index backfill.
                            if extenddb_core::validation::validate_index_keys(
                                &item,
                                &[extenddb_core::validation::IndexKeyRef {
                                    index_name: &i.name,
                                    key_schema: &i.schema,
                                }],
                                &info.attribute_definitions,
                            )
                            .is_err()
                            {
                                continue;
                            }
                            index::apply(
                                &e,
                                tx,
                                &info.table_id,
                                i,
                                &info.key_schema,
                                None,
                                Some(&item),
                            )
                            .await?;
                        }
                        for v in &t.vectors {
                            crate::vector::apply(&e, tx, &t, v, None, Some(&item)).await?;
                        }
                        Ok(())
                    })
                })
                .await?;
            self.count += 1;
            self.size += item_size_bytes(item) as i64;
        }
        Ok(())
    }
    pub async fn finish(&self) -> Result<TableDescription, StorageError> {
        self.e
            .db
            .run(|tx| {
                let e = self.e.clone();
                let stage = self.stage.clone();
                let mut t = self.table.clone();
                let generation = self.generation.clone();
                let (count, size) = (self.count, self.size);
                Box::pin(async move {
                    let a = &t.account;
                    let owner =
                        kv::get::<crate::catalog::Account>(tx, e.key(&["account", a])).await?;
                    if owner.is_none_or(|o| o.generation != generation) {
                        return Err(StorageError::Validation(
                            "Restore account no longer exists".into(),
                        ));
                    }
                    let map = e.key(&["tables", a, &t.description.table_name]);
                    if kv::get::<String>(tx, map.clone()).await?.is_some() {
                        return Err(StorageError::TableAlreadyExists(t.description.table_name));
                    }
                    let guard = e.key(&["account_tables", a]);
                    let n: u64 = kv::get(tx, guard.clone()).await?.unwrap_or(0);
                    t.description.item_count = count;
                    t.description.table_size_bytes = size;
                    stage.publish(tx).await?;
                    kv::put(tx, guard, &(n + 1)).await?;
                    kv::put(tx, map, &t.description.table_id).await?;
                    e.save_table(tx, &t).await?;
                    Ok(t.describe())
                })
            })
            .await
    }
    pub async fn abandon(&self) {
        let _ = self.stage.abandon().await;
    }
}
