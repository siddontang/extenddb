// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Resumable maintenance with public single-step entry points for deterministic
//! tests. Each table/index batch has one commit; progress shares that commit.
//! Concurrent workers race safely on protected table metadata. Backfills read
//! current rows and writers maintain building indexes, preventing resurrection.
use crate::{
    TikvEngine,
    data::{Change, Mutation},
    index, kv,
    table::Table,
    ttl,
};
use extenddb_core::{expression::ExpressionMaps, types::*};
use extenddb_storage::{
    DataEngine, MetadataEngine, StreamCapture, StreamEngine, WorkerStore, error::StorageError,
};
use futures::future::BoxFuture;
const BATCH: u32 = 64;
impl TikvEngine {
    pub(crate) async fn item_page(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &Table,
        cursor: &[u8],
    ) -> Result<Vec<kv::Pair>, StorageError> {
        let p = self.item_prefix(&t.description.table_id);
        let start = if cursor.is_empty() {
            p.clone()
        } else {
            let mut k = cursor.to_vec();
            k.push(0);
            k
        };
        tx.scan(start, crate::codec::prefix_end(&p), BATCH, false)
            .await
            .map_err(kv::storage_error)
    }
    pub(crate) async fn backfill_ttl(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &mut Table,
    ) -> Result<(), StorageError> {
        let Some(cursor) = &t.ttl_cursor else {
            return Ok(());
        };
        let rows = self.item_page(tx, t, cursor).await?;
        for (_, v) in &rows {
            let item: Item = kv::decode(v)?;
            ttl::apply(self, tx, t, None, Some(&item)).await?;
        }
        t.ttl_cursor = if rows.len() < BATCH as usize {
            t.ttl_ready = true;
            None
        } else {
            rows.last().map(|(k, _)| k.clone())
        };
        Ok(())
    }
    /// Run one bounded table/index lifecycle batch per known table.
    pub async fn lifecycle_step(&self) -> Result<Vec<(String, &'static str)>, StorageError> {
        let tables = self
            .db
            .run(|tx| {
                let e = self.clone();
                Box::pin(async move { e.tables(tx).await })
            })
            .await?;
        let mut transitions = vec![];
        for table in tables {
            let batches = if table.description.table_status == TableStatus::Deleting {
                16
            } else {
                1
            };
            let id = table.description.table_id;
            for _ in 0..batches {
                let _admission = self.admit_account_write(&table.account).await;
                let result = self
                    .db
                    .run(|tx| {
                        let e = self.clone();
                        let id = id.clone();
                        Box::pin(async move {
                            let Some(mut t) = kv::get::<Table>(tx, e.key(&["table", &id])).await?
                            else {
                                return Ok(None);
                            };
                            if t.description.table_status == TableStatus::Deleting {
                                let rows = kv::prefix(tx, e.key(&["data", &id]), BATCH).await?;
                                for (k, _) in &rows {
                                    kv::delete(tx, k.clone()).await?;
                                }
                                if rows.len() == BATCH as usize {
                                    return Ok(None);
                                }
                                if let Some(spec) = &mut t.description.stream_specification {
                                    spec.stream_enabled = false;
                                    e.configure_stream(tx, &mut t).await?;
                                }
                                kv::delete(tx, e.key(&["tags", &t.description.table_arn])).await?;
                                kv::delete(
                                    tx,
                                    e.key(&["tables", &t.account, &t.description.table_name]),
                                )
                                .await?;
                                kv::delete(tx, e.key(&["table", &id])).await?;
                                let guard = e.key(&["account_tables", &t.account]);
                                let count: u64 = kv::get(tx, guard.clone()).await?.unwrap_or(0);
                                kv::put(tx, guard, &count.saturating_sub(1)).await?;
                                return Ok(Some((t.description.table_name, "deleted")));
                            }
                            let activated = t.description.table_status == TableStatus::Creating
                                && t.transition_at <= e.clock.now_ms();
                            if activated {
                                t.description.table_status = TableStatus::Active;
                            }
                            let mut changed = activated;
                            if t.description.table_status == TableStatus::Active {
                                for i in 0..t.indexes.len() {
                                    if let Some(cursor) = t.indexes[i].cursor.clone() {
                                        let rows = e.item_page(tx, &t, &cursor).await?;
                                        for (_, v) in &rows {
                                            let item: Item = kv::decode(v)?;
                                            let idx = &t.indexes[i];
                                            if extenddb_core::validation::validate_index_keys(
                                                &item,
                                                &[extenddb_core::validation::IndexKeyRef {
                                                    index_name: &idx.name,
                                                    key_schema: &idx.schema,
                                                }],
                                                &t.description.attribute_definitions,
                                            )
                                            .is_err()
                                            {
                                                continue;
                                            }
                                            index::apply(
                                                &e,
                                                tx,
                                                &id,
                                                &t.indexes[i],
                                                &t.description.key_schema,
                                                None,
                                                Some(&item),
                                            )
                                            .await?;
                                        }
                                        t.indexes[i].cursor = if rows.len() < BATCH as usize {
                                            None
                                        } else {
                                            rows.last().map(|(k, _)| k.clone())
                                        };
                                        changed = true;
                                    }
                                }
                                if t.ttl_cursor.is_some() {
                                    e.backfill_ttl(tx, &mut t).await?;
                                    changed = true;
                                }
                            }
                            if changed {
                                e.save_table(tx, &t).await?;
                            }
                            Ok(activated.then_some((t.description.table_name, "active")))
                        })
                    })
                    .await?;
                if let Some(t) = result {
                    transitions.push(t);
                    break;
                }
            }
        }
        // Tombstoned index generations cannot receive new writes, so prefix removal
        // needs no range predicate lock; the queue entry serializes competing workers.
        self.db
            .run(|tx| {
                let e = self.clone();
                Box::pin(async move {
                    for (k, v) in kv::prefix(tx, e.key(&["garbage"]), BATCH).await? {
                        let p: Vec<u8> = kv::decode(&v)?;
                        let rows = kv::prefix(tx, p, BATCH).await?;
                        for (key, _) in &rows {
                            kv::delete(tx, key.clone()).await?;
                        }
                        if rows.len() < BATCH as usize {
                            kv::delete(tx, k).await?;
                        }
                    }
                    Ok(())
                })
            })
            .await?;
        self.vector_build_step().await?;
        self.reap_staging().await?;
        Ok(transitions)
    }
    /// Delete at most 64 expired candidates per table, with an in-transaction
    /// recheck so a refreshed TTL or changed table generation is never deleted.
    pub async fn ttl_step(&self) -> Result<usize, StorageError> {
        let mut deleted = 0;
        for (account, name, attribute) in self.all_tables_with_ttl_index_ready().await? {
            for candidate in self
                .find_expired_items_indexed(&account, &name, &attribute, BATCH as usize)
                .await?
            {
                let count = self
                    .db
                    .run(|tx| {
                        let (e, account, name, attribute, candidate) = (
                            self.clone(),
                            account.clone(),
                            name.clone(),
                            attribute.clone(),
                            candidate.clone(),
                        );
                        Box::pin(async move {
                            let t = e.table_by_name(tx, &account, &name).await?;
                            if t.description.table_status != TableStatus::Active
                                || t.ttl_attribute.as_ref() != Some(&attribute)
                            {
                                return Ok(0);
                            }
                            let info = t.key_info();
                            let key = extract_key(&candidate, &info.base_key_schema);
                            let Some(item) = kv::get::<Item>(tx, e.item_key(&info, &key)?).await?
                            else {
                                return Ok(0);
                            };
                            if !ttl::expired(&item, &attribute, e.clock.now_ms() / 1000) {
                                return Ok(0);
                            }
                            let stream = t
                                .description
                                .stream_specification
                                .as_ref()
                                .filter(|s| s.stream_enabled)
                                .map(|s| StreamCapture {
                                    view_type: s
                                        .stream_view_type
                                        .unwrap_or(StreamViewType::KeysOnly),
                                    user_identity: Some(UserIdentity {
                                        identity_type: "Service".into(),
                                        principal_id: "dynamodb.amazonaws.com".into(),
                                    }),
                                    region: e.region.clone(),
                                });
                            e.mutate(
                                tx,
                                &Mutation {
                                    info,
                                    item: key,
                                    condition: None,
                                    maps: ExpressionMaps::default(),
                                    stream,
                                    change: Change::Delete,
                                    ccf: ReturnValuesOnConditionCheckFailure::None,
                                },
                            )
                            .await?;
                            Ok(1)
                        })
                    })
                    .await?;
                deleted += count;
            }
        }
        Ok(deleted)
    }
    /// Full scheduled maintenance tick. Public for integration tests and operators.
    pub async fn maintenance_step(&self) -> Result<(), StorageError> {
        self.lifecycle_step().await?;
        self.ttl_step().await?;
        self.cleanup_expired_idempotency_tokens(600).await?;
        self.cleanup_expired_stream_records(24).await?;
        Ok(())
    }
}
impl WorkerStore for TikvEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(self.lifecycle_step())
    }
}
