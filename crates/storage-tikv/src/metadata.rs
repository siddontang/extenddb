// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL configuration, resource tags and table statistics.
//! Metadata updates fence writers through the table document. TTL scans return
//! candidates only; the maintenance worker rechecks their current values in the
//! deletion transaction. All entry points share the injectable engine boundary.
use crate::{TikvEngine, kv, table::Table, ttl};
use extenddb_core::types::*;
use extenddb_storage::{MetadataEngine, TtlTableInfo, error::StorageError};
use futures::future::BoxFuture;
impl MetadataEngine for TikvEngine {
    fn describe_ttl(
        &self,
        account: &str,
        name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                let name = name.clone();
                Box::pin(async move {
                    let t = e.table_by_name(tx, &account, &name).await?;
                    Ok(TimeToLiveDescription {
                        time_to_live_status: if t.ttl_attribute.is_some() {
                            TimeToLiveStatus::Enabled
                        } else {
                            TimeToLiveStatus::Disabled
                        },
                        attribute_name: t.ttl_attribute,
                    })
                })
            })
            .await
        })
    }
    fn update_ttl(
        &self,
        account: &str,
        name: &str,
        attribute: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        let name = name.to_owned();
        let attribute = attribute.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                let name = name.clone();
                let attribute = attribute.clone();
                Box::pin(async move {
                    let mut t = e.table_by_name(tx, &account, &name).await?;
                    if t.description.table_status != TableStatus::Active {
                        return Err(StorageError::TableNotActive(name));
                    }
                    if enabled && t.ttl_attribute.as_ref().is_some_and(|a| a != &attribute) {
                        return Err(StorageError::Validation(
                            "Disable the current TTL attribute before changing it".into(),
                        ));
                    }
                    if !enabled && t.ttl_attribute.as_ref().is_some_and(|a| a != &attribute) {
                        return Err(StorageError::Validation(
                            "TTL attribute does not match".into(),
                        ));
                    }
                    if enabled && t.ttl_attribute.as_ref() == Some(&attribute) {
                        return Ok(());
                    }
                    if t.ttl_attribute.is_some() {
                        let old = ttl::prefix(&e, &t);
                        kv::put(
                            tx,
                            e.key(&["garbage", &t.description.table_id, &t.ttl_generation]),
                            &old,
                        )
                        .await?;
                    }
                    t.ttl_attribute = enabled.then_some(attribute);
                    t.ttl_ready = false;
                    t.ttl_cursor = enabled.then(Vec::new);
                    t.ttl_generation = uuid::Uuid::new_v4().to_string();
                    e.save_table(tx, &t).await
                })
            })
            .await
        })
    }
    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let e = self.clone();
        let arn = arn.to_owned();
        let tags = tags.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let arn = arn.clone();
                let tags = tags.clone();
                Box::pin(async move {
                    e.table_for_arn(tx, &arn).await?;
                    let k = e.key(&["tags", &arn]);
                    let mut stored: Vec<Tag> = kv::get(tx, k.clone()).await?.unwrap_or_default();
                    for t in tags {
                        stored.retain(|s| s.key != t.key);
                        stored.push(t);
                    }
                    stored.sort_by(|a, b| a.key.cmp(&b.key));
                    extenddb_core::validation::tags::validate_tags(&stored)
                        .map_err(|error| StorageError::Validation(error.to_string()))?;
                    kv::put(tx, k, &stored).await
                })
            })
            .await
        })
    }
    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let e = self.clone();
        let arn = arn.to_owned();
        let tag_keys = tag_keys.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let arn = arn.clone();
                let tag_keys = tag_keys.clone();
                Box::pin(async move {
                    e.table_for_arn(tx, &arn).await?;
                    let k = e.key(&["tags", &arn]);
                    let mut stored: Vec<Tag> = kv::get(tx, k.clone()).await?.unwrap_or_default();
                    stored.retain(|t| !tag_keys.contains(&t.key));
                    kv::put(tx, k, &stored).await
                })
            })
            .await
        })
    }
    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let e = self.clone();
        let arn = arn.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let arn = arn.clone();
                Box::pin(async move {
                    e.table_for_arn(tx, &arn).await?;
                    Ok(kv::get(tx, e.key(&["tags", &arn]))
                        .await?
                        .unwrap_or_default())
                })
            })
            .await
        })
    }
    fn tables_with_ttl(
        &self,
        account: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                Box::pin(async move {
                    Ok(e.tables(tx)
                        .await?
                        .into_iter()
                        .filter(|t| {
                            t.account == account
                                && t.description.table_status == TableStatus::Active
                        })
                        .filter_map(|t| t.ttl_attribute.map(|a| (t.description.table_name, a)))
                        .collect())
                })
            })
            .await
        })
    }
    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        let e = self.clone();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                Box::pin(async move {
                    Ok(e.tables(tx)
                        .await?
                        .into_iter()
                        .filter(|t| t.description.table_status == TableStatus::Active)
                        .filter_map(|t| {
                            t.ttl_attribute
                                .map(|a| (t.account, t.description.table_name, a))
                        })
                        .collect())
                })
            })
            .await
        })
    }
    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        let e = self.clone();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                Box::pin(async move {
                    Ok(e.tables(tx)
                        .await?
                        .into_iter()
                        .filter(|t| {
                            t.description.table_status == TableStatus::Active && t.ttl_ready
                        })
                        .filter_map(|t| {
                            t.ttl_attribute
                                .map(|a| (t.account, t.description.table_name, a))
                        })
                        .collect())
                })
            })
            .await
        })
    }
    fn create_ttl_index(
        &self,
        account: &str,
        name: &str,
        attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        let name = name.to_owned();
        let attribute = attribute.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                let name = name.clone();
                let attribute = attribute.clone();
                Box::pin(async move {
                    let mut t = e.table_by_name(tx, &account, &name).await?;
                    if t.ttl_attribute.as_ref() != Some(&attribute) {
                        return Err(StorageError::Validation("TTL configuration changed".into()));
                    }
                    e.backfill_ttl(tx, &mut t).await?;
                    e.save_table(tx, &t).await
                })
            })
            .await
        })
    }
    fn drop_ttl_index(
        &self,
        account: &str,
        name: &str,
        attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        let name = name.to_owned();
        let attribute = attribute.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                let name = name.clone();
                let attribute = attribute.clone();
                Box::pin(async move {
                    let mut t = e.table_by_name(tx, &account, &name).await?;
                    if t.ttl_attribute.as_ref().is_some_and(|a| a != &attribute) {
                        return Err(StorageError::Validation("TTL configuration changed".into()));
                    }
                    let p = ttl::prefix(&e, &t);
                    kv::put(
                        tx,
                        e.key(&["garbage", &t.description.table_id, &t.ttl_generation]),
                        &p,
                    )
                    .await?;
                    t.ttl_ready = false;
                    t.ttl_cursor = None;
                    e.save_table(tx, &t).await
                })
            })
            .await
        })
    }
    fn find_expired_items_indexed(
        &self,
        account: &str,
        name: &str,
        attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        let name = name.to_owned();
        let attribute = attribute.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                let name = name.clone();
                let attribute = attribute.clone();
                Box::pin(async move {
                    let t = e.table_by_name(tx, &account, &name).await?;
                    if t.ttl_attribute.as_ref() != Some(&attribute) || !t.ttl_ready {
                        return Ok(vec![]);
                    }
                    let now = e.clock.now_ms() / 1000;
                    let p = ttl::prefix(&e, &t);
                    let mut end = p.clone();
                    end.extend_from_slice(&(now.max(0) as u64).saturating_add(1).to_be_bytes());
                    let rows = tx
                        .scan(p, Some(end), limit.clamp(1, 256) as u32, false)
                        .await
                        .map_err(kv::storage_error)?;
                    let mut out = vec![];
                    for (k, v) in rows {
                        let key: Item = kv::decode(&v)?;
                        let item: Option<Item> =
                            kv::get(tx, e.item_key(&t.key_info(), &key)?).await?;
                        if let Some(item) = item
                            && ttl::entry(&e, &t, &item)?.as_ref() == Some(&k)
                        {
                            if ttl::expired(&item, &attribute, now) {
                                out.push(item);
                            } else if ttl::expiry(&item, &attribute)
                                .is_some_and(|at| at < now - 5 * 365 * 24 * 3600)
                            {
                                kv::delete(tx, k).await?;
                            }
                        } else {
                            kv::delete(tx, k).await?;
                        }
                    }
                    Ok(out)
                })
            })
            .await
        })
    }
    fn refresh_table_size(
        &self,
        account: &str,
        name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            let snapshot = e.capture_statistics(&account, &name).await?;
            e.publish_statistics(&account, &name, &snapshot).await
        })
    }

    fn list_active_table_names(
        &self,
        account: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let e = self.clone();
        let account = account.to_owned();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                Box::pin(async move {
                    Ok(e.tables(tx)
                        .await?
                        .into_iter()
                        .filter(|t| {
                            t.account == account
                                && t.description.table_status == TableStatus::Active
                        })
                        .map(|t| t.description.table_name)
                        .collect())
                })
            })
            .await
        })
    }
    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let e = self.clone();
        Box::pin(async move {
            e.db.run(|tx| {
                let e = e.clone();
                Box::pin(async move {
                    Ok(e.tables(tx)
                        .await?
                        .into_iter()
                        .filter(|t| t.description.table_status == TableStatus::Active)
                        .map(|t| (t.account, t.description.table_name))
                        .collect())
                })
            })
            .await
        })
    }
}
impl TikvEngine {
    pub(crate) async fn table_for_arn(
        &self,
        tx: &mut dyn kv::Transaction,
        arn: &str,
    ) -> Result<Table, StorageError> {
        let mut parts = arn.splitn(6, ':');
        let account = parts.nth(4).unwrap_or("");
        let resource = parts.next().unwrap_or("");
        let Some(name) = resource.strip_prefix("table/") else {
            return Err(StorageError::TableNotFound(arn.into()));
        };
        let t = self.table_by_name(tx, account, name).await?;
        if t.description.table_arn != arn {
            return Err(StorageError::TableNotFound(arn.into()));
        }
        Ok(t)
    }
}

/// An approximate point-in-time observation, fenced to one table incarnation
/// and schema. Publishing it never writes or locks the scanned item set.
struct StatisticsSnapshot {
    table_id: String,
    generation: u64,
    count: i64,
    bytes: i64,
}

impl TikvEngine {
    async fn capture_statistics(
        &self,
        account: &str,
        name: &str,
    ) -> Result<StatisticsSnapshot, StorageError> {
        self.db
            .run(|tx| {
                let (e, account, name) = (self.clone(), account.to_owned(), name.to_owned());
                Box::pin(async move {
                    let table = e.table_by_name(tx, &account, &name).await?;
                    let prefix = e.item_prefix(&table.description.table_id);
                    let end = crate::codec::prefix_end(&prefix);
                    let mut start = prefix;
                    let (mut count, mut bytes) = (0i64, 0i64);
                    loop {
                        let rows = tx
                            .scan(start.clone(), end.clone(), 256, false)
                            .await
                            .map_err(kv::storage_error)?;
                        for (_, value) in &rows {
                            count += 1;
                            bytes += item_size_bytes(&kv::decode::<Item>(value)?) as i64;
                        }
                        if rows.len() < 256 {
                            break;
                        }
                        start = rows.last().unwrap().0.clone();
                        start.push(0);
                    }
                    Ok(StatisticsSnapshot {
                        table_id: table.description.table_id,
                        generation: table.fence_generation,
                        count,
                        bytes,
                    })
                })
            })
            .await
    }

    async fn publish_statistics(
        &self,
        account: &str,
        name: &str,
        snapshot: &StatisticsSnapshot,
    ) -> Result<(), StorageError> {
        self.db
            .run(|tx| {
                let (e, account, name) = (self.clone(), account.to_owned(), name.to_owned());
                let (id, generation, count, bytes) = (
                    snapshot.table_id.clone(),
                    snapshot.generation,
                    snapshot.count,
                    snapshot.bytes,
                );
                Box::pin(async move {
                    let mut table = match e.table_by_name(tx, &account, &name).await {
                        Ok(table) => table,
                        Err(StorageError::TableNotFound(_)) => return Ok(()),
                        Err(error) => return Err(error),
                    };
                    if table.description.table_id != id
                        || table.fence_generation != generation
                        || table.description.table_status != TableStatus::Active
                    {
                        return Ok(());
                    }
                    table.description.item_count = count;
                    table.description.table_size_bytes = bytes;
                    e.save_table(tx, &table).await
                })
            })
            .await
    }
}

#[cfg(test)]
mod statistics_tests {
    use super::*;
    use crate::{catalog::TikvCatalog, kv::memory::MemoryStore};
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_storage::{DataEngine, TableEngine, management_store::ManagementStore};
    use std::sync::Arc;
    const ACCOUNT: &str = "111111111111";

    fn input() -> CreateTableInput {
        serde_json::from_value(
            serde_json::json!({"TableName":"statistics", "BillingMode":"PAY_PER_REQUEST",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}),
        )
        .unwrap()
    }
    async fn setup() -> (TikvEngine, Arc<MemoryStore>) {
        let store = Arc::new(MemoryStore::default());
        let engine = TikvEngine::new(store.clone(), "statistics_test", "us-east-1").unwrap();
        TikvCatalog::new(engine.clone())
            .create_account(ACCOUNT, "owner")
            .await
            .unwrap();
        engine.create_table(ACCOUNT, input()).await.unwrap();
        let info = engine.table_key_info(ACCOUNT, "statistics").await.unwrap();
        for key in ["a", "b", "c"] {
            let item = serde_json::from_value(serde_json::json!({"pk":{"S":key}})).unwrap();
            engine
                .put_item(&info, item, false, None, &ExpressionMaps::default(), None)
                .await
                .unwrap();
        }
        (engine, store)
    }
    #[tokio::test]
    async fn publishing_statistics_does_not_lock_the_scanned_item_set() {
        let (engine, store) = setup().await;
        let before = store.scan_write_commits();
        engine
            .refresh_table_size(ACCOUNT, "statistics")
            .await
            .unwrap();
        assert_eq!(store.scan_write_commits(), before);
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(description.item_count, 3);
        assert_eq!(description.table_size_bytes, 9);
    }
    #[tokio::test]
    async fn a_schema_change_invalidates_the_statistics_observation() {
        let (engine, _) = setup().await;
        let snapshot = engine
            .capture_statistics(ACCOUNT, "statistics")
            .await
            .unwrap();
        engine
            .update_ttl(ACCOUNT, "statistics", "expires", true)
            .await
            .unwrap();
        engine
            .publish_statistics(ACCOUNT, "statistics", &snapshot)
            .await
            .unwrap();
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(description.item_count, 0);
        engine
            .refresh_table_size(ACCOUNT, "statistics")
            .await
            .unwrap();
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(description.item_count, 3);
        assert_eq!(
            engine
                .describe_ttl(ACCOUNT, "statistics")
                .await
                .unwrap()
                .attribute_name
                .as_deref(),
            Some("expires")
        );
    }
    #[tokio::test]
    async fn a_statistics_snapshot_cannot_be_published_to_a_recreated_table() {
        let (engine, _) = setup().await;
        let snapshot = engine
            .capture_statistics(ACCOUNT, "statistics")
            .await
            .unwrap();
        engine
            .delete_table(
                ACCOUNT,
                DeleteTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        for _ in 0..4 {
            engine.lifecycle_step().await.unwrap();
        }
        engine.create_table(ACCOUNT, input()).await.unwrap();
        engine
            .publish_statistics(ACCOUNT, "statistics", &snapshot)
            .await
            .unwrap();
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_ne!(description.table_id, snapshot.table_id);
        assert_eq!(description.item_count, 0);
        assert_eq!(description.table_size_bytes, 0);
    }
}
