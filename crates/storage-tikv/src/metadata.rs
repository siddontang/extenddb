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
            e.db.run(|tx| {
                let e = e.clone();
                let account = account.clone();
                let name = name.clone();
                Box::pin(async move {
                    let mut t = e.table_by_name(tx, &account, &name).await?;
                    let rows = kv::all(tx, e.item_prefix(&t.description.table_id)).await?;
                    let mut size = 0;
                    for (_, v) in &rows {
                        size += item_size_bytes(&kv::decode::<Item>(v)?);
                    }
                    t.description.item_count = rows.len() as i64;
                    t.description.table_size_bytes = size as i64;
                    e.save_table(tx, &t).await
                })
            })
            .await
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
