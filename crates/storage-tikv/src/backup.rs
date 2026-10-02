// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Account-scoped streaming snapshots and atomic publication.
//!
//! One MVCC snapshot feeds immutable per-item chunks. A small manifest becomes
//! visible only when every chunk is durable. Restores rebuild an unreachable
//! generation and atomically publish it. Leased staging prefixes make failures,
//! cancellation and unknown commit outcomes reclaimable without deleting a
//! successfully published result. Legacy inline backups remain readable.
mod restore;
mod snapshot;
use crate::{TikvEngine, index, kv, table::Table};
use extenddb_core::types::*;
use extenddb_storage::{BackupEngine, error::StorageError};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Backup {
    #[serde(default)]
    account_generation: String,
    table: Table,
    details: BackupDetails,
    #[serde(default)]
    items: Vec<Item>,
    #[serde(default)]
    data_id: Option<String>,
    #[serde(default)]
    item_count: u64,
}
impl Backup {
    fn count(&self) -> u64 {
        if self.data_id.is_some() {
            self.item_count
        } else {
            self.items.len() as u64
        }
    }

    fn description(&self) -> BackupDescription {
        let d = &self.table.description;
        BackupDescription {
            backup_details: self.details.clone(),
            source_table_details: SourceTableDetails {
                table_name: d.table_name.clone(),
                table_id: d.table_id.clone(),
                table_arn: d.table_arn.clone(),
                key_schema: d.key_schema.clone(),
                item_count: self.count() as i64,
                table_size_bytes: self.details.backup_size_bytes,
                billing_mode: d.billing_mode_summary.as_ref().map(|b| {
                    match b.billing_mode {
                        BillingMode::PayPerRequest => "PAY_PER_REQUEST",
                        BillingMode::Provisioned => "PROVISIONED",
                    }
                    .into()
                }),
                table_creation_date_time: d.creation_date_time,
            },
        }
    }
    fn summary(&self) -> BackupSummary {
        let d = &self.details;
        BackupSummary {
            backup_arn: d.backup_arn.clone(),
            backup_name: d.backup_name.clone(),
            table_name: self.table.description.table_name.clone(),
            table_arn: self.table.description.table_arn.clone(),
            backup_status: d.backup_status.clone(),
            backup_type: d.backup_type.clone(),
            backup_size_bytes: d.backup_size_bytes,
            backup_creation_date_time: d.backup_creation_date_time,
        }
    }
}
fn missing() -> StorageError {
    StorageError::Validation("Backup not found".into())
}
impl TikvEngine {
    async fn backup(
        &self,
        tx: &mut dyn kv::Transaction,
        account: &str,
        arn: &str,
    ) -> Result<Backup, StorageError> {
        let b: Backup = kv::get(tx, self.key(&["backup", account, arn]))
            .await?
            .ok_or_else(missing)?;
        let owner = kv::get::<crate::catalog::Account>(tx, self.key(&["account", account])).await?;
        if b.table.account != account || owner.is_none_or(|a| a.generation != b.account_generation)
        {
            return Err(missing());
        }
        Ok(b)
    }
}
impl BackupEngine for TikvEngine {
    fn create_backup(
        &self,
        account: &str,
        name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let (e, a, n, b) = (
            self.clone(),
            account.to_owned(),
            name.to_owned(),
            backup_name.to_owned(),
        );
        Box::pin(async move {
            let mut reader = snapshot::Reader::open(&e, &a, &n).await?;
            let id = uuid::Uuid::new_v4().to_string();
            let stage = crate::staging::Stage::begin(&e, e.key(&["backup_data", &id])).await?;
            let result = async {
                let mut count = 0u64;
                let mut size = 0i64;
                loop {
                    let items = reader.next().await?;
                    if items.is_empty() {
                        break;
                    }
                    for item in items {
                        let key = e.key(&["backup_data", &id, &format!("{count:020}")]);
                        e.db.run(|tx| {
                            let stage = stage.clone();
                            let key = key.clone();
                            let item = item.clone();
                            Box::pin(async move {
                                stage.touch(tx).await?;
                                kv::put(tx, key, &item).await
                            })
                        })
                        .await?;
                        count += 1;
                        size += item_size_bytes(&item) as i64;
                    }
                }
                let details = BackupDetails {
                    backup_arn: format!("{}/backup/{}", reader.table.description.table_arn, id),
                    backup_name: b,
                    backup_status: "AVAILABLE".into(),
                    backup_type: "USER".into(),
                    backup_size_bytes: size,
                    backup_creation_date_time: e.clock.now_ms() as f64 / 1000.,
                };
                let backup = Backup {
                    account_generation: reader.account_generation,
                    table: reader.table,
                    details: details.clone(),
                    items: vec![],
                    data_id: Some(id),
                    item_count: count,
                };
                e.db.run(|tx| {
                    let e = e.clone();
                    let a = a.clone();
                    let stage = stage.clone();
                    let backup = backup.clone();
                    Box::pin(async move {
                        let owner =
                            kv::get::<crate::catalog::Account>(tx, e.key(&["account", &a])).await?;
                        if owner.is_none_or(|o| o.generation != backup.account_generation) {
                            return Err(missing());
                        }
                        stage.publish(tx).await?;
                        kv::put(
                            tx,
                            e.key(&["backup", &a, &backup.details.backup_arn]),
                            &backup,
                        )
                        .await
                    })
                })
                .await?;
                Ok(details)
            }
            .await;
            if result.is_err() {
                let _ = stage.abandon().await;
            }
            result
        })
    }
    fn describe_backup(
        &self,
        account: &str,
        arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let (e, a, b) = (self.clone(), account.to_owned(), arn.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, b) = (e.clone(), a.clone(), b.clone());
                Box::pin(async move { Ok(e.backup(tx, &a, &b).await?.description()) })
            })
            .await
        })
    }
    fn list_backups(
        &self,
        account: &str,
        name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let (e, a, n) = (self.clone(), account.to_owned(), name.map(str::to_owned));
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, n) = (e.clone(), a.clone(), n.clone());
                Box::pin(async move {
                    let Some(owner) =
                        kv::get::<crate::catalog::Account>(tx, e.key(&["account", &a])).await?
                    else {
                        return Ok(vec![]);
                    };
                    let mut out = vec![];
                    for (_, v) in kv::all(tx, e.key(&["backup", &a])).await? {
                        let b: Backup = kv::decode(&v)?;
                        if b.account_generation == owner.generation
                            && n.as_ref()
                                .is_none_or(|n| b.table.description.table_name == *n)
                        {
                            out.push(b.summary());
                        }
                    }
                    out.sort_by(|a, b| {
                        a.backup_creation_date_time
                            .total_cmp(&b.backup_creation_date_time)
                    });
                    Ok(out)
                })
            })
            .await
        })
    }
    fn delete_backup(
        &self,
        account: &str,
        arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let (e, a, b) = (self.clone(), account.to_owned(), arn.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, b) = (e.clone(), a.clone(), b.clone());
                Box::pin(async move {
                    let backup = e.backup(tx, &a, &b).await?;
                    if let Some(id) = &backup.data_id {
                        kv::put(
                            tx,
                            e.key(&["garbage", "backup", id]),
                            &e.key(&["backup_data", id]),
                        )
                        .await?;
                    }
                    kv::delete(tx, e.key(&["backup", &a, &b])).await?;
                    Ok(backup.description())
                })
            })
            .await
        })
    }
    fn restore_table_from_backup(
        &self,
        account: &str,
        name: &str,
        arn: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let (e, a, n, b) = (
            self.clone(),
            account.to_owned(),
            name.to_owned(),
            arn.to_owned(),
        );
        Box::pin(async move {
            let mut tx = e.db.snapshot().await?;
            let backup = e.backup(tx.as_mut(), &a, &b).await?;
            let mut writer =
                restore::Writer::begin(&e, &backup.table, &n, backup.account_generation.clone())
                    .await?;
            let result = async {
                if let Some(id) = &backup.data_id {
                    for i in 0..backup.count() {
                        let item: Item = tx
                            .get_snapshot(e.key(&["backup_data", id, &format!("{i:020}")]))
                            .await
                            .map_err(kv::storage_error)?
                            .map(|v| kv::decode(&v))
                            .transpose()?
                            .ok_or_else(|| StorageError::Internal("Backup chunk missing".into()))?;
                        writer.write(&[item]).await?;
                    }
                } else {
                    writer.write(&backup.items).await?;
                }
                writer.finish().await
            }
            .await;
            if result.is_err() {
                writer.abandon().await;
            }
            result
        })
    }
    fn describe_continuous_backups(
        &self,
        account: &str,
        name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let (e, a, n) = (self.clone(), account.to_owned(), name.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, n) = (e.clone(), a.clone(), n.clone());
                Box::pin(async move {
                    e.table_by_name(tx, &a, &n).await?;
                    Ok(ContinuousBackupsDescription {
                        continuous_backups_status: "DISABLED".into(),
                        point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                            point_in_time_recovery_status: "DISABLED".into(),
                            earliest_restorable_date_time: None,
                            latest_restorable_date_time: None,
                        }),
                    })
                })
            })
            .await
        })
    }
    fn update_continuous_backups(
        &self,
        account: &str,
        name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        if enabled {
            return Box::pin(async {
                Err(StorageError::Unsupported(
                    "TiKV point-in-time recovery".into(),
                ))
            });
        }
        self.describe_continuous_backups(account, name)
    }
    fn restore_table_to_point_in_time(
        &self,
        _account: &str,
        _source: &str,
        _target: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        Box::pin(async {
            Err(StorageError::Unsupported(
                "TiKV point-in-time recovery".into(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::TikvCatalog, kv::memory::MemoryStore};
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_storage::{DataEngine, TableEngine, management_store::ManagementStore};
    use std::sync::Arc;
    #[tokio::test]
    async fn snapshot_and_publication_boundaries() {
        let e = TikvEngine::new(Arc::new(MemoryStore::default()), "snapshot", "us-east-1").unwrap();
        TikvCatalog::new(e.clone())
            .create_account("a", "a")
            .await
            .unwrap();
        e.create_table("a",serde_json::from_value(serde_json::json!({"TableName":"source","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"})).unwrap()).await.unwrap();
        let info = e.table_key_info("a", "source").await.unwrap();
        let maps = ExpressionMaps::default();
        for i in 0..20 {
            e.put_item(
                &info,
                Item::from_iter([("pk".into(), AttributeValue::S(format!("{i:02}")))]),
                false,
                None,
                &maps,
                None,
            )
            .await
            .unwrap();
        }
        let mut r = snapshot::Reader::open(&e, "a", "source").await.unwrap();
        let mut w = restore::Writer::begin(&e, &r.table, "target", r.account_generation.clone())
            .await
            .unwrap();
        w.write(&r.next().await.unwrap()).await.unwrap();
        assert!(e.table_key_info("a", "target").await.is_err());
        let key = Item::from_iter([("pk".into(), AttributeValue::S("19".into()))]);
        e.delete_item(&info, &key, false, None, &maps, None)
            .await
            .unwrap();
        e.put_item(
            &info,
            Item::from_iter([("pk".into(), AttributeValue::S("99".into()))]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
        loop {
            let rows = r.next().await.unwrap();
            if rows.is_empty() {
                break;
            }
            w.write(&rows).await.unwrap();
        }
        assert_eq!(w.finish().await.unwrap().item_count, 20);
        let target = e.table_key_info("a", "target").await.unwrap();
        assert!(e.get_item(&target, &key).await.unwrap().is_some());
        assert!(
            e.get_item(
                &target,
                &Item::from_iter([("pk".into(), AttributeValue::S("99".into()))])
            )
            .await
            .unwrap()
            .is_none()
        );
        let b = e.create_backup("a", "source", "b").await.unwrap();
        e.db.run(|tx| {
            let e = e.clone();
            let arn = b.backup_arn.clone();
            Box::pin(async move {
                let b = e.backup(tx, "a", &arn).await?;
                kv::delete(
                    tx,
                    e.key(&[
                        "backup_data",
                        b.data_id.as_ref().unwrap(),
                        "00000000000000000000",
                    ]),
                )
                .await
            })
        })
        .await
        .unwrap();
        assert!(
            e.restore_table_from_backup("a", "broken", &b.backup_arn)
                .await
                .is_err()
        );
        assert!(e.table_key_info("a", "broken").await.is_err());
    }
}
