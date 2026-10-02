// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Atomic, account-scoped on-demand snapshots for small tables.
//!
//! The current implementation deliberately caps the encoded backup at 4 MiB.
//! It refuses larger tables before writing anything, rather than publishing an
//! inconsistent chunked snapshot. Restore publishes a fresh table generation,
//! rebuilt indexes and all items in one commit. Production bulk backup belongs
//! in a future pinned-timestamp/export implementation. PITR is not advertised.
use crate::{TikvEngine, index, kv, table::Table};
use extenddb_core::types::*;
use extenddb_storage::{BackupEngine, error::StorageError};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
const MAX_BACKUP: usize = 4 * 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
struct Backup {
    #[serde(default)]
    account_generation: String,
    table: Table,
    details: BackupDetails,
    items: Vec<Item>,
}
impl Backup {
    fn description(&self) -> BackupDescription {
        let d = &self.table.description;
        BackupDescription {
            backup_details: self.details.clone(),
            source_table_details: SourceTableDetails {
                table_name: d.table_name.clone(),
                table_id: d.table_id.clone(),
                table_arn: d.table_arn.clone(),
                key_schema: d.key_schema.clone(),
                item_count: self.items.len() as i64,
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
            e.db.run(|tx| {
                let (e, a, n, b) = (e.clone(), a.clone(), n.clone(), b.clone());
                Box::pin(async move {
                    let t = e.table_by_name(tx, &a, &n).await?;
                    if t.description.table_status != TableStatus::Active
                        || t.indexes.iter().any(|i| i.cursor.is_some())
                    {
                        return Err(StorageError::TableNotActive(n));
                    }
                    let p = e.item_prefix(&t.description.table_id);
                    let mut start = p.clone();
                    let mut items = vec![];
                    let mut bytes = 0;
                    loop {
                        let rows = tx
                            .scan(start.clone(), crate::codec::prefix_end(&p), 64, false)
                            .await
                            .map_err(kv::storage_error)?;
                        if rows.is_empty() {
                            break;
                        }
                        for (k, v) in &rows {
                            bytes += v.len();
                            if bytes > MAX_BACKUP {
                                return Err(StorageError::Validation(
                                    "TiKV on-demand backup exceeds the 4 MiB snapshot limit".into(),
                                ));
                            }
                            items.push(kv::decode::<Item>(v)?);
                            start = k.clone();
                            start.push(0);
                        }
                        if rows.len() < 64 {
                            break;
                        }
                    }
                    let size = items.iter().map(item_size_bytes).sum::<usize>();
                    let details = BackupDetails {
                        backup_arn: format!(
                            "{}/backup/{:017}-{:08x}",
                            t.description.table_arn,
                            e.clock.now_ms(),
                            rand::random::<u32>()
                        ),
                        backup_name: b,
                        backup_status: "AVAILABLE".into(),
                        backup_type: "USER".into(),
                        backup_size_bytes: size as i64,
                        backup_creation_date_time: e.clock.now_ms() as f64 / 1000.,
                    };
                    let backup = Backup {
                        account_generation: kv::get::<crate::catalog::Account>(
                            tx,
                            e.key(&["account", &a]),
                        )
                        .await?
                        .ok_or_else(missing)?
                        .generation,
                        table: t,
                        details: details.clone(),
                        items,
                    };
                    if serde_json::to_vec(&backup)
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .len()
                        > MAX_BACKUP
                    {
                        return Err(StorageError::Validation(
                            "TiKV on-demand backup exceeds the 4 MiB snapshot limit".into(),
                        ));
                    }
                    let key = e.key(&["backup", &a, &details.backup_arn]);
                    // The DynamoDB-shaped suffix is only 32 bits. Protect its
                    // absence and retry the whole closure on a collision.
                    if kv::get::<Backup>(tx, key.clone()).await?.is_some() {
                        return Err(StorageError::TransactionConflict(
                            "Backup id collision".into(),
                        ));
                    }
                    kv::put(tx, key, &backup).await?;
                    Ok(details)
                })
            })
            .await
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
                    let mut desc = e.backup(tx, &a, &b).await?.description();
                    kv::delete(tx, e.key(&["backup", &a, &b])).await?;
                    desc.backup_details.backup_status = "DELETED".into();
                    Ok(desc)
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
            e.db.run(|tx| {
                let (e, a, n, b) = (e.clone(), a.clone(), n.clone(), b.clone());
                Box::pin(async move {
                    let backup = e.backup(tx, &a, &b).await?;
                    if kv::get::<crate::catalog::Account>(tx, e.key(&["account", &a]))
                        .await?
                        .is_none()
                    {
                        return Err(StorageError::Validation("Account does not exist".into()));
                    }
                    let map = e.key(&["tables", &a, &n]);
                    if kv::get::<String>(tx, map.clone()).await?.is_some() {
                        return Err(StorageError::TableAlreadyExists(n));
                    }
                    let mut t = backup.table;
                    t.description.table_id = uuid::Uuid::new_v4().to_string();
                    t.description.table_name = n;
                    t.description.table_arn =
                        extenddb_storage::util::table_arn(&e.region, &a, &t.description.table_name);
                    t.description.creation_date_time = e.clock.now_ms() as f64 / 1000.;
                    // Data and indexes are already complete at commit. Expose
                    // the normal CREATING -> ACTIVE lifecycle without allowing
                    // a reader to observe an incomplete restored table.
                    t.description.table_status = TableStatus::Creating;
                    t.transition_at = e.clock.now_ms() + e.control_plane_delay_ms(tx).await?.max(1);
                    t.description.latest_stream_arn = None;
                    t.description.latest_stream_label = None;
                    t.description.stream_specification = None;
                    t.description.deletion_protection_enabled = false;
                    t.ttl_attribute = None;
                    t.ttl_ready = false;
                    t.ttl_cursor = None;
                    t.ttl_generation = String::new();
                    for i in &mut t.indexes {
                        i.id = uuid::Uuid::new_v4().to_string();
                        i.cursor = None;
                    }
                    t.description.item_count = backup.items.len() as i64;
                    t.description.table_size_bytes = backup.details.backup_size_bytes;
                    let info = t.key_info();
                    for item in backup.items {
                        kv::put(tx, e.item_key(&info, &item)?, &item).await?;
                        for i in &t.indexes {
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
                    }
                    let guard = e.key(&["account_tables", &a]);
                    let count: u64 = kv::get(tx, guard.clone()).await?.unwrap_or(0);
                    kv::put(tx, guard, &(count + 1)).await?;
                    kv::put(tx, map, &info.table_id).await?;
                    e.save_table(tx, &t).await?;
                    Ok(t.describe())
                })
            })
            .await
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
