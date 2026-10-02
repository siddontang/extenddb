// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Table catalog and schema lifecycle. Name-to-id mappings are account scoped.
//!
//! Every data operation re-reads the table by immutable id in its transaction,
//! fencing stale cached schemas against concurrent deletion/index creation.
//! Table names can be reused only after the old generation is removed. Online
//! index builds publish their metadata first; concurrent writers maintain the
//! building index while a resumable worker walks existing rows.

use crate::{
    TikvEngine,
    index::{Index, schema_order},
    kv,
};
use extenddb_core::types::*;
use extenddb_storage::{TableEngine, error::StorageError};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub account: String,
    pub description: TableDescription,
    pub indexes: Vec<Index>,
    pub transition_at: i64,
    pub ttl_attribute: Option<String>,
    pub ttl_ready: bool,
}
impl Table {
    pub fn key_info(&self) -> TableKeyInfo {
        let d = &self.description;
        TableKeyInfo {
            table_name: d.table_name.clone(),
            account_id: self.account.clone(),
            table_id: d.table_id.clone(),
            key_schema: d.key_schema.clone(),
            base_key_schema: d.key_schema.clone(),
            attribute_definitions: d.attribute_definitions.clone(),
            has_lsi: self.indexes.iter().any(|i| i.local),
            global_secondary_indexes: self
                .indexes
                .iter()
                .filter(|i| !i.local)
                .map(Index::info)
                .collect(),
            local_secondary_indexes: self
                .indexes
                .iter()
                .filter(|i| i.local)
                .map(Index::info)
                .collect(),
            stream_specification: d.stream_specification.clone(),
            vector_indexes: vec![],
        }
    }
    pub fn describe(&self) -> TableDescription {
        let mut d = self.description.clone();
        let gsis: Vec<_> = self
            .indexes
            .iter()
            .filter(|i| !i.local)
            .map(|i| GsiDescription {
                index_name: i.name.clone(),
                key_schema: i.schema.clone(),
                projection: i.projection.clone(),
                index_status: if i.cursor.is_some() {
                    "CREATING"
                } else {
                    "ACTIVE"
                }
                .into(),
                provisioned_throughput: i.throughput.as_ref().map(throughput),
                index_size_bytes: 0,
                item_count: 0,
                index_arn: format!("{}/index/{}", d.table_arn, i.name),
            })
            .collect();
        let lsis: Vec<_> = self
            .indexes
            .iter()
            .filter(|i| i.local)
            .map(|i| LsiDescription {
                index_name: i.name.clone(),
                key_schema: i.schema.clone(),
                projection: i.projection.clone(),
                index_size_bytes: 0,
                item_count: 0,
                index_arn: format!("{}/index/{}", d.table_arn, i.name),
            })
            .collect();
        d.global_secondary_indexes = (!gsis.is_empty()).then_some(gsis);
        d.local_secondary_indexes = (!lsis.is_empty()).then_some(lsis);
        d
    }
}
fn throughput(p: &ProvisionedThroughput) -> ProvisionedThroughputDescription {
    ProvisionedThroughputDescription {
        read_capacity_units: p.read_capacity_units,
        write_capacity_units: p.write_capacity_units,
        ..Default::default()
    }
}

impl TikvEngine {
    pub(crate) async fn table_by_name(
        &self,
        tx: &mut dyn kv::Transaction,
        account: &str,
        name: &str,
    ) -> Result<Table, StorageError> {
        let id: String = kv::get(tx, self.key(&["tables", account, name]))
            .await?
            .ok_or_else(|| StorageError::TableNotFound(name.into()))?;
        let t: Table = kv::get(tx, self.key(&["table", &id]))
            .await?
            .ok_or_else(|| StorageError::TableNotFound(name.into()))?;
        if t.account != account {
            return Err(StorageError::TableNotFound(name.into()));
        }
        Ok(t)
    }
    pub(crate) async fn live_table(
        &self,
        tx: &mut dyn kv::Transaction,
        info: &TableKeyInfo,
    ) -> Result<Table, StorageError> {
        let t: Table = kv::get(tx, self.key(&["table", &info.table_id]))
            .await?
            .ok_or_else(|| StorageError::TableNotFound(info.table_name.clone()))?;
        if t.account != info.account_id || t.description.table_name != info.table_name {
            return Err(StorageError::TableNotFound(info.table_name.clone()));
        }
        if t.description.table_status != TableStatus::Active {
            return Err(StorageError::TableNotActive(info.table_name.clone()));
        }
        Ok(t)
    }
    pub(crate) async fn save_table(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &Table,
    ) -> Result<(), StorageError> {
        kv::put(tx, self.key(&["table", &t.description.table_id]), t).await
    }
    pub(crate) async fn tables(
        &self,
        tx: &mut dyn kv::Transaction,
    ) -> Result<Vec<Table>, StorageError> {
        kv::all(tx, self.key(&["table"]))
            .await?
            .into_iter()
            .map(|(_, v)| kv::decode(&v))
            .collect()
    }
}

impl TableEngine for TikvEngine {
    fn create_table(
        &self,
        account: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let (engine, account) = (self.clone(), account.to_owned());
        Box::pin(async move {
            engine
                .db
                .run(|tx| {
                    let (e, a, input) = (engine.clone(), account.clone(), input.clone());
                    Box::pin(async move {
                        if input.vector_indexes.as_ref().is_some_and(|v| !v.is_empty()) {
                            return Err(StorageError::Unsupported("Vector indexes".into()));
                        }
                        let map = e.key(&["tables", &a, &input.table_name]);
                        if kv::get::<String>(tx, map.clone()).await?.is_some() {
                            return Err(StorageError::TableAlreadyExists(input.table_name));
                        }
                        // Account deletion and table creation both write this guard.
                        let guard = e.key(&["account_tables", &a]);
                        let n: u64 = kv::get(tx, guard.clone()).await?.unwrap_or(0);
                        kv::put(tx, guard, &(n + 1)).await?;
                        let id = uuid::Uuid::new_v4().to_string();
                        let now = e.clock.now_ms();
                        let delay = e
                            .setting(tx, "control_plane_delay_seconds", 0)
                            .await?
                            .max(0)
                            * 1000;
                        let mut indexes = vec![];
                        for i in input.global_secondary_indexes.unwrap_or_default() {
                            indexes.push(Index {
                                id: uuid::Uuid::new_v4().to_string(),
                                name: i.index_name,
                                local: false,
                                schema: schema_order(&i.key_schema),
                                projection: i.projection,
                                throughput: i.provisioned_throughput,
                                cursor: None,
                            });
                        }
                        for i in input.local_secondary_indexes.unwrap_or_default() {
                            indexes.push(Index {
                                id: uuid::Uuid::new_v4().to_string(),
                                name: i.index_name,
                                local: true,
                                schema: schema_order(&i.key_schema),
                                projection: i.projection,
                                throughput: None,
                                cursor: None,
                            });
                        }
                        let arn =
                            extenddb_storage::util::table_arn(&e.region, &a, &input.table_name);
                        let description = TableDescription {
                            table_name: input.table_name,
                            key_schema: schema_order(&input.key_schema),
                            attribute_definitions: input.attribute_definitions,
                            table_status: if delay == 0 {
                                TableStatus::Active
                            } else {
                                TableStatus::Creating
                            },
                            creation_date_time: now as f64 / 1000.,
                            table_arn: arn,
                            table_id: id.clone(),
                            provisioned_throughput: input
                                .provisioned_throughput
                                .as_ref()
                                .map(throughput)
                                .unwrap_or_default(),
                            billing_mode_summary: input.billing_mode.map(|billing_mode| {
                                BillingModeSummary {
                                    billing_mode,
                                    last_update_to_pay_per_request_date_time: Some(
                                        now as f64 / 1000.,
                                    ),
                                }
                            }),
                            deletion_protection_enabled: input
                                .deletion_protection_enabled
                                .unwrap_or(false),
                            stream_specification: input.stream_specification,
                            on_demand_throughput: input.on_demand_throughput,
                            table_class_summary: input
                                .table_class
                                .map(|c| serde_json::json!({"TableClass":c})),
                            ..Default::default()
                        };
                        let mut t = Table {
                            account: a,
                            description,
                            indexes,
                            transition_at: now + delay,
                            ttl_attribute: None,
                            ttl_ready: false,
                        };
                        e.configure_stream(tx, &mut t).await?;
                        kv::put(tx, map, &id).await?;
                        e.save_table(tx, &t).await?;
                        if let Some(tags) = input.tags {
                            kv::put(tx, e.key(&["tags", &t.description.table_arn]), &tags).await?;
                        }
                        Ok(t.describe())
                    })
                })
                .await
        })
    }
    fn describe_table(
        &self,
        account: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let (e, a) = (self.clone(), account.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, n) = (e.clone(), a.clone(), input.table_name.clone());
                Box::pin(async move { Ok(e.table_by_name(tx, &a, &n).await?.describe()) })
            })
            .await
        })
    }
    fn table_key_info(
        &self,
        account: &str,
        name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        let (e, a, n) = (self.clone(), account.to_owned(), name.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, n) = (e.clone(), a.clone(), n.clone());
                Box::pin(async move {
                    let t = e.table_by_name(tx, &a, &n).await?;
                    if t.description.table_status != TableStatus::Active {
                        return Err(StorageError::TableNotActive(n));
                    }
                    Ok(t.key_info())
                })
            })
            .await
        })
    }
    fn list_tables(
        &self,
        account: &str,
        input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        let (e, a) = (self.clone(), account.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, input) = (e.clone(), a.clone(), input.clone());
                Box::pin(async move {
                    let mut names: Vec<_> = e
                        .tables(tx)
                        .await?
                        .into_iter()
                        .filter(|t| t.account == a)
                        .map(|t| t.description.table_name)
                        .filter(|n| {
                            input
                                .exclusive_start_table_name
                                .as_ref()
                                .is_none_or(|s| n > s)
                        })
                        .collect();
                    names.sort();
                    let limit = input.limit.unwrap_or(100).clamp(1, 100) as usize;
                    let more = names.len() > limit;
                    names.truncate(limit);
                    Ok(ListTablesOutput {
                        last_evaluated_table_name: more.then(|| names.last().unwrap().clone()),
                        table_names: names,
                    })
                })
            })
            .await
        })
    }
    fn delete_table(
        &self,
        account: &str,
        input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let (e, a) = (self.clone(), account.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, n) = (e.clone(), a.clone(), input.table_name.clone());
                Box::pin(async move {
                    let mut t = e.table_by_name(tx, &a, &n).await?;
                    if t.description.deletion_protection_enabled {
                        return Err(StorageError::DeletionProtected(n));
                    }
                    if t.indexes.iter().any(|i| i.cursor.is_some()) {
                        return Err(StorageError::IndexesInUse(
                            "Index backfill in progress".into(),
                        ));
                    }
                    t.description.table_status = TableStatus::Deleting;
                    t.transition_at = e.clock.now_ms();
                    e.save_table(tx, &t).await?;
                    Ok(t.describe())
                })
            })
            .await
        })
    }
    fn index_info(
        &self,
        account: &str,
        name: &str,
        index: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let (e, a, n, i) = (
            self.clone(),
            account.to_owned(),
            name.to_owned(),
            index.to_owned(),
        );
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, n, i) = (e.clone(), a.clone(), n.clone(), i.clone());
                Box::pin(async move {
                    let t = e.table_by_name(tx, &a, &n).await?;
                    t.indexes
                        .iter()
                        .find(|x| x.name == i)
                        .map(Index::info)
                        .ok_or(StorageError::IndexNotFound(i))
                })
            })
            .await
        })
    }
    fn index_info_by_table_id(
        &self,
        id: &str,
        index: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let (e, id, i) = (self.clone(), id.to_owned(), index.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, id, i) = (e.clone(), id.clone(), i.clone());
                Box::pin(async move {
                    let t: Table = kv::get(tx, e.key(&["table", &id]))
                        .await?
                        .ok_or(StorageError::TableNotFound(id))?;
                    t.indexes
                        .iter()
                        .find(|x| x.name == i)
                        .map(Index::info)
                        .ok_or(StorageError::IndexNotFound(i))
                })
            })
            .await
        })
    }
    fn update_table(
        &self,
        account: &str,
        input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let (e, a) = (self.clone(), account.to_owned());
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, a, input) = (e.clone(), a.clone(), input.clone());
                Box::pin(async move {
                    let mut t = e.table_by_name(tx, &a, &input.table_name).await?;
                    if t.description.table_status != TableStatus::Active {
                        return Err(StorageError::TableNotActive(input.table_name));
                    }
                    if input
                        .vector_index_updates
                        .as_ref()
                        .is_some_and(|v| !v.is_empty())
                    {
                        return Err(StorageError::Unsupported("Vector indexes".into()));
                    }
                    if let Some(v) = input.deletion_protection_enabled {
                        t.description.deletion_protection_enabled = v;
                    }
                    if let Some(v) = input.billing_mode {
                        t.description.billing_mode_summary = Some(BillingModeSummary {
                            billing_mode: v,
                            last_update_to_pay_per_request_date_time: Some(
                                e.clock.now_ms() as f64 / 1000.,
                            ),
                        });
                    }
                    if let Some(v) = input.provisioned_throughput {
                        t.description.provisioned_throughput = throughput(&v);
                    }
                    if let Some(v) = input.on_demand_throughput {
                        t.description.on_demand_throughput = Some(v);
                    }
                    if let Some(v) = input.table_class {
                        t.description.table_class_summary =
                            Some(serde_json::json!({"TableClass":v}));
                    }
                    if let Some(v) = input.stream_specification {
                        t.description.stream_specification = Some(v);
                        e.configure_stream(tx, &mut t).await?;
                    }
                    for update in input.global_secondary_index_updates.unwrap_or_default() {
                        if let Some(c) = update.create {
                            if t.indexes.iter().any(|i| i.name == c.index_name) {
                                return Err(StorageError::IndexAlreadyExists(c.index_name));
                            }
                            if t.indexes.iter().any(|i| i.cursor.is_some()) {
                                return Err(StorageError::IndexesInUse(
                                    "Index backfill in progress".into(),
                                ));
                            }
                            t.indexes.push(Index {
                                id: uuid::Uuid::new_v4().to_string(),
                                name: c.index_name,
                                local: false,
                                schema: schema_order(&c.key_schema),
                                projection: c.projection,
                                throughput: c.provisioned_throughput,
                                cursor: Some(vec![]),
                            });
                        }
                        if let Some(d) = update.delete {
                            let pos = t
                                .indexes
                                .iter()
                                .position(|i| !i.local && i.name == d.index_name)
                                .ok_or(StorageError::IndexNotFound(d.index_name))?;
                            let removed = t.indexes.remove(pos);
                            kv::put(
                                tx,
                                e.key(&["garbage", &t.description.table_id, &removed.id]),
                                &e.key(&["data", &t.description.table_id, "index", &removed.id]),
                            )
                            .await?;
                        }
                    }
                    let schemas: Vec<_> = t.indexes.iter().map(|i| i.schema.clone()).collect();
                    t.description.attribute_definitions =
                        extenddb_storage::util::effective_attribute_definitions(
                            &t.description.attribute_definitions,
                            &input.attribute_definitions.unwrap_or_default(),
                            &t.description.key_schema,
                            &schemas,
                        );
                    e.save_table(tx, &t).await?;
                    Ok(t.describe())
                })
            })
            .await
        })
    }
}
