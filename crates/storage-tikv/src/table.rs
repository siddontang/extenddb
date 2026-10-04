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
    #[serde(default)]
    pub fence_generation: u64,
    pub description: TableDescription,
    pub indexes: Vec<Index>,
    #[serde(default)]
    pub vectors: Vec<crate::vector::VectorIndex>,
    pub transition_at: i64,
    pub ttl_attribute: Option<String>,
    pub ttl_ready: bool,
    #[serde(default)]
    pub ttl_cursor: Option<Vec<u8>>,
    #[serde(default)]
    pub ttl_generation: String,
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
            vector_indexes: self
                .vectors
                .iter()
                .map(crate::vector::VectorIndex::key_info)
                .collect(),
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
        d.vector_indexes = (!self.vectors.is_empty()).then(|| {
            self.vectors
                .iter()
                .map(|v| v.describe(&d.table_arn))
                .collect()
        });
        d.global_secondary_indexes = (!gsis.is_empty()).then_some(gsis);
        d.local_secondary_indexes = (!lsis.is_empty()).then_some(lsis);
        // Provisioned mode stays in the internal catalog, but DynamoDB omits
        // BillingModeSummary (and its wire alias) for provisioned tables.
        d.billing_mode_summary = d
            .billing_mode_summary
            .filter(|summary| summary.billing_mode == BillingMode::PayPerRequest);
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
        let t: Table = kv::snapshot_get(tx, self.key(&["table", &info.table_id]))
            .await?
            .ok_or_else(|| StorageError::TableNotFound(info.table_name.clone()))?;
        if t.account != info.account_id || t.description.table_name != info.table_name {
            return Err(StorageError::TableNotFound(info.table_name.clone()));
        }
        if t.description.table_status != TableStatus::Active {
            return Err(StorageError::TableNotFound(info.table_name.clone()));
        }
        Ok(t)
    }
    pub(crate) async fn save_table(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &Table,
    ) -> Result<(), StorageError> {
        let key = self.key(&["table", &t.description.table_id]);
        let old: Option<Table> = kv::get(tx, key.clone()).await?;
        let mut t = t.clone();
        let changed = match &old {
            None => true,
            Some(old) => schema_fingerprint(old)? != schema_fingerprint(&t)?,
        };
        t.fence_generation = old.as_ref().map_or(0, |t| t.fence_generation);
        if changed {
            t.fence_generation = t.fence_generation.checked_add(1).ok_or_else(|| {
                StorageError::Internal("Table schema generation exhausted".into())
            })?;
            for slot in 0..FENCE_SLOTS {
                kv::put(
                    tx,
                    self.fence_key(&t.description.table_id, slot),
                    &t.fence_generation,
                )
                .await?;
            }
        }
        kv::put(tx, key, &t).await
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
    fn validate_sse_specification(
        &self,
        specification: Option<&serde_json::Value>,
    ) -> Result<(), StorageError> {
        if specification
            .and_then(|spec| spec.get("Enabled"))
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            return Err(StorageError::Unsupported("TiKV SSE/KMS encryption".into()));
        }
        Ok(())
    }
    fn create_table(
        &self,
        account: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let (engine, account) = (self.clone(), account.to_owned());
        Box::pin(async move {
            engine.validate_sse_specification(input.sse_specification.as_ref())?;
            engine
                .db
                .run(|tx| {
                    let (e, a, input) = (engine.clone(), account.clone(), input.clone());
                    Box::pin(async move {
                        if kv::get::<crate::catalog::Account>(tx, e.key(&["account", &a]))
                            .await?
                            .is_none()
                        {
                            return Err(StorageError::Validation("Account does not exist".into()));
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
                        let delay = e.control_plane_delay_ms(tx).await?;
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
                            fence_generation: 0,
                            description,
                            indexes,
                            vectors: input
                                .vector_indexes
                                .unwrap_or_default()
                                .into_iter()
                                .map(|v| crate::vector::VectorIndex::new(v, false))
                                .collect::<Result<_, _>>()?,
                            transition_at: now + delay,
                            ttl_attribute: None,
                            ttl_ready: false,
                            ttl_cursor: None,
                            ttl_generation: String::new(),
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
                        return Err(StorageError::TableNotFound(n));
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
                    validate_billing_update(&t.description, &input)?;
                    for update in input.vector_index_updates.unwrap_or_default() {
                        if let Some(spec) = update.create {
                            if t.vectors
                                .iter()
                                .any(|v| v.spec.index_name == spec.index_name)
                                || t.indexes.iter().any(|v| v.name == spec.index_name)
                            {
                                return Err(StorageError::IndexAlreadyExists(spec.index_name));
                            }
                            t.vectors.push(crate::vector::VectorIndex::new(spec, true)?);
                        }
                        if let Some(delete) = update.delete {
                            let pos = t
                                .vectors
                                .iter()
                                .position(|v| v.spec.index_name == delete.index_name)
                                .ok_or_else(|| {
                                    StorageError::IndexNotFound(delete.index_name.clone())
                                })?;
                            if t.vectors[pos].backfilling == Some(false) {
                                return Err(StorageError::IndexesInUse(
                                    "Cannot delete vector index before backfill starts".into(),
                                ));
                            }
                            let removed = t.vectors.remove(pos);
                            kv::put(
                                tx,
                                e.key(&["garbage", &t.description.table_id, &removed.id]),
                                &crate::vector::prefix(&e, &t.description.table_id, &removed.id),
                            )
                            .await?;
                        }
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
                    let mut schemas: Vec<_> = t.indexes.iter().map(|i| i.schema.clone()).collect();
                    schemas.extend(t.vectors.iter().map(|v| {
                        v.spec
                            .search_schema
                            .iter()
                            .flatten()
                            .map(|s| KeySchemaElement {
                                attribute_name: s.attribute_name.clone(),
                                key_type: KeyType::Hash,
                            })
                            .collect()
                    }));
                    t.description.attribute_definitions =
                        extenddb_storage::util::effective_attribute_definitions(
                            &t.description.attribute_definitions,
                            &input.attribute_definitions.clone().unwrap_or_default(),
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

/// Validate against the current catalog in the same transaction as the update.
/// Request-only validation cannot infer the effective mode when it is omitted.
fn validate_billing_update(
    current: &TableDescription,
    input: &UpdateTableInput,
) -> Result<(), StorageError> {
    let mode = current
        .billing_mode_summary
        .as_ref()
        .map_or(BillingMode::Provisioned, |s| s.billing_mode);
    let effective = input.billing_mode.unwrap_or(mode);
    if let Some(requested) = &input.provisioned_throughput {
        if effective == BillingMode::PayPerRequest {
            return Err(StorageError::Validation(
                "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST".into(),
            ));
        }
        let old = &current.provisioned_throughput;
        if input.billing_mode == Some(BillingMode::Provisioned)
            && mode == BillingMode::Provisioned
            && old.read_capacity_units == requested.read_capacity_units
            && old.write_capacity_units == requested.write_capacity_units
        {
            return Err(StorageError::NoOpUpdate(format!(
                "The provisioned throughput for the table will not change. \
                 The requested value equals the current value. \
                 Current ReadCapacityUnits provisioned for the table: {}. \
                 Requested ReadCapacityUnits: {}. \
                 Current WriteCapacityUnits provisioned for the table: {}. \
                 Requested WriteCapacityUnits: {}.",
                old.read_capacity_units,
                requested.read_capacity_units,
                old.write_capacity_units,
                requested.write_capacity_units,
            )));
        }
    }
    Ok(())
}

/// Separate read-dependency slots prevent unrelated item writes from locking
/// the same metadata key. Every schema change updates all slots atomically.
pub(crate) const FENCE_SLOTS: usize = 256;
fn schema_fingerprint(t: &Table) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&(
        &t.description.key_schema,
        &t.description.attribute_definitions,
        &t.description.table_status,
        &t.description.stream_specification,
        &t.description.latest_stream_arn,
        &t.ttl_attribute,
        &t.ttl_generation,
        t.vectors
            .iter()
            .map(|v| (&v.id, &v.spec))
            .collect::<Vec<_>>(),
        t.indexes
            .iter()
            .map(|i| (&i.id, &i.name, &i.schema, &i.projection))
            .collect::<Vec<_>>(),
    ))
    .map_err(|e| StorageError::Internal(e.to_string()))
}
impl TikvEngine {
    pub(crate) fn fence_key(&self, id: &str, slot: usize) -> Vec<u8> {
        self.key(&["data", id, "fence", &format!("{slot:03}")])
    }
    pub(crate) async fn protect_table(
        &self,
        tx: &mut dyn kv::Transaction,
        t: &Table,
        item_key: &[u8],
    ) -> Result<(), StorageError> {
        let slot = crate::query::stable_hash(item_key) as usize % FENCE_SLOTS;
        let version: u64 = kv::get(tx, self.fence_key(&t.description.table_id, slot))
            .await?
            .unwrap_or(0);
        if version != t.fence_generation {
            return Err(StorageError::TransactionConflict(
                "Table schema changed".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod fencing_tests {
    use super::*;
    use crate::{
        catalog::TikvCatalog,
        data::{Change, Mutation},
        kv::{Store, Transaction, memory::MemoryStore},
    };
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_storage::management_store::ManagementStore;
    use std::sync::Arc;
    #[tokio::test]
    async fn schema_change_fences_old_writes_without_serializing_distinct_slots() {
        let store = Arc::new(MemoryStore::default());
        let e = TikvEngine::new(store.clone(), "fencing", "us-east-1").unwrap();
        TikvCatalog::new(e.clone())
            .create_account("111111111111", "test")
            .await
            .unwrap();
        let input=serde_json::from_value(serde_json::json!({"TableName":"items","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"})).unwrap();
        e.create_table("111111111111", input).await.unwrap();
        let info = e.table_key_info("111111111111", "items").await.unwrap();
        async fn stage(e: &TikvEngine, tx: &mut dyn Transaction, info: &TableKeyInfo, pk: &str) {
            let item: Item = serde_json::from_value(serde_json::json!({"pk":{"S":pk}})).unwrap();
            e.mutate(
                tx,
                &Mutation {
                    info: info.clone(),
                    item,
                    condition: None,
                    maps: ExpressionMaps::default(),
                    stream: None,
                    change: Change::Put,
                    ccf: ReturnValuesOnConditionCheckFailure::None,
                },
            )
            .await
            .unwrap();
        }
        let mut left = store.begin().await.unwrap();
        let mut right = store.begin().await.unwrap();
        stage(&e, left.as_mut(), &info, "a").await;
        stage(&e, right.as_mut(), &info, "b").await;
        left.commit().await.unwrap();
        right.commit().await.unwrap();
        let mut stale = store.begin().await.unwrap();
        stage(&e, stale.as_mut(), &info, "c").await;
        let update=serde_json::from_value(serde_json::json!({"TableName":"items","StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"}})).unwrap();
        e.update_table("111111111111", update).await.unwrap();
        assert!(matches!(stale.commit().await, Err(kv::Error::Conflict)));
        stale.rollback().await.unwrap();
    }
}
