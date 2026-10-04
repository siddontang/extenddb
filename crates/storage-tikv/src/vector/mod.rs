// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional vector indexes and exact, partition-scoped similarity search.
//!
//! Rows live under an immutable index generation. Shared vector lifecycle helpers
//! own projection, validation and poison handling. Live writes update rows in the
//! base transaction; backfill protects each source read, so it cannot resurrect a
//! stale vector. Search uses one snapshot and retains only a page plus top-k.
//! This is exact search, not ANN: cost grows with the selected partition.
pub mod build;
mod score;
use crate::{TikvEngine, codec, kv, table::Table};
use extenddb_core::{
    types::*,
    validation::{vector_components, vector_item::vector_attribute},
};
use extenddb_storage::{
    VectorHit, VectorSearch, VectorSearchEngine, VectorSearchOutput, VectorSearchResult,
    error::StorageError, vector_lifecycle::*,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndex {
    pub id: String,
    pub spec: VectorIndexSpecification,
    pub backfilling: Option<bool>,
    pub cursor: Vec<u8>,
    pub complete: bool,
    pub skipped: usize,
}
impl VectorIndex {
    pub fn new(mut spec: VectorIndexSpecification, building: bool) -> Result<Self, StorageError> {
        spec.normalize_search_schema();
        if spec.projection.is_none() || spec.dimensions == 0 || spec.dimensions > 4096 {
            return Err(StorageError::Validation(
                "Invalid vector index specification".into(),
            ));
        }
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            spec,
            backfilling: building.then_some(false),
            cursor: vec![],
            complete: !building,
            skipped: 0,
        })
    }
    pub fn meta(&self) -> VectorIndexMeta {
        VectorIndexMeta {
            index_id: self.id.clone(),
            dimensions: self.spec.dimensions as usize,
            vector_attribute_name: self.spec.vector_attribute.attribute_name.clone(),
            hash_attribute_name: self
                .spec
                .search_schema
                .iter()
                .flatten()
                .find(|s| s.element_type == SearchSchemaElementType::Hash)
                .map(|s| s.attribute_name.clone()),
            search_schema_attribute_names: self
                .spec
                .search_schema
                .iter()
                .flatten()
                .map(|s| s.attribute_name.clone())
                .collect(),
            projection: self
                .spec
                .projection
                .clone()
                .expect("validated vector projection"),
        }
    }
    pub fn key_info(&self) -> VectorIndexKeyInfo {
        let m = self.meta();
        VectorIndexKeyInfo {
            index_name: self.spec.index_name.clone(),
            dimensions: self.spec.dimensions,
            vector_attribute_name: m.vector_attribute_name,
            search_schema: self.spec.search_schema.clone().unwrap_or_default(),
            projection: m.projection,
        }
    }
    pub fn describe(&self, arn: &str) -> VectorIndexDescription {
        VectorIndexDescription {
            index_name: self.spec.index_name.clone(),
            vector_attribute: self.spec.vector_attribute.clone(),
            dimensions: self.spec.dimensions,
            search_schema: self.spec.search_schema.clone(),
            distance_function: self.spec.distance_function,
            index_status: if self.backfilling.is_some() {
                IndexStatus::Creating
            } else {
                IndexStatus::Active
            },
            backfilling: self.backfilling,
            index_size_bytes: 0,
            item_count: 0,
            index_arn: format!("{arn}/index/{}", self.spec.index_name),
            projection: self.spec.projection.clone(),
        }
    }
}
#[derive(Serialize, Deserialize)]
struct Row {
    components: Vec<f32>,
    item: Item,
}
pub(crate) fn prefix(e: &TikvEngine, table: &str, index: &str) -> Vec<u8> {
    e.key(&["data", table, "vector", index])
}
fn row_key(
    e: &TikvEngine,
    t: &Table,
    index: &VectorIndex,
    item: &Item,
) -> Result<Option<Vec<u8>>, StorageError> {
    let meta = index.meta();
    if !item_is_indexable(item, &meta) {
        return Ok(None);
    }
    let mut key = prefix(e, &t.description.table_id, &index.id);
    codec::component(&mut key, item_partition(item, &meta)?.as_bytes());
    codec::item_key(&mut key, item, &t.description.key_schema)?;
    Ok(Some(key))
}
/// Idempotent index maintenance shared by mutations, backfill and restore.
/// Malformed pre-index values are omitted; callers validate changed attributes.
pub(crate) async fn apply(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    t: &Table,
    index: &VectorIndex,
    old: Option<&Item>,
    new: Option<&Item>,
) -> Result<(), StorageError> {
    if let Some(old) = old {
        match row_key(e, t, index, old) {
            Ok(Some(key)) => kv::delete(tx, key).await?,
            Ok(None) | Err(StorageError::Validation(_)) => {}
            Err(error) => return Err(error),
        }
    }
    if let Some(item) = new {
        let meta = index.meta();
        if let Some(components) = item
            .get(&meta.vector_attribute_name)
            .and_then(vector_components)
            && components.len() == meta.dimensions
            && let Ok(Some(key)) = row_key(e, t, index, item)
        {
            kv::put(
                tx,
                key,
                &Row {
                    components,
                    item: projected_payload(item, &t.description.key_schema, &meta),
                },
            )
            .await?;
        }
    }
    Ok(())
}
impl VectorSearchEngine for TikvEngine {
    fn search_vectors(&self, req: VectorSearch<'_>) -> BoxFuture<'_, VectorSearchResult> {
        let (e, info, name, query, k) = (
            self.clone(),
            req.key_info.clone(),
            req.index_name.to_owned(),
            req.query_vector.to_vec(),
            req.top_k,
        );
        let hash = req.hash_key.map(|(n, v)| (n.to_owned(), v.clone()));
        let filters: Vec<_> = req
            .filters
            .iter()
            .map(|(n, v)| (n.to_string(), (*v).clone()))
            .collect();
        Box::pin(async move {
            e.db.run(|tx| {
                let (e, info, name, query, hash, filters) = (
                    e.clone(),
                    info.clone(),
                    name.clone(),
                    query.clone(),
                    hash.clone(),
                    filters.clone(),
                );
                Box::pin(async move {
                    let t = e.live_table(tx, &info).await?;
                    let idx = t
                        .vectors
                        .iter()
                        .find(|v| v.spec.index_name == name)
                        .ok_or_else(|| StorageError::IndexNotFound(name.clone()))?;
                    if idx.backfilling.is_some() {
                        return Err(StorageError::IndexesInUse(
                            "Vector index is backfilling".into(),
                        ));
                    }
                    if query.len() != idx.spec.dimensions as usize
                        || query.iter().any(|v| !v.is_finite())
                        || !(1..=100).contains(&k)
                    {
                        return Err(StorageError::Validation(
                            "Invalid query dimensions, components or top-k".into(),
                        ));
                    }
                    let meta = idx.meta();
                    if meta.hash_attribute_name.as_deref() != hash.as_ref().map(|h| h.0.as_str()) {
                        return Err(StorageError::Validation(
                            "Search must supply the vector index HASH attribute".into(),
                        ));
                    }
                    let partition = partition_value(hash.as_ref().map(|(n, v)| (n.as_str(), v)))?;
                    let mut start = prefix(&e, &info.table_id, &idx.id);
                    codec::component(&mut start, partition.as_bytes());
                    let end = codec::prefix_end(&start);
                    let mut top = score::TopK::new(k as usize, idx.spec.distance_function);
                    loop {
                        let rows = tx
                            .scan_snapshot(start.clone(), end.clone(), 64, false)
                            .await
                            .map_err(kv::storage_error)?;
                        let short = rows.len() < 64;
                        for (key, bytes) in rows {
                            start = key;
                            start.push(0);
                            let row: Row = kv::decode(&bytes)?;
                            if row.components.len() != query.len()
                                || row.components.iter().any(|v| !v.is_finite())
                            {
                                return Err(StorageError::Internal("Corrupt stored vector".into()));
                            }
                            if filters.iter().all(|(n, v)| row.item.get(n) == Some(v)) {
                                top.offer(
                                    score::score(
                                        idx.spec.distance_function,
                                        &query,
                                        &row.components,
                                    ),
                                    row.item,
                                    row.components,
                                );
                            }
                        }
                        if short {
                            break;
                        }
                    }
                    Ok(VectorSearchOutput {
                        distance_function: idx.spec.distance_function,
                        hits: top
                            .hits
                            .into_iter()
                            .map(|(score, mut item, components)| {
                                item.insert(
                                    meta.vector_attribute_name.clone(),
                                    vector_attribute(&components),
                                );
                                VectorHit { item, score }
                            })
                            .collect(),
                    })
                })
            })
            .await
        })
    }
}
