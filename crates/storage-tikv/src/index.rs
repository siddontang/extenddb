// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Secondary-index key construction and projection, with no network dependencies.
//!
//! Entries include the complete base key after the index key, preserving all
//! duplicates and making forward/reverse pagination exact inverses. Sparse
//! items are omitted. LSIs and GSIs use the same representation and are maintained
//! atomically with the base item. Backfill writes use
//! the same helpers as ordinary writes.

use crate::{TikvEngine, codec, kv};
use extenddb_core::types::*;
use extenddb_storage::error::StorageError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub id: String,
    pub name: String,
    pub local: bool,
    pub schema: Vec<KeySchemaElement>,
    pub projection: Projection,
    pub throughput: Option<ProvisionedThroughput>,
    /// `Some` while building; an empty cursor starts at the beginning.
    pub cursor: Option<Vec<u8>>,
}
impl Index {
    pub fn info(&self) -> IndexInfo {
        IndexInfo {
            index_id: self.id.clone(),
            index_name: self.name.clone(),
            index_type: if self.local {
                IndexType::Lsi
            } else {
                IndexType::Gsi
            },
            key_schema: self.schema.clone(),
            projection: self.projection.clone(),
        }
    }
}
pub fn schema_order(schema: &[KeySchemaElement]) -> Vec<KeySchemaElement> {
    schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
        .chain(schema.iter().filter(|k| k.key_type == KeyType::Range))
        .cloned()
        .collect()
}
pub fn project(item: &Item, index: &Index, base: &[KeySchemaElement]) -> Item {
    if index.projection.projection_type == ProjectionType::All {
        return item.clone();
    }
    let mut out = Item::new();
    for name in base
        .iter()
        .chain(&index.schema)
        .map(|k| &k.attribute_name)
        .chain(index.projection.non_key_attributes.iter().flatten())
    {
        if let Some(v) = item.get(name) {
            out.insert(name.clone(), v.clone());
        }
    }
    out
}
pub fn entry_key(
    engine: &TikvEngine,
    table_id: &str,
    index: &Index,
    item: &Item,
    base: &[KeySchemaElement],
) -> Result<Option<Vec<u8>>, StorageError> {
    if index
        .schema
        .iter()
        .any(|k| !item.contains_key(&k.attribute_name))
    {
        return Ok(None);
    }
    let mut key = engine.key(&["data", table_id, "index", &index.id]);
    codec::item_key(&mut key, item, &schema_order(&index.schema))?;
    codec::item_key(&mut key, item, &schema_order(base))?;
    Ok(Some(key))
}
pub async fn apply(
    engine: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    table_id: &str,
    index: &Index,
    base: &[KeySchemaElement],
    old: Option<&Item>,
    new: Option<&Item>,
) -> Result<(), StorageError> {
    if let Some(item) = old {
        match entry_key(engine, table_id, index, item, base) {
            Ok(Some(key)) => kv::delete(tx, key).await?,
            // Online index creation skips pre-existing invalid key values.
            // Such an item has no old entry, but must remain repairable/deletable.
            Ok(None) | Err(StorageError::Validation(_)) => {}
            Err(error) => return Err(error),
        }
    }
    if let Some(item) = new
        && let Some(key) = entry_key(engine, table_id, index, item, base)?
    {
        kv::put(tx, key, &project(item, index, base)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn projection_keeps_keys_and_selected_attributes() {
        let item: Item = serde_json::from_value(
            serde_json::json!({"pk":{"S":"a"},"g":{"S":"b"},"keep":{"N":"1"},"drop":{"S":"no"}}),
        )
        .unwrap();
        let base = vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }];
        let idx = Index {
            id: "i".into(),
            name: "g".into(),
            local: false,
            schema: vec![KeySchemaElement {
                attribute_name: "g".into(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::Include,
                non_key_attributes: Some(vec!["keep".into()]),
            },
            throughput: None,
            cursor: None,
        };
        let result = project(&item, &idx, &base);
        assert_eq!(result.len(), 3);
        assert!(!result.contains_key("drop"));
    }
}
