// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pure range planning plus bounded ordered reads for Query/Scan.
//!
//! Sort-key bounds are translated to byte ranges before contacting TiKV.
//! Prefix queries remove the tuple terminator to include longer values.
//! Index cursors include the full base key. Filters/projections and the 1 MiB
//! wire budget remain in ExtendDB's shared engine, so `limit` counts evaluated
//! key matches. Multipart residual equalities are checked before that limit.

use crate::{TikvEngine, codec, index, kv};
use extenddb_core::{
    expression::{CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition},
    types::*,
};
use extenddb_storage::{QueryResult, error::StorageError};

#[derive(Clone)]
pub(crate) struct Read {
    pub info: TableKeyInfo,
    pub condition: Option<KeyCondition>,
    pub maps: ExpressionMaps,
    pub forward: bool,
    pub limit: u32,
    pub start: Option<Item>,
    pub index: Option<String>,
    pub segment: Option<(i64, i64)>,
}
fn value(e: &Expr, maps: &ExpressionMaps) -> Result<AttributeValue, StorageError> {
    match e {
        Expr::Placeholder(n) => maps
            .resolve_value(n)
            .cloned()
            .map_err(|e| StorageError::Validation(e.to_string())),
        _ => Err(StorageError::Validation("Expected key placeholder".into())),
    }
}
fn append(mut p: Vec<u8>, v: &Expr, m: &ExpressionMaps) -> Result<Vec<u8>, StorageError> {
    codec::component(&mut p, &codec::scalar(&value(v, m)?)?);
    Ok(p)
}
fn successor(p: Vec<u8>) -> Result<Vec<u8>, StorageError> {
    codec::prefix_end(&p).ok_or_else(|| StorageError::Internal("Unbounded encoded tuple".into()))
}
fn bounds(
    prefix: Vec<u8>,
    condition: Option<&SortKeyCondition>,
    maps: &ExpressionMaps,
) -> Result<(Vec<u8>, Option<Vec<u8>>), StorageError> {
    let end = codec::prefix_end(&prefix);
    let Some(c) = condition else {
        return Ok((prefix, end));
    };
    Ok(match c {
        SortKeyCondition::Compare { op, value, .. } => {
            let v = append(prefix.clone(), value, maps)?;
            match op {
                CompareOp::Eq => (v.clone(), Some(successor(v)?)),
                CompareOp::Lt => (prefix, Some(v)),
                CompareOp::Le => (prefix, Some(successor(v)?)),
                CompareOp::Gt => (successor(v)?, end),
                CompareOp::Ge => (v, end),
                CompareOp::Ne => {
                    return Err(StorageError::Validation(
                        "Unsupported key comparison".into(),
                    ));
                }
            }
        }
        SortKeyCondition::Between { low, high, .. } => (
            append(prefix.clone(), low, maps)?,
            Some(successor(append(prefix, high, maps)?)?),
        ),
        SortKeyCondition::BeginsWith { prefix: p, .. } => {
            let v = value(p, maps)?;
            if matches!(v, AttributeValue::N(_)) {
                return Err(StorageError::Validation(
                    "begins_with does not support numeric keys".into(),
                ));
            }
            let mut start = prefix;
            codec::component(&mut start, &codec::scalar(&v)?);
            start.truncate(start.len() - 2);
            let end = codec::prefix_end(&start);
            (start, end)
        }
    })
}
fn path_name<'a>(
    path: &'a [PathElement],
    maps: &'a ExpressionMaps,
) -> Result<&'a str, StorageError> {
    match path {
        [PathElement::Attribute(n)] => {
            if let Some(n) = n.strip_prefix('#') {
                maps.names
                    .get(n)
                    .map(String::as_str)
                    .ok_or_else(|| StorageError::Validation("Missing key name".into()))
            } else {
                Ok(n)
            }
        }
        _ => Err(StorageError::Validation("Invalid key path".into())),
    }
}
pub(crate) fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}

impl TikvEngine {
    pub(crate) async fn read(&self, req: Read) -> QueryResult {
        self.db
            .run(|tx| {
                let (e, r) = (self.clone(), req.clone());
                Box::pin(async move {
                    let table = e.live_table(tx, &r.info).await?;
                    let idx = r
                        .index
                        .as_ref()
                        .map(|name| {
                            table
                                .indexes
                                .iter()
                                .find(|i| &i.name == name)
                                .cloned()
                                .ok_or_else(|| StorageError::IndexNotFound(name.clone()))
                        })
                        .transpose()?;
                    if idx.as_ref().is_some_and(|i| i.cursor.is_some()) {
                        return Err(StorageError::IndexesInUse(
                            "Cannot read from backfilling global secondary index".into(),
                        ));
                    }
                    let schema = idx
                        .as_ref()
                        .map(|i| &i.schema)
                        .unwrap_or(&table.description.key_schema);
                    let mut prefix = if let Some(i) = &idx {
                        e.key(&["data", &r.info.table_id, "index", &i.id])
                    } else {
                        e.item_prefix(&r.info.table_id)
                    };
                    if let Some(c) = &r.condition {
                        let mut values = Item::new();
                        values.insert(
                            path_name(&c.pk_path, &r.maps)?.into(),
                            value(&c.pk_value, &r.maps)?,
                        );
                        for (p, v) in &c.extra_pk_conditions {
                            values.insert(path_name(p, &r.maps)?.into(), value(v, &r.maps)?);
                        }
                        let hashes: Vec<_> = schema
                            .iter()
                            .filter(|k| k.key_type == KeyType::Hash)
                            .cloned()
                            .collect();
                        codec::item_key(&mut prefix, &values, &hashes)?;
                    }
                    let (mut start, mut end) = if let Some(KeyCondition {
                        sk_condition: Some(SortKeyCondition::Between { low, high, .. }),
                        ..
                    }) = &r.condition
                    {
                        (
                            append(prefix.clone(), low, &r.maps)?,
                            Some(successor(append(prefix, high, &r.maps)?)?),
                        )
                    } else {
                        bounds(
                            prefix,
                            r.condition.as_ref().and_then(|c| c.sk_condition.as_ref()),
                            &r.maps,
                        )?
                    };
                    if let Some(cursor) = &r.start {
                        let key = if let Some(i) = &idx {
                            index::entry_key(
                                &e,
                                &r.info.table_id,
                                i,
                                cursor,
                                &table.description.key_schema,
                            )?
                            .ok_or_else(|| {
                                StorageError::Validation("Incomplete index cursor".into())
                            })?
                        } else {
                            e.item_key(&table.key_info(), cursor)?
                        };
                        if r.forward {
                            let mut after = key;
                            after.push(0);
                            start = start.max(after);
                        } else {
                            end = Some(end.map_or(key.clone(), |b| b.min(key)));
                        }
                    }
                    if end.as_ref().is_some_and(|b| &start >= b) {
                        return Ok((vec![], None));
                    }
                    let mut items = vec![];
                    let wanted = r.limit as usize + 1;
                    loop {
                        let rows = tx
                            .scan(start.clone(), end.clone(), 128, !r.forward)
                            .await
                            .map_err(kv::storage_error)?;
                        if rows.is_empty() {
                            break;
                        }
                        let next = rows.last().unwrap().0.clone();
                        let short = rows.len() < 128;
                        for (_, bytes) in rows {
                            let item: Item = kv::decode(&bytes)?;
                            if let Some((segment, total)) = r.segment {
                                if total <= 0 || segment < 0 || segment >= total {
                                    return Err(StorageError::Validation(
                                        "Invalid scan segment".into(),
                                    ));
                                }
                                let hash_schema: Vec<_> = table
                                    .description
                                    .key_schema
                                    .iter()
                                    .filter(|k| k.key_type == KeyType::Hash)
                                    .cloned()
                                    .collect();
                                let mut key = vec![];
                                codec::item_key(&mut key, &item, &hash_schema)?;
                                if stable_hash(&key) % total as u64 != segment as u64 {
                                    continue;
                                }
                            }
                            if let Some(c) = &r.condition {
                                let mut matches = true;
                                for (p, v) in &c.extra_sk_conditions {
                                    let Some(actual) = item.get(path_name(p, &r.maps)?) else {
                                        matches = false;
                                        break;
                                    };
                                    if codec::scalar(actual)? != codec::scalar(&value(v, &r.maps)?)?
                                    {
                                        matches = false;
                                        break;
                                    }
                                }
                                if !matches {
                                    continue;
                                }
                            }
                            items.push(item);
                            if items.len() == wanted {
                                break;
                            }
                        }
                        if items.len() == wanted || short {
                            break;
                        }
                        if r.forward {
                            start = next;
                            start.push(0);
                        } else {
                            end = Some(next);
                        }
                    }
                    let more = items.len() > r.limit as usize;
                    items.truncate(r.limit as usize);
                    let cursor = if more {
                        let mut schema = table.description.key_schema.clone();
                        if let Some(i) = &idx {
                            schema.extend(i.schema.clone());
                        }
                        Some(extract_key(items.last().unwrap(), &schema))
                    } else {
                        None
                    };
                    Ok((items, cursor))
                })
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn range_inclusive_and_prefix_bounds() {
        let mut m = ExpressionMaps::default();
        m.values.insert("v".into(), AttributeValue::S("a\0".into()));
        let p = codec::tuple(&["partition"]);
        let c = SortKeyCondition::BeginsWith {
            path: vec![],
            prefix: Expr::Placeholder("v".into()),
        };
        let (s, e) = bounds(p.clone(), Some(&c), &m).unwrap();
        let mut inside = p.clone();
        codec::component(&mut inside, b"a\0z");
        let mut outside = p;
        codec::component(&mut outside, b"ab");
        assert!(inside >= s && inside < e.clone().unwrap());
        assert!(outside >= e.unwrap());
    }
}
