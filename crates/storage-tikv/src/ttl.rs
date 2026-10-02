// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned TTL index. Each enable creates a new generation, so old backfills
//! cannot publish candidates for a changed attribute. Values are revalidated
//! transactionally before deletion. Numbers use decimal parsing, including
//! exponent notation; ancient (>5 years) values are ignored like DynamoDB.
use crate::{TikvEngine, codec, kv, table::Table};
use bigdecimal::{BigDecimal, ToPrimitive};
use extenddb_core::types::*;
use extenddb_storage::error::StorageError;
use std::str::FromStr;
pub(crate) fn expiry(item: &Item, attribute: &str) -> Option<i64> {
    let AttributeValue::N(n) = item.get(attribute)? else {
        return None;
    };
    let d = BigDecimal::from_str(n).ok()?;
    if d < 0 {
        return None;
    }
    d.to_i64()
}
pub(crate) fn expired(item: &Item, attribute: &str, now: i64) -> bool {
    let Some(AttributeValue::N(n)) = item.get(attribute) else {
        return false;
    };
    let Ok(n) = BigDecimal::from_str(n) else {
        return false;
    };
    n <= now && n >= now.saturating_sub(5 * 365 * 24 * 3600)
}
pub(crate) fn prefix(e: &TikvEngine, t: &Table) -> Vec<u8> {
    e.key(&["data", &t.description.table_id, "ttl", &t.ttl_generation])
}
pub(crate) fn entry(
    e: &TikvEngine,
    t: &Table,
    item: &Item,
) -> Result<Option<Vec<u8>>, StorageError> {
    let Some(attr) = &t.ttl_attribute else {
        return Ok(None);
    };
    let Some(at) = expiry(item, attr) else {
        return Ok(None);
    };
    let mut k = prefix(e, t);
    k.extend_from_slice(&(at as u64).to_be_bytes());
    codec::item_key(&mut k, item, &t.description.key_schema)?;
    Ok(Some(k))
}
pub(crate) async fn apply(
    e: &TikvEngine,
    tx: &mut dyn kv::Transaction,
    t: &Table,
    old: Option<&Item>,
    new: Option<&Item>,
) -> Result<(), StorageError> {
    for (image, insert) in [(old, false), (new, true)] {
        if let Some(item) = image
            && let Some(k) = entry(e, t, item)?
        {
            if insert {
                kv::put(tx, k, &extract_key(item, &t.description.key_schema)).await?;
            } else {
                kv::delete(tx, k).await?;
            }
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_expiry_and_five_year_rule() {
        let mut i = Item::new();
        i.insert("ttl".into(), AttributeValue::N("1.009E2".into()));
        assert_eq!(expiry(&i, "ttl"), Some(100));
        assert!(!expired(&i, "ttl", 100));
        assert!(expired(&i, "ttl", 101));
        assert!(!expired(&i, "ttl", 200_000_000));
        i.insert("ttl".into(), AttributeValue::S("100".into()));
        assert_eq!(expiry(&i, "ttl"), None);
        i.insert("ttl".into(), AttributeValue::N("-1".into()));
        assert_eq!(expiry(&i, "ttl"), None);
    }
}
