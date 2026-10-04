// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Identical vector semantics on memory MVCC and TiKV: lifecycle, projections,
//! scoped exact ranking, mutation races, transactional rollback and restoration.
#![cfg(feature = "test-support")]
mod common;
use extenddb_core::{expression::ExpressionMaps, types::*};
use extenddb_storage::{
    BackupEngine, DataEngine, TableEngine, VectorSearch, VectorSearchEngine,
    management_store::ManagementStore,
    vector_lifecycle::{VectorIndexBuild, run_backfill},
};
use extenddb_storage_tikv::{
    TikvEngine, catalog::TikvCatalog, kv::memory::MemoryStore, vector::build::Build,
};
use std::{sync::Arc, time::Duration};
const ACCOUNT: &str = "111111111111";
fn spec(name: &str) -> VectorIndexSpecification {
    serde_json::from_value(serde_json::json!({"IndexName":name,"Dimensions":2,"DistanceFunction":"EUCLIDEAN","VectorAttribute":{"AttributeName":"embedding"},"SearchSchema":[{"AttributeName":"group","SearchSchemaElementType":"HASH"},{"AttributeName":"color","SearchSchemaElementType":"INLINE_FILTER"}],"Projection":{"ProjectionType":"KEYS_ONLY"}})).unwrap()
}
fn item(i: usize) -> Item {
    serde_json::from_value(serde_json::json!({"pk":{"S":format!("{i:04}")},"embedding":{"L":[{"N":i.to_string()},{"N":"0"}]},"group":{"S":"one"},"color":{"S":if i.is_multiple_of(2) {"red"}else{"blue"}},"private":{"S":"not projected"}})).unwrap()
}
async fn search(
    e: &TikvEngine,
    info: &TableKeyInfo,
    index: &str,
) -> extenddb_storage::VectorSearchResult {
    let group = AttributeValue::S("one".into());
    let color = AttributeValue::S("red".into());
    e.search_vectors(VectorSearch {
        key_info: info,
        index_name: index,
        query_vector: &[0., 0.],
        top_k: 3,
        hash_key: Some(("group", &group)),
        filters: &[("color", &color)],
    })
    .await
}
async fn contract(e: TikvEngine) {
    TikvCatalog::new(e.clone())
        .create_account(ACCOUNT, "vectors")
        .await
        .unwrap();
    let input=serde_json::from_value(serde_json::json!({"TableName":"vectors","BillingMode":"PAY_PER_REQUEST","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"group","AttributeType":"S"},{"AttributeName":"color","AttributeType":"S"}],"VectorIndexes":[spec("initial")]})).unwrap();
    let created = e.create_table(ACCOUNT, input).await.unwrap();
    assert_eq!(
        created.vector_indexes.unwrap()[0].index_status,
        IndexStatus::Active
    );
    let info = e.table_key_info(ACCOUNT, "vectors").await.unwrap();
    let maps = ExpressionMaps::default();
    for i in 0..130 {
        e.put_item(&info, item(i), false, None, &maps, None)
            .await
            .unwrap();
    }
    let hits = search(&e, &info, "initial").await.unwrap().hits;
    assert_eq!(
        hits.iter().map(|h| h.score).collect::<Vec<_>>(),
        [0., 2., 4.]
    );
    assert!(!hits[0].item.contains_key("private"));
    assert!(hits[0].item.contains_key("embedding"));
    let mut forged = info.clone();
    forged.account_id = "222222222222".into();
    assert!(search(&e, &forged, "initial").await.is_err());
    let update = serde_json::from_value(
        serde_json::json!({"TableName":"vectors","VectorIndexUpdates":[{"Create":spec("online")}]}),
    )
    .unwrap();
    let desc = e.update_table(ACCOUNT, update).await.unwrap();
    assert_eq!(desc.vector_indexes.unwrap()[1].backfilling, Some(false));
    assert!(search(&e, &info, "online").await.is_err());
    let mut build = Build::load(e.clone(), ACCOUNT, "vectors", "online")
        .await
        .unwrap();
    build.set_backfilling().await.unwrap();
    assert!(build.mark_active(0).await.is_err());
    assert_eq!(build.backfill_batch(None, 64).await.unwrap().fetched, 64);
    let mut changed = item(0);
    changed.insert("group".into(), AttributeValue::S("other".into()));
    e.put_item(&info, changed, false, None, &maps, None)
        .await
        .unwrap();
    e.delete_item(
        &info,
        &extract_key(&item(64), &info.key_schema),
        false,
        None,
        &maps,
        None,
    )
    .await
    .unwrap();
    // Reconstruct the worker: persistent cursor and skipped count survive restart.
    let mut resumed = Build::load(e.clone(), ACCOUNT, "vectors", "online")
        .await
        .unwrap();
    let result = run_backfill(&mut resumed, 64, Duration::ZERO)
        .await
        .unwrap();
    resumed.mark_active(result.skipped).await.unwrap();
    assert_eq!(
        search(&e, &info, "online")
            .await
            .unwrap()
            .hits
            .iter()
            .map(|h| h.score)
            .collect::<Vec<_>>(),
        [2., 4., 6.]
    );
    let backup = e
        .create_backup(ACCOUNT, "vectors", "vector-backup")
        .await
        .unwrap();
    e.restore_table_from_backup(ACCOUNT, "restored", &backup.backup_arn)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    e.lifecycle_step().await.unwrap();
    let restored = e.table_key_info(ACCOUNT, "restored").await.unwrap();
    assert_eq!(search(&e, &restored, "online").await.unwrap().hits.len(), 3);
    // Old build handles cannot resurrect a deleted/recreated generation.
    let delete=serde_json::from_value(serde_json::json!({"TableName":"vectors","VectorIndexUpdates":[{"Delete":{"IndexName":"online"}}]})).unwrap();
    e.update_table(ACCOUNT, delete).await.unwrap();
    assert!(resumed.mark_active(0).await.is_err());
    assert!(search(&e, &info, "online").await.is_err());
    let info = e.table_key_info(ACCOUNT, "vectors").await.unwrap();
    assert_eq!(info.vector_indexes.len(), 1);
    assert!(
        info.attribute_definitions
            .iter()
            .any(|d| d.attribute_name == "group")
    );
    let mut malformed = item(1);
    malformed.insert("embedding".into(), AttributeValue::L(vec![]));
    assert!(
        e.put_item(&info, malformed, false, None, &maps, None)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn memory_vectors() {
    contract(TikvEngine::new(Arc::new(MemoryStore::default()), "vectors", "us-east-1").unwrap())
        .await;
}
#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires TiKV/PD and TIKV_PD_ENDPOINTS"]
async fn real_vectors() {
    common::real_contract(contract).await;
}
