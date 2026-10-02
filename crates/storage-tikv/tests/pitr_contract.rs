// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Recovery contracts use committed MVCC history, not synthetic current-state
//! copies. The same public storage API runs on reference and real TiKV stores.
#![cfg(feature = "test-support")]
mod common;
use extenddb_core::{expression::ExpressionMaps, types::*};
use extenddb_storage::{management_store::ManagementStore, *};
use extenddb_storage_tikv::{
    TikvEngine,
    catalog::TikvCatalog,
    kv::{Store, memory::MemoryStore},
};
use std::sync::Arc;
fn item(pk: &str, value: &str) -> Item {
    Item::from_iter([
        ("pk".into(), AttributeValue::S(pk.into())),
        ("value".into(), AttributeValue::S(value.into())),
    ])
}
async fn advance(store: &Option<Arc<MemoryStore>>) {
    if let Some(s) = store {
        s.advance_time(100);
    } else {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
async fn contract(e: TikvEngine, store: Option<Arc<MemoryStore>>) {
    let c = TikvCatalog::new(e.clone());
    c.create_account("a", "a").await.unwrap();
    c.create_account("b", "b").await.unwrap();
    e.create_table("a",serde_json::from_value(serde_json::json!({"TableName":"history","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"})).unwrap()).await.unwrap();
    let info = e.table_key_info("a", "history").await.unwrap();
    let maps = ExpressionMaps::default();
    assert!(
        e.restore_table_at("a", "history", "disabled", RestorePoint::Latest)
            .await
            .is_err()
    );
    e.update_continuous_backups("a", "history", true)
        .await
        .unwrap();
    for i in 0..20 {
        e.put_item(
            &info,
            item(&format!("{i:02}"), "before"),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    }
    let before = e
        .describe_continuous_backups("a", "history")
        .await
        .unwrap()
        .point_in_time_recovery_description
        .unwrap()
        .latest_restorable_date_time
        .unwrap();
    advance(&store).await;
    e.put_item(&info, item("00", "after"), false, None, &maps, None)
        .await
        .unwrap();
    let deleted = Item::from_iter([("pk".into(), AttributeValue::S("19".into()))]);
    e.delete_item(&info, &deleted, false, None, &maps, None)
        .await
        .unwrap();
    e.put_item(&info, item("99", "new"), false, None, &maps, None)
        .await
        .unwrap();
    assert!(
        e.restore_table_at("b", "history", "forbidden", RestorePoint::Timestamp(before))
            .await
            .is_err()
    );
    assert!(
        e.restore_table_at(
            "a",
            "history",
            "future",
            RestorePoint::Timestamp(before + 3600.)
        )
        .await
        .is_err()
    );
    assert!(
        e.restore_table_at(
            "a",
            "history",
            "early",
            RestorePoint::Timestamp(before - 3600.)
        )
        .await
        .is_err()
    );
    // Restore uses the source's current index settings, even when the data
    // timestamp predates creation of that index.
    e.update_table("a",serde_json::from_value(serde_json::json!({"TableName":"history","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"value","AttributeType":"S"}],"GlobalSecondaryIndexUpdates":[{"Create":{"IndexName":"by-value","KeySchema":[{"AttributeName":"value","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}}]})).unwrap()).await.unwrap();
    e.lifecycle_step().await.unwrap();
    let old = e
        .restore_table_at("a", "history", "old", RestorePoint::Timestamp(before))
        .await
        .unwrap();
    assert_eq!(old.item_count, 20);
    let oi = e.table_key_info("a", "old").await.unwrap();
    assert_eq!(oi.global_secondary_indexes.len(), 1);
    assert_eq!(
        e.scan(&oi, None, None, None, None, Some("by-value"))
            .await
            .unwrap()
            .0
            .len(),
        20
    );
    assert_eq!(
        e.get_item(&oi, &item("00", "ignored"))
            .await
            .unwrap()
            .unwrap(),
        item("00", "before")
    );
    assert!(e.get_item(&oi, &deleted).await.unwrap().is_some());
    assert!(
        e.get_item(&oi, &item("99", "ignored"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        e.restore_table_at("a", "history", "old", RestorePoint::Latest)
            .await
            .is_err()
    );
    e.restore_table_at("a", "history", "latest", RestorePoint::Latest)
        .await
        .unwrap();
    let li = e.table_key_info("a", "latest").await.unwrap();
    assert_eq!(
        e.get_item(&li, &item("00", "ignored"))
            .await
            .unwrap()
            .unwrap(),
        item("00", "after")
    );
    assert!(e.get_item(&li, &deleted).await.unwrap().is_none());
    if let Some(store) = &store {
        advance(&Some(store.clone())).await;
        let gc = store.timestamp().await.unwrap();
        store.collect_history_before(gc);
        assert!(
            e.restore_table_at("a", "history", "collected", RestorePoint::Timestamp(before))
                .await
                .is_err()
        );
        assert!(e.table_key_info("a", "collected").await.is_err());
        store.advance_time(36 * 86_400_000);
        e.pitr_step().await.unwrap();
        let window = e
            .describe_continuous_backups("a", "history")
            .await
            .unwrap()
            .point_in_time_recovery_description
            .unwrap();
        assert!(window.earliest_restorable_date_time.unwrap() > before);
    }
    e.update_continuous_backups("a", "history", false)
        .await
        .unwrap();
    assert_eq!(
        e.describe_continuous_backups("a", "history")
            .await
            .unwrap()
            .continuous_backups_status,
        "DISABLED"
    );
    assert!(
        e.restore_table_at("a", "history", "stopped", RestorePoint::Latest)
            .await
            .is_err()
    );
    advance(&store).await;
    e.update_continuous_backups("a", "history", true)
        .await
        .unwrap();
    assert!(
        e.restore_table_at(
            "a",
            "history",
            "previous-window",
            RestorePoint::Timestamp(before)
        )
        .await
        .is_err()
    );
    e.update_continuous_backups("a", "history", false)
        .await
        .unwrap();
}
#[tokio::test]
async fn memory_pitr() {
    let s = Arc::new(MemoryStore::default());
    s.advance_time(1_000_000);
    contract(
        TikvEngine::new(s.clone(), "pitr", "us-east-1").unwrap(),
        Some(s),
    )
    .await;
}
#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires dedicated TiKV/PD cluster; set TIKV_PD_ENDPOINTS"]
async fn real_pitr() {
    common::real_contract(|e| contract(e, None)).await;
}

#[tokio::test]
async fn unknown_enable_is_reconciled_without_losing_its_history() {
    let store = Arc::new(MemoryStore::default());
    store.advance_time(1_000_000);
    let e = TikvEngine::new(store.clone(), "pitr-unknown", "us-east-1").unwrap();
    TikvCatalog::new(e.clone())
        .create_account("a", "a")
        .await
        .unwrap();
    e.create_table("a",serde_json::from_value(serde_json::json!({"TableName":"history","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"})).unwrap()).await.unwrap();
    store.inject_unknown_commits(1);
    assert!(
        e.update_continuous_backups("a", "history", true)
            .await
            .is_err()
    );
    e.pitr_step().await.unwrap();
    assert_eq!(
        e.describe_continuous_backups("a", "history")
            .await
            .unwrap()
            .continuous_backups_status,
        "ENABLED"
    );
    e.update_continuous_backups("a", "history", false)
        .await
        .unwrap();
}
