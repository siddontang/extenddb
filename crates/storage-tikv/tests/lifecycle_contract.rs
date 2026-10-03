// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic lifecycle, resumable backfill, TTL, backup and deletion tests.
//! The clock advances explicitly; no sleeps or running server are needed.
#![cfg(feature = "test-support")]
mod common;
use extenddb_core::{
    expression::{self, ExpressionMaps},
    types::*,
};
use extenddb_storage::{
    BackupEngine, DataEngine, MetadataEngine, StreamEngine, TableEngine, error::StorageError,
    management_store::*,
};
use extenddb_storage_tikv::{
    TikvEngine, catalog::TikvCatalog, engine::Clock, kv::memory::MemoryStore,
};
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
fn item(i: usize, g: &str, ttl: &str) -> Item {
    serde_json::from_value(
        serde_json::json!({"pk":{"S":format!("{i:04}")},"g":{"S":g},"expires":{"N":ttl}}),
    )
    .unwrap()
}
async fn lifecycle_contract(e: TikvEngine, clock: Arc<TestClock>) {
    let c = TikvCatalog::new(e.clone());
    let a = "111111111111";
    c.create_account(a, "one").await.unwrap();
    c.set_setting("control_plane_delay_seconds", "1")
        .await
        .unwrap();
    let input=serde_json::from_value(serde_json::json!({"TableName":"items","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST","StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}})).unwrap();
    assert_eq!(
        e.create_table(a, input).await.unwrap().table_status,
        TableStatus::Creating
    );
    assert!(e.table_key_info(a, "items").await.is_err());
    assert!(matches!(
        c.delete_account(a).await,
        Err(OpError::HasDependents(_))
    ));
    e.lifecycle_step().await.unwrap();
    clock.0.fetch_add(1000, Ordering::SeqCst);
    assert_eq!(e.lifecycle_step().await.unwrap().len(), 1);
    let info = e.table_key_info(a, "items").await.unwrap();
    let maps = ExpressionMaps::default();
    for i in 0..130 {
        let mut value = item(i, "old", "999");
        if i == 0 || i == 65 {
            // A future index may target attributes that used to be non-keys.
            value.insert("g".into(), AttributeValue::Bool(true));
        }
        e.put_item(&info, value, false, None, &maps, None)
            .await
            .unwrap();
    }
    let update=serde_json::from_value(serde_json::json!({"TableName":"items","AttributeDefinitions":[{"AttributeName":"g","AttributeType":"S"}],"GlobalSecondaryIndexUpdates":[{"Create":{"IndexName":"gsi","KeySchema":[{"AttributeName":"g","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}}]})).unwrap();
    e.update_table(a, update).await.unwrap();
    e.lifecycle_step().await.unwrap(); // one of three batches
    e.put_item(&info, item(0, "new", "2000"), false, None, &maps, None)
        .await
        .unwrap();
    e.delete_item(
        &info,
        &extract_key(&item(65, "old", "999"), &info.key_schema),
        false,
        None,
        &maps,
        None,
    )
    .await
    .unwrap();
    e.lifecycle_step().await.unwrap();
    e.lifecycle_step().await.unwrap();
    let d = e
        .describe_table(
            a,
            DescribeTableInput {
                table_name: "items".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        d.global_secondary_indexes.as_ref().unwrap()[0].index_status,
        "ACTIVE"
    );
    let mut idx = e.table_key_info(a, "items").await.unwrap();
    idx.key_schema = idx.global_secondary_indexes[0].key_schema.clone();
    let key = expression::parse_key_condition(&expression::tokenize("g = :g").unwrap()).unwrap();
    let mut m = ExpressionMaps::default();
    m.values.insert("g".into(), AttributeValue::S("old".into()));
    let (rows, _) = e
        .query(&idx, &key, &m, true, Some(256), None, Some("gsi"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 128);
    assert!(
        !rows
            .iter()
            .any(|i| i["pk"] == AttributeValue::S("0000".into())
                || i["pk"] == AttributeValue::S("0065".into()))
    );
    e.tag_resource(
        &d.table_arn,
        &[Tag {
            key: "owner".into(),
            value: "test".into(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(e.list_tags(&d.table_arn).await.unwrap().len(), 1);
    e.untag_resource(&d.table_arn, &["owner".into()])
        .await
        .unwrap();
    assert!(e.list_tags(&d.table_arn).await.unwrap().is_empty());
    e.refresh_table_size(a, "items").await.unwrap();
    assert_eq!(
        e.describe_table(
            a,
            DescribeTableInput {
                table_name: "items".into()
            }
        )
        .await
        .unwrap()
        .item_count,
        129
    );
    let backup = e.create_backup(a, "items", "before-ttl").await.unwrap();
    assert!(
        e.describe_backup("222222222222", &backup.backup_arn)
            .await
            .is_err()
    );
    assert!(
        e.delete_backup("222222222222", &backup.backup_arn)
            .await
            .is_err()
    );
    let restored = e
        .restore_table_from_backup(a, "restored", &backup.backup_arn)
        .await
        .unwrap();
    assert_ne!(restored.table_id, d.table_id);
    assert_eq!(restored.table_status, TableStatus::Creating);
    assert!(e.table_key_info(a, "restored").await.is_err());
    clock.0.fetch_add(1000, Ordering::SeqCst);
    e.lifecycle_step().await.unwrap();
    let restored_info = e.table_key_info(a, "restored").await.unwrap();
    assert_eq!(
        e.scan(&restored_info, Some(256), None, None, None, None)
            .await
            .unwrap()
            .0
            .len(),
        129
    );
    assert!(matches!(
        e.restore_table_from_backup(a, "restored", &backup.backup_arn)
            .await,
        Err(StorageError::TableAlreadyExists(_))
    ));
    e.update_ttl(a, "items", "expires", true).await.unwrap();
    for _ in 0..3 {
        e.lifecycle_step().await.unwrap();
    }
    assert_eq!(e.all_tables_with_ttl_index_ready().await.unwrap().len(), 1);
    assert_eq!(e.ttl_step().await.unwrap(), 64);
    assert_eq!(e.ttl_step().await.unwrap(), 64);
    assert_eq!(e.ttl_step().await.unwrap(), 0);
    assert_eq!(
        e.scan(&info, Some(256), None, None, None, None)
            .await
            .unwrap()
            .0
            .len(),
        1
    );
    let stream = e
        .describe_stream(
            a,
            &DescribeStreamInput {
                stream_arn: d.latest_stream_arn.clone().unwrap(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    let mut ttl_events = 0;
    for shard in stream.shards {
        for record in e
            .get_stream_records(a, &shard.shard_id, None, 1000)
            .await
            .unwrap()
            .0
        {
            if record
                .user_identity
                .as_ref()
                .is_some_and(|u| u.principal_id == "dynamodb.amazonaws.com")
            {
                ttl_events += 1;
            }
        }
    }
    assert_eq!(ttl_events, 128);
    assert!(e.update_ttl(a, "items", "different", true).await.is_err());
    e.update_ttl(a, "items", "expires", false).await.unwrap();
    assert!(e.all_tables_with_ttl().await.unwrap().is_empty());
    e.delete_backup(a, &backup.backup_arn).await.unwrap();
    assert!(e.list_backups(a, None).await.unwrap().is_empty());
    let retained_backup = e.create_backup(a, "restored", "retained").await.unwrap();
    assert!(e.update_continuous_backups(a, "items", true).await.is_err());
    for name in ["items", "restored"] {
        e.delete_table(
            a,
            DeleteTableInput {
                table_name: name.into(),
            },
        )
        .await
        .unwrap();
    }
    assert!(
        e.get_item(
            &info,
            &extract_key(&item(0, "new", "2000"), &info.key_schema)
        )
        .await
        .is_err()
    );
    for _ in 0..10 {
        e.lifecycle_step().await.unwrap();
    }
    assert!(
        e.list_tables(
            a,
            ListTablesInput {
                limit: None,
                exclusive_start_table_name: None
            }
        )
        .await
        .unwrap()
        .table_names
        .is_empty()
    );
    c.delete_account(a).await.unwrap();
    c.create_account(a, "recreated").await.unwrap();
    assert!(e.list_backups(a, None).await.unwrap().is_empty());
    assert!(
        e.describe_backup(a, &retained_backup.backup_arn)
            .await
            .is_err()
    );
    assert!(
        e.restore_table_from_backup(a, "stolen", &retained_backup.backup_arn)
            .await
            .is_err()
    );
    assert!(
        e.describe_stream(
            a,
            &DescribeStreamInput {
                stream_arn: d.latest_stream_arn.unwrap(),
                limit: None,
                exclusive_start_shard_id: None
            }
        )
        .await
        .is_err()
    );
    assert!(
        e.get_item(
            &info,
            &extract_key(&item(0, "new", "2000"), &info.key_schema)
        )
        .await
        .is_err()
    );
    c.delete_account(a).await.unwrap();
}

#[tokio::test]
async fn resumable_backfill_ttl_backup_and_delete() {
    let clock = Arc::new(TestClock(AtomicI64::new(1_000_000)));
    let e = TikvEngine::new(Arc::new(MemoryStore::default()), "life", "us-east-1")
        .unwrap()
        .with_clock(clock.clone());
    lifecycle_contract(e, clock).await;
}

#[tokio::test]
async fn chunked_backup_restores_above_the_old_single_value_limit() {
    let clock = Arc::new(TestClock(AtomicI64::new(1_000_000)));
    let e = TikvEngine::new(
        Arc::new(MemoryStore::default()),
        "backup_limit",
        "us-east-1",
    )
    .unwrap()
    .with_clock(clock.clone());
    let account = "111111111111";
    TikvCatalog::new(e.clone())
        .create_account(account, "backup")
        .await
        .unwrap();
    let input = serde_json::from_value(serde_json::json!({
        "TableName":"large", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
        "BillingMode":"PAY_PER_REQUEST"
    }))
    .unwrap();
    e.create_table(account, input).await.unwrap();
    let info = e.table_key_info(account, "large").await.unwrap();
    let maps = ExpressionMaps::default();
    for i in 0..12 {
        let mut value = Item::new();
        value.insert("pk".into(), AttributeValue::S(i.to_string()));
        value.insert("payload".into(), AttributeValue::S("x".repeat(380 * 1024)));
        e.put_item(&info, value, false, None, &maps, None)
            .await
            .unwrap();
    }
    let backup = e.create_backup(account, "large", "chunked").await.unwrap();
    assert!(backup.backup_size_bytes > 4 * 1024 * 1024);
    // Mutating the source after the snapshot must not alter its restored data.
    let changed =
        serde_json::from_value(serde_json::json!({"pk":{"S":"0"},"payload":{"S":"changed"}}))
            .unwrap();
    e.put_item(&info, changed, false, None, &maps, None)
        .await
        .unwrap();
    let restored = e
        .restore_table_from_backup(account, "restored_large", &backup.backup_arn)
        .await
        .unwrap();
    assert_eq!(restored.table_status, TableStatus::Creating);
    clock.0.fetch_add(5, Ordering::SeqCst);
    e.lifecycle_step().await.unwrap();
    let restored = e.table_key_info(account, "restored_large").await.unwrap();
    for i in 0..12 {
        let key = serde_json::from_value(serde_json::json!({"pk":{"S":i.to_string()}})).unwrap();
        let row = e.get_item(&restored, &key).await.unwrap().unwrap();
        assert_eq!(row["payload"], AttributeValue::S("x".repeat(380 * 1024)));
    }
    e.delete_backup(account, &backup.backup_arn).await.unwrap();
    assert!(e.list_backups(account, None).await.unwrap().is_empty());
    assert_eq!(
        e.scan(&info, Some(100), None, None, None, None)
            .await
            .unwrap()
            .0
            .len(),
        12
    );
}

#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires dedicated TiKV/PD cluster; set TIKV_PD_ENDPOINTS"]
async fn real_lifecycle_contract() {
    common::real_contract(|e| async move {
        let clock = Arc::new(TestClock(AtomicI64::new(1_000_000)));
        lifecycle_contract(e.with_clock(clock.clone()), clock).await;
    })
    .await;
}
