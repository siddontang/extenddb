// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL contracts independent of external suites and wall-clock scheduling.
//!
//! Public behavior source: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
//! Non-numeric timestamps are ignored, renewed/removed timestamps preserve the
//! item, and deletions remove secondary-index entries and emit service records.
//! These are storage invariants, not AWS timing or exact-error golden tests.
//! The same cases run on the deterministic MVCC store and a real TiKV namespace.
#![cfg(feature = "test-support")]
mod common;

use extenddb_core::{expression::ExpressionMaps, types::*};
use extenddb_storage::{
    DataEngine, MetadataEngine, StreamEngine, TableEngine, management_store::ManagementStore,
};
use extenddb_storage_tikv::{
    TikvEngine, catalog::TikvCatalog, engine::Clock, kv::memory::MemoryStore,
};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

const ACCOUNT: &str = "111111111111";
const NOW: i64 = 1_800_000_000;
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst) * 1000
    }
}

fn clock() -> Arc<TestClock> {
    Arc::new(TestClock(AtomicI64::new(NOW)))
}
fn memory() -> TikvEngine {
    TikvEngine::new(
        Arc::new(MemoryStore::default()),
        "ttl_contract",
        "us-east-1",
    )
    .unwrap()
}
fn number(n: i64) -> AttributeValue {
    AttributeValue::N(n.to_string())
}
fn item(id: &str, expires: Option<AttributeValue>) -> Item {
    let mut item: Item = serde_json::from_value(serde_json::json!({
        "pk": {"S": "partition"}, "sk": {"S": id},
        "g": {"S": "group"}, "l": {"S": id}
    }))
    .unwrap();
    if let Some(expires) = expires {
        item.insert("expires".into(), expires);
    }
    item
}
async fn setup(e: &TikvEngine) -> TableDescription {
    TikvCatalog::new(e.clone())
        .create_account(ACCOUNT, "ttl")
        .await
        .unwrap();
    e.create_table(ACCOUNT, serde_json::from_value(serde_json::json!({
        "TableName": "items", "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
        "AttributeDefinitions": [
            {"AttributeName":"pk","AttributeType":"S"}, {"AttributeName":"sk","AttributeType":"S"},
            {"AttributeName":"g","AttributeType":"S"}, {"AttributeName":"l","AttributeType":"S"}
        ],
        "GlobalSecondaryIndexes": [{"IndexName":"gsi","KeySchema":[{"AttributeName":"g","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}],
        "LocalSecondaryIndexes": [{"IndexName":"lsi","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"l","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}],
        "StreamSpecification": {"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap()
}
async fn put(e: &TikvEngine, value: Item) {
    let info = e.table_key_info(ACCOUNT, "items").await.unwrap();
    e.put_item(&info, value, false, None, &ExpressionMaps::default(), None)
        .await
        .unwrap();
}
async fn enable(e: &TikvEngine, attribute: &str) {
    e.update_ttl(ACCOUNT, "items", attribute, true)
        .await
        .unwrap();
    // Each fixture contains fewer than one backfill batch of items.
    e.lifecycle_step().await.unwrap();
    assert_eq!(e.all_tables_with_ttl_index_ready().await.unwrap().len(), 1);
}
async fn ids(e: &TikvEngine, index: Option<&str>) -> BTreeSet<String> {
    let mut info = e.table_key_info(ACCOUNT, "items").await.unwrap();
    if let Some(name) = index {
        info.key_schema = if name == "gsi" {
            info.global_secondary_indexes[0].key_schema.clone()
        } else {
            info.local_secondary_indexes[0].key_schema.clone()
        };
    }
    let (rows, cursor) = e
        .scan(&info, Some(100), None, None, None, index)
        .await
        .unwrap();
    assert!(cursor.is_none());
    rows.into_iter()
        .map(|row| match &row["sk"] {
            AttributeValue::S(s) => s.clone(),
            other => panic!("unexpected key {other:?}"),
        })
        .collect()
}
async fn assert_ids(e: &TikvEngine, expected: &[&str]) {
    let expected: BTreeSet<_> = expected.iter().map(|s| s.to_string()).collect();
    for index in [None, Some("gsi"), Some("lsi")] {
        assert_eq!(ids(e, index).await, expected, "index {index:?}");
    }
}

async fn delete_contract(e: TikvEngine) {
    let e = e.with_clock(clock());
    let table = setup(&e).await;
    for (id, expiry) in [
        ("expired", Some(number(NOW - 10))),
        ("future", Some(number(NOW + 100))),
        ("ancient", Some(number(NOW - 6 * 365 * 24 * 3600))),
        ("string", Some(AttributeValue::S((NOW - 10).to_string()))),
        ("boolean", Some(AttributeValue::Bool(true))),
        ("missing", None),
    ] {
        put(&e, item(id, expiry)).await;
    }
    enable(&e, "expires").await;
    assert_eq!(e.ttl_step().await.unwrap(), 1);
    assert_eq!(
        e.ttl_step().await.unwrap(),
        0,
        "deletion must not be replayed"
    );
    assert_ids(&e, &["future", "ancient", "string", "boolean", "missing"]).await;
    let stream = e
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn: table.latest_stream_arn.unwrap(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    let mut records = Vec::new();
    for shard in stream.shards {
        records.extend(
            e.get_stream_records(ACCOUNT, &shard.shard_id, None, 100)
                .await
                .unwrap()
                .0,
        );
    }
    assert_eq!(records.len(), 7, "six inserts and exactly one TTL deletion");
    let removals: Vec<_> = records
        .iter()
        .filter(|record| record.event_name == StreamEventName::Remove)
        .collect();
    assert_eq!(removals.len(), 1);
    let record = removals[0];
    assert_eq!(record.event_name, StreamEventName::Remove);
    assert_eq!(
        record.user_identity.as_ref().unwrap().principal_id,
        "dynamodb.amazonaws.com"
    );
    assert_eq!(
        record.dynamodb.old_image.as_ref().unwrap()["sk"],
        AttributeValue::S("expired".into())
    );
    assert!(record.dynamodb.new_image.is_none());
}

async fn renewal_contract(e: TikvEngine) {
    let clock = clock();
    let e = e.with_clock(clock.clone());
    setup(&e).await;
    for id in ["renewed", "removed"] {
        put(&e, item(id, Some(number(NOW - 10)))).await;
    }
    enable(&e, "expires").await;
    put(&e, item("renewed", Some(number(NOW + 100)))).await;
    put(&e, item("removed", None)).await;
    assert_eq!(e.ttl_step().await.unwrap(), 0);
    assert_ids(&e, &["renewed", "removed"]).await;
    clock.0.store(NOW + 101, Ordering::SeqCst);
    assert_eq!(e.ttl_step().await.unwrap(), 1);
    assert_ids(&e, &["removed"]).await;
}

async fn generation_contract(e: TikvEngine) {
    let clock = clock();
    let e = e.with_clock(clock.clone());
    setup(&e).await;
    let mut value = item("switched", Some(number(NOW - 10)));
    value.insert("next_expiry".into(), number(NOW + 100));
    put(&e, value).await;
    enable(&e, "expires").await;
    e.update_ttl(ACCOUNT, "items", "expires", false)
        .await
        .unwrap();
    assert_eq!(e.ttl_step().await.unwrap(), 0);
    assert_ids(&e, &["switched"]).await;
    enable(&e, "next_expiry").await;
    assert_eq!(
        e.ttl_step().await.unwrap(),
        0,
        "the old generation must be ignored"
    );
    assert_ids(&e, &["switched"]).await;
    clock.0.store(NOW + 101, Ordering::SeqCst);
    assert_eq!(e.ttl_step().await.unwrap(), 1);
    assert_ids(&e, &[]).await;
}

#[tokio::test]
async fn expiry_removes_secondary_indexes_and_emits_one_service_record() {
    delete_contract(memory()).await;
}
#[tokio::test]
async fn renewal_and_attribute_removal_preserve_items_and_indexes() {
    renewal_contract(memory()).await;
}
#[tokio::test]
async fn changing_ttl_attribute_isolates_old_generation() {
    generation_contract(memory()).await;
}
#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires TIKV_PD_ENDPOINTS"]
async fn real_tikv_ttl_contracts() {
    common::real_contract(delete_contract).await;
    common::real_contract(renewal_contract).await;
    common::real_contract(generation_contract).await;
}
