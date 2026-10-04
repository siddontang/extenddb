// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Identical data-path assertions against reference MVCC and real TiKV.
//! Run the ignored test explicitly with `TIKV_PD_ENDPOINTS` set. A missing
//! endpoint is a failure, never a silently skipped real-cluster check.
#![cfg(feature = "test-support")]
mod common;
use extenddb_core::{
    expression::{self, ExpressionMaps},
    types::*,
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, StreamEngine, TableEngine, TransactWriteOp, error::StorageError,
};
use extenddb_storage_tikv::{TikvEngine, kv::memory::MemoryStore};
use std::sync::Arc;

fn item(pk: &str, sk: &str) -> Item {
    serde_json::from_value(serde_json::json!({"pk":{"S":pk},"sk":{"N":sk},"g":{"S":"group"}}))
        .unwrap()
}
async fn setup(e: &TikvEngine) -> TableKeyInfo {
    use extenddb_storage::management_store::ManagementStore;
    extenddb_storage_tikv::catalog::TikvCatalog::new(e.clone())
        .create_account("111111111111", "data-test")
        .await
        .unwrap();
    let input=serde_json::from_value(serde_json::json!({"TableName":"items","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"},{"AttributeName":"g","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST","GlobalSecondaryIndexes":[{"IndexName":"gsi","KeySchema":[{"AttributeName":"g","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}],"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}})).unwrap();
    e.create_table("111111111111", input).await.unwrap();
    e.table_key_info("111111111111", "items").await.unwrap()
}
fn condition(text: &str) -> expression::Expr {
    expression::parse_condition(&expression::tokenize(text).unwrap()).unwrap()
}
fn key_condition(pk_name: &str) -> (expression::KeyCondition, ExpressionMaps) {
    let c =
        expression::parse_key_condition(&expression::tokenize(&format!("{pk_name} = :p")).unwrap())
            .unwrap();
    let mut m = ExpressionMaps::default();
    m.values.insert(
        "p".into(),
        AttributeValue::S(if pk_name == "g" { "group" } else { "a" }.into()),
    );
    (c, m)
}
async fn contract(e: TikvEngine) {
    let info = setup(&e).await;
    let maps = ExpressionMaps::default();
    for (pk, sk) in [("a", "-10"), ("a", "2"), ("a", "10"), ("b", "2")] {
        e.put_item(&info, item(pk, sk), false, None, &maps, None)
            .await
            .unwrap();
    }
    let mut key = item("a", "2.000");
    key.remove("g");
    assert!(e.get_item(&info, &key).await.unwrap().is_some());
    let (c, m) = key_condition("pk");
    let (page, cursor) = e
        .query(&info, &c, &m, true, Some(2), None, None)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0]["sk"], AttributeValue::N("-10".into()));
    let (last, end) = e
        .query(&info, &c, &m, true, Some(2), cursor.as_ref(), None)
        .await
        .unwrap();
    assert_eq!(last.len(), 1);
    assert!(end.is_none());
    assert_eq!(last[0]["sk"], AttributeValue::N("10".into()));
    let (reverse, _) = e
        .query(&info, &c, &m, false, Some(10), None, None)
        .await
        .unwrap();
    let mut all = page;
    all.extend(last);
    all.reverse();
    assert_eq!(all, reverse);
    let mut index_info = info.clone();
    index_info.key_schema = info.global_secondary_indexes[0].key_schema.clone();
    let (c, m) = key_condition("g");
    let mut cursor = None;
    let mut indexed = vec![];
    loop {
        let (mut rows, next) = e
            .query(
                &index_info,
                &c,
                &m,
                true,
                Some(1),
                cursor.as_ref(),
                Some("gsi"),
            )
            .await
            .unwrap();
        indexed.append(&mut rows);
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(indexed.len(), 4);
    assert_eq!(
        indexed
            .iter()
            .filter(|i| i["sk"] == AttributeValue::N("2".into()))
            .count(),
        2
    );
    let fail = condition("attribute_not_exists(pk)");
    let new = item("a", "99");
    let existing = item("a", "2");
    let ops = [
        TransactWriteOp::Put {
            key_info: &info,
            item: &new,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::Put {
            key_info: &info,
            item: &existing,
            condition: Some(&fail),
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::AllOld,
            stream: None,
        },
    ];
    assert!(matches!(
        e.transact_write_items(&ops, None).await,
        Err(StorageError::TransactionCanceled(_))
    ));
    assert!(e.get_item(&info, &new).await.unwrap().is_none());
    let ops = &ops[..1];
    e.transact_write_items(
        ops,
        Some(IdempotencyKey {
            account_id: "111111111111",
            token: "once",
            fingerprint: "payload",
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        e.transact_write_items(
            ops,
            Some(IdempotencyKey {
                account_id: "111111111111",
                token: "once",
                fingerprint: "payload"
            })
        )
        .await,
        Err(StorageError::IdempotentReplay)
    ));
    assert!(matches!(
        e.transact_write_items(
            ops,
            Some(IdempotencyKey {
                account_id: "111111111111",
                token: "once",
                fingerprint: "different"
            })
        )
        .await,
        Err(StorageError::IdempotentMismatch)
    ));
    let desc = e
        .describe_table(
            "111111111111",
            DescribeTableInput {
                table_name: "items".into(),
            },
        )
        .await
        .unwrap();
    let stream = e
        .describe_stream(
            "111111111111",
            &DescribeStreamInput {
                stream_arn: desc.latest_stream_arn.unwrap(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    let mut records = vec![];
    for shard in stream.shards {
        let (mut r, _) = e
            .get_stream_records("111111111111", &shard.shard_id, None, 100)
            .await
            .unwrap();
        assert!(
            e.get_stream_records("222222222222", &shard.shard_id, None, 100)
                .await
                .is_err()
        );
        assert!(
            r.windows(2)
                .all(|w| w[0].dynamodb.sequence_number < w[1].dynamodb.sequence_number)
        );
        records.append(&mut r);
    }
    assert_eq!(
        records.len(),
        5,
        "failed and replayed transactions cannot emit records"
    );
    let mut forged = info.clone();
    forged.account_id = "222222222222".into();
    assert!(e.get_item(&forged, &key).await.is_err());
    let mut scanned = vec![];
    for segment in 0..3 {
        let (rows, _) = e
            .scan(&info, Some(100), None, Some(segment), Some(3), None)
            .await
            .unwrap();
        scanned.extend(rows);
    }
    assert_eq!(scanned.len(), 5);
    // The same cursor succeeds in exactly one segment, even if the cursor
    // item has since been deleted. No network lookup defines membership.
    let cursor = item("cursor-not-stored", "7");
    let mut accepted = 0;
    for segment in 0..3 {
        match e
            .scan(&info, Some(1), Some(&cursor), Some(segment), Some(3), None)
            .await
        {
            Ok(_) => accepted += 1,
            Err(StorageError::Validation(message)) => {
                assert!(message.contains("ExclusiveStartKey"))
            }
            other => panic!("unexpected segment outcome: {other:?}"),
        }
    }
    assert_eq!(accepted, 1);

    multipart_contract(&e).await;
}

async fn multipart_contract(e: &TikvEngine) {
    let input = serde_json::from_value(serde_json::json!({
        "TableName":"multipart", "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "AttributeDefinitions":[
            {"AttributeName":"pk","AttributeType":"S"}, {"AttributeName":"g","AttributeType":"S"},
            {"AttributeName":"h","AttributeType":"S"}, {"AttributeName":"s1","AttributeType":"S"},
            {"AttributeName":"s2","AttributeType":"N"}],
        "GlobalSecondaryIndexes":[{"IndexName":"compound", "Projection":{"ProjectionType":"ALL"},
            "KeySchema":[{"AttributeName":"g","KeyType":"HASH"}, {"AttributeName":"h","KeyType":"HASH"},
                {"AttributeName":"s1","KeyType":"RANGE"}, {"AttributeName":"s2","KeyType":"RANGE"}]}]
    })).unwrap();
    e.create_table("111111111111", input).await.unwrap();
    let mut info = e.table_key_info("111111111111", "multipart").await.unwrap();
    for (first, second) in [("a", "2"), ("a", "3"), ("a", "10"), ("b", "2")] {
        let item = serde_json::from_value(serde_json::json!({
            "pk":{"S":format!("{first}{second}")}, "g":{"S":"g"}, "h":{"S":"h"},
            "s1":{"S":first}, "s2":{"N":second}
        }))
        .unwrap();
        e.put_item(&info, item, false, None, &ExpressionMaps::default(), None)
            .await
            .unwrap();
    }
    info.key_schema = info.global_secondary_indexes[0].key_schema.clone();
    let mut maps = ExpressionMaps::default();
    for (name, value) in [
        ("g", AttributeValue::S("g".into())),
        ("h", AttributeValue::S("h".into())),
        ("a", AttributeValue::S("a".into())),
        ("lo", AttributeValue::N("2".into())),
        ("hi", AttributeValue::N("3".into())),
    ] {
        maps.values.insert(name.into(), value);
    }
    for (text, expected) in [
        (
            "g = :g AND h = :h AND s1 = :a AND s2 BETWEEN :lo AND :hi",
            vec!["a2", "a3"],
        ),
        ("h = :h AND g = :g AND s2 = :lo AND s1 = :a", vec!["a2"]),
        ("g = :g AND h = :h AND s2 = :lo", vec!["a2", "b2"]),
    ] {
        let mut condition =
            expression::parse_key_condition(&expression::tokenize(text).unwrap()).unwrap();
        condition
            .resolve_multipart(&["g", "h"], &maps.names)
            .unwrap();
        for forward in [true, false] {
            let mut cursor = None;
            let mut found = vec![];
            loop {
                let (rows, next) = e
                    .query(
                        &info,
                        &condition,
                        &maps,
                        forward,
                        Some(1),
                        cursor.as_ref(),
                        Some("compound"),
                    )
                    .await
                    .unwrap();
                found.extend(rows.into_iter().map(|row| row["pk"].clone()));
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
            let mut want: Vec<_> = expected
                .iter()
                .map(|v| AttributeValue::S((*v).into()))
                .collect();
            if !forward {
                want.reverse();
            }
            assert_eq!(found, want, "{text}, forward={forward}");
        }
    }
}
#[tokio::test]
async fn memory_data_contract() {
    contract(TikvEngine::new(Arc::new(MemoryStore::default()), "contract", "us-east-1").unwrap())
        .await;
}
#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires dedicated TiKV/PD cluster; set TIKV_PD_ENDPOINTS"]
async fn real_tikv_data_contract() {
    common::real_contract(contract).await;
}

#[tokio::test]
async fn concurrent_conditional_creation_has_one_winner() {
    let e = TikvEngine::new(Arc::new(MemoryStore::default()), "race", "us-east-1").unwrap();
    let info = setup(&e).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut jobs = vec![];
    for _ in 0..8 {
        let (e, info, b) = (e.clone(), info.clone(), barrier.clone());
        jobs.push(tokio::spawn(async move {
            b.wait().await;
            e.put_item(
                &info,
                item("race", "1"),
                false,
                Some(&condition("attribute_not_exists(pk)")),
                &ExpressionMaps::default(),
                None,
            )
            .await
        }));
    }
    let mut wins = 0;
    for job in jobs {
        match job.await.unwrap() {
            Ok(_) => wins += 1,
            Err(StorageError::ConditionFailed(_)) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert_eq!(wins, 1);
}
