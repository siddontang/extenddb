// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Approximate table statistics with a bounded publication transaction.
//!
//! Capture counts and bytes in one read-only MVCC snapshot; publish them in a
//! separate transaction which reads only catalog records. Item changes can make
//! an observation stale, as allowed for table statistics. Table incarnation and
//! schema-generation checks prevent observations crossing lifecycle boundaries.
//! The split is essential: locking a large scanned item set at publication can
//! exceed TiKV's Raft request size or outlive lock TTLs. Unknown commit results
//! are never converted to success or blindly retried here.

use crate::{TikvEngine, kv};
use extenddb_core::types::*;
use extenddb_storage::error::StorageError;

/// An approximate point-in-time observation, fenced to one table incarnation
/// and schema. Publishing it never writes or locks the scanned item set.
pub(crate) struct StatisticsSnapshot {
    table_id: String,
    generation: u64,
    count: i64,
    bytes: i64,
}

impl TikvEngine {
    /// Observe bounded scan pages within one read-only MVCC transaction.
    pub(crate) async fn capture_statistics(
        &self,
        account: &str,
        name: &str,
    ) -> Result<StatisticsSnapshot, StorageError> {
        self.db
            .run(|tx| {
                let (e, account, name) = (self.clone(), account.to_owned(), name.to_owned());
                Box::pin(async move {
                    let table = e.table_by_name(tx, &account, &name).await?;
                    let prefix = e.item_prefix(&table.description.table_id);
                    let end = crate::codec::prefix_end(&prefix);
                    let mut start = prefix;
                    let (mut count, mut bytes) = (0i64, 0i64);
                    loop {
                        let rows = tx
                            .scan(start.clone(), end.clone(), 256, false)
                            .await
                            .map_err(kv::storage_error)?;
                        for (_, value) in &rows {
                            count += 1;
                            bytes += item_size_bytes(&kv::decode::<Item>(value)?) as i64;
                        }
                        if rows.len() < 256 {
                            break;
                        }
                        start = rows.last().unwrap().0.clone();
                        start.push(0);
                    }
                    Ok(StatisticsSnapshot {
                        table_id: table.description.table_id,
                        generation: table.fence_generation,
                        count,
                        bytes,
                    })
                })
            })
            .await
    }

    /// Publish only if the table incarnation and schema still match.
    pub(crate) async fn publish_statistics(
        &self,
        account: &str,
        name: &str,
        snapshot: &StatisticsSnapshot,
    ) -> Result<(), StorageError> {
        self.db
            .run(|tx| {
                let (e, account, name) = (self.clone(), account.to_owned(), name.to_owned());
                let (id, generation, count, bytes) = (
                    snapshot.table_id.clone(),
                    snapshot.generation,
                    snapshot.count,
                    snapshot.bytes,
                );
                Box::pin(async move {
                    let mut table = match e.table_by_name(tx, &account, &name).await {
                        Ok(table) => table,
                        Err(StorageError::TableNotFound(_)) => return Ok(()),
                        Err(error) => return Err(error),
                    };
                    if table.description.table_id != id
                        || table.fence_generation != generation
                        || table.description.table_status != TableStatus::Active
                    {
                        return Ok(());
                    }
                    table.description.item_count = count;
                    table.description.table_size_bytes = bytes;
                    e.save_table(tx, &table).await
                })
            })
            .await
    }
}

#[cfg(test)]
mod statistics_tests {
    use super::*;
    use crate::{catalog::TikvCatalog, kv::memory::MemoryStore};
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_storage::{
        DataEngine, MetadataEngine, TableEngine, management_store::ManagementStore,
    };
    use std::sync::Arc;
    const ACCOUNT: &str = "111111111111";

    fn input() -> CreateTableInput {
        serde_json::from_value(
            serde_json::json!({"TableName":"statistics", "BillingMode":"PAY_PER_REQUEST",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}),
        )
        .unwrap()
    }
    async fn setup() -> (TikvEngine, Arc<MemoryStore>) {
        let store = Arc::new(MemoryStore::default());
        let engine = TikvEngine::new(store.clone(), "statistics_test", "us-east-1").unwrap();
        TikvCatalog::new(engine.clone())
            .create_account(ACCOUNT, "owner")
            .await
            .unwrap();
        engine.create_table(ACCOUNT, input()).await.unwrap();
        let info = engine.table_key_info(ACCOUNT, "statistics").await.unwrap();
        for key in ["a", "b", "c"] {
            let item = serde_json::from_value(serde_json::json!({"pk":{"S":key}})).unwrap();
            engine
                .put_item(&info, item, false, None, &ExpressionMaps::default(), None)
                .await
                .unwrap();
        }
        (engine, store)
    }
    #[tokio::test]
    async fn publishing_statistics_does_not_lock_the_scanned_item_set() {
        let (engine, store) = setup().await;
        let before = store.scan_write_commits();
        engine
            .refresh_table_size(ACCOUNT, "statistics")
            .await
            .unwrap();
        assert_eq!(store.scan_write_commits(), before);
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(description.item_count, 3);
        assert_eq!(description.table_size_bytes, 9);
    }
    #[tokio::test]
    async fn a_schema_change_invalidates_the_statistics_observation() {
        let (engine, _) = setup().await;
        let snapshot = engine
            .capture_statistics(ACCOUNT, "statistics")
            .await
            .unwrap();
        engine
            .update_ttl(ACCOUNT, "statistics", "expires", true)
            .await
            .unwrap();
        engine
            .publish_statistics(ACCOUNT, "statistics", &snapshot)
            .await
            .unwrap();
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(description.item_count, 0);
        engine
            .refresh_table_size(ACCOUNT, "statistics")
            .await
            .unwrap();
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(description.item_count, 3);
        assert_eq!(
            engine
                .describe_ttl(ACCOUNT, "statistics")
                .await
                .unwrap()
                .attribute_name
                .as_deref(),
            Some("expires")
        );
    }
    #[tokio::test]
    async fn a_statistics_snapshot_cannot_be_published_to_a_recreated_table() {
        let (engine, _) = setup().await;
        let snapshot = engine
            .capture_statistics(ACCOUNT, "statistics")
            .await
            .unwrap();
        engine
            .delete_table(
                ACCOUNT,
                DeleteTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        for _ in 0..4 {
            engine.lifecycle_step().await.unwrap();
        }
        engine.create_table(ACCOUNT, input()).await.unwrap();
        engine
            .publish_statistics(ACCOUNT, "statistics", &snapshot)
            .await
            .unwrap();
        let description = engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: "statistics".into(),
                },
            )
            .await
            .unwrap();
        assert_ne!(description.table_id, snapshot.table_id);
        assert_eq!(description.item_count, 0);
        assert_eq!(description.table_size_bytes, 0);
    }
}
