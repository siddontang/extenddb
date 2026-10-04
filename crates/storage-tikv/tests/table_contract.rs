// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog-dependent validation and concurrent creation, on memory and TiKV.
#![cfg(feature = "test-support")]
mod common;
use extenddb_core::types::*;
use extenddb_storage::{
    BackupEngine, TableEngine,
    error::StorageError,
    management_store::{ManagementStore, OpError, SettingsStore},
};
use extenddb_storage_tikv::{TikvEngine, catalog::TikvCatalog, kv::memory::MemoryStore};
use std::sync::Arc;

fn create(name: &str) -> CreateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": name, "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [{"AttributeName":"pk", "KeyType":"HASH"}],
        "AttributeDefinitions": [{"AttributeName":"pk", "AttributeType":"S"}]
    }))
    .unwrap()
}

async fn contract(e: TikvEngine) {
    let account = "111111111111";
    let catalog = TikvCatalog::new(e.clone());
    catalog
        .create_account(account, "table-contract")
        .await
        .unwrap();
    let mut encrypted = create("unsupported-encryption");
    encrypted.sse_specification = Some(serde_json::json!({"Enabled":true,"SSEType":"KMS"}));
    assert!(matches!(e.create_table(account, encrypted).await,
        Err(StorageError::Unsupported(message)) if message.contains("SSE/KMS")));
    assert!(matches!(
        e.table_key_info(account, "unsupported-encryption").await,
        Err(StorageError::TableNotFound(_))
    ));
    assert!(
        e.validate_sse_specification(Some(&serde_json::json!({"Enabled":false})))
            .is_ok()
    );
    // Exercise the shared account guard under concurrent control-plane writes.
    let jobs = (0..32).map(|i| {
        let e = e.clone();
        async move { e.create_table(account, create(&format!("table-{i}"))).await }
    });
    for result in futures::future::join_all(jobs).await {
        assert_eq!(result.unwrap().table_status, TableStatus::Active);
    }
    let update = |v| serde_json::from_value::<UpdateTableInput>(v).unwrap();
    let throughput = serde_json::json!({"ReadCapacityUnits":5, "WriteCapacityUnits":5});
    assert!(matches!(
        e.update_table(
            account,
            update(serde_json::json!({
                "TableName":"table-0", "ProvisionedThroughput":throughput,
                "DeletionProtectionEnabled":true
            }))
        )
        .await,
        Err(StorageError::Validation(_))
    ));
    let current = e
        .describe_table(
            account,
            DescribeTableInput {
                table_name: "table-0".into(),
            },
        )
        .await
        .unwrap();
    assert!(
        !current.deletion_protection_enabled,
        "failed update must be atomic"
    );
    let provision = serde_json::json!({
        "TableName":"table-0", "BillingMode":"PROVISIONED",
        "ProvisionedThroughput":throughput
    });
    let provisioned = e
        .update_table(account, update(provision.clone()))
        .await
        .unwrap();
    assert!(provisioned.billing_mode_summary.is_none());
    assert!(matches!(
        e.update_table(account, update(provision)).await,
        Err(StorageError::NoOpUpdate(_))
    ));
    e.update_table(
        account,
        update(serde_json::json!({
            "TableName":"table-0", "ProvisionedThroughput":{
                "ReadCapacityUnits":6,"WriteCapacityUnits":7
            }
        })),
    )
    .await
    .unwrap();
    e.update_table(
        account,
        update(serde_json::json!({
            "TableName":"table-0", "BillingMode":"PAY_PER_REQUEST"
        })),
    )
    .await
    .unwrap();

    catalog
        .set_setting("control_plane_delay_seconds", "0.05")
        .await
        .unwrap();
    assert_eq!(
        e.create_table(account, create("fractional"))
            .await
            .unwrap()
            .table_status,
        TableStatus::Creating
    );
    for bad in ["NaN", "inf", "-1", "1e100"] {
        catalog
            .set_setting("control_plane_delay_seconds", bad)
            .await
            .unwrap();
        assert!(
            e.create_table(account, create("invalid-delay"))
                .await
                .is_err()
        );
        assert!(matches!(
            e.describe_table(
                account,
                DescribeTableInput {
                    table_name: "invalid-delay".into()
                }
            )
            .await,
            Err(StorageError::TableNotFound(_))
        ));
    }
}

#[tokio::test]
async fn memory_table_contract() {
    contract(TikvEngine::new(Arc::new(MemoryStore::default()), "tables", "us-east-1").unwrap())
        .await;
}

#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires dedicated TiKV/PD cluster; set TIKV_PD_ENDPOINTS"]
async fn real_tikv_table_contract() {
    common::real_contract(contract).await;
}

// Independent catalog operations protect the same account. Exercise publication
// and reclamation together, checking the externally visible table-count guard.
async fn publication_contract(e: TikvEngine) {
    let account = "222222222222";
    let catalog = TikvCatalog::new(e.clone());
    catalog
        .create_account(account, "publication")
        .await
        .unwrap();
    e.create_table(account, create("source")).await.unwrap();
    let backup = e.create_backup(account, "source", "seed").await.unwrap();
    let jobs = (0..24).map(|i| {
        let e = e.clone();
        let arn = backup.backup_arn.clone();
        async move {
            let name = format!("published-{i}");
            match i % 3 {
                0 => {
                    e.create_table(account, create(&name)).await?;
                }
                1 => {
                    e.restore_table_from_backup(account, &name, &arn).await?;
                }
                _ => {
                    e.create_backup(account, "source", &name).await?;
                }
            }
            Ok::<_, StorageError>(())
        }
    });
    for result in futures::future::join_all(jobs).await {
        result.unwrap();
    }
    assert!(matches!(
        catalog.delete_account(account).await,
        Err(OpError::HasDependents(_))
    ));
    let tables = e
        .list_tables(
            account,
            ListTablesInput {
                exclusive_start_table_name: None,
                limit: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(tables.table_names.len(), 17);
    for name in tables.table_names {
        e.delete_table(account, DeleteTableInput { table_name: name })
            .await
            .unwrap();
    }
    for result in futures::future::join_all((0..4).map(|_| e.lifecycle_step())).await {
        result.unwrap();
    }
    assert!(
        e.list_tables(
            account,
            ListTablesInput {
                exclusive_start_table_name: None,
                limit: None
            }
        )
        .await
        .unwrap()
        .table_names
        .is_empty()
    );
    catalog.delete_account(account).await.unwrap();
}

#[tokio::test]
async fn memory_catalog_publication_contract() {
    publication_contract(
        TikvEngine::new(Arc::new(MemoryStore::default()), "publication", "us-east-1").unwrap(),
    )
    .await;
}

#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires dedicated TiKV/PD cluster; set TIKV_PD_ENDPOINTS"]
async fn real_tikv_catalog_publication_contract() {
    common::real_contract(publication_contract).await;
}
