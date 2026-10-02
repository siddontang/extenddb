// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bootstrap is tested with injected storage: namespace ownership, idempotent
//! seeds, fail-closed startup and namespace-limited destruction.
#![cfg(feature = "test-support")]
use extenddb_storage::{bootstrapper::Bootstrapper, management_store::*};
use extenddb_storage_tikv::{
    TikvEngine,
    bootstrap::{TikvBootstrapper, ready_catalog},
    catalog::TikvCatalog,
    config::TikvConfig,
    kv::memory::MemoryStore,
};
use std::sync::Arc;
#[tokio::test]
async fn lifecycle_is_idempotent_and_destroy_is_scoped() {
    let store = Arc::new(MemoryStore::default());
    let mut config = TikvConfig::default();
    config.namespace = "bootstrap".into();
    let config = config.validated().unwrap();
    let e = TikvEngine::new(store.clone(), "bootstrap", "us-east-1").unwrap();
    let other = TikvCatalog::new(TikvEngine::new(store, "other", "us-east-1").unwrap());
    other.set_setting("sentinel", "preserve").await.unwrap();
    let b = TikvBootstrapper::new(e.clone(), config);
    assert!(ready_catalog(e.clone()).await.is_err());
    b.create_catalog_db().await.unwrap();
    assert!(b.create_catalog_db().await.is_err());
    b.run_catalog_migrations().await.unwrap();
    assert!(ready_catalog(e.clone()).await.is_err());
    b.run_data_migrations().await.unwrap();
    b.record_data_connection().await.unwrap();
    b.bootstrap_encryption_key().await.unwrap();
    b.bootstrap_default_account().await.unwrap();
    let result = b
        .bootstrap_admin_user(Some("admin"), Some("test-password-only"))
        .await
        .unwrap();
    assert!(result.from_env);
    assert!(result.generated_password.is_none());
    let c = ready_catalog(e.clone()).await.unwrap();
    let key = c.get_setting("encryption_key").await.unwrap();
    let id = c.default_account_id().await.unwrap();
    b.bootstrap_encryption_key().await.unwrap();
    b.bootstrap_default_account().await.unwrap();
    assert!(
        b.bootstrap_admin_user(Some("admin"), Some("different"))
            .await
            .unwrap()
            .already_existed
    );
    assert_eq!(c.get_setting("encryption_key").await.unwrap(), key);
    assert_eq!(c.default_account_id().await.unwrap(), id);
    assert_eq!(
        c.verify_admin_password("admin", "test-password-only")
            .await
            .unwrap(),
        Some(true)
    );
    assert!(b.pending_data_migrations().await.unwrap().is_empty());
    assert!(b.drop_databases("other").await.is_err());
    b.drop_databases("bootstrap").await.unwrap();
    assert!(ready_catalog(e).await.is_err());
    assert_eq!(
        other.get_setting("sentinel").await.unwrap().as_deref(),
        Some("preserve")
    );
}
#[cfg(feature = "client")]
#[test]
fn bootstrap_config_overrides_are_explicit() {
    use extenddb_storage_tikv::backend::bootstrap_config;
    let path = format!("/tmp/extenddb-tikv-config-{}.toml", uuid::Uuid::new_v4());
    let args = vec![
        "--tikv-namespace".into(),
        "test".into(),
        "--tikv-pd-endpoints".into(),
        "127.0.0.1:2379,127.0.0.1:2389".into(),
    ];
    let c = bootstrap_config(&path, &args).unwrap();
    assert_eq!(c.namespace, "test");
    assert_eq!(c.pd_endpoints.len(), 2);
    std::fs::write(
        &path,
        "[storage]\nbackend='tikv'\n[storage.tikv]\nnamespace='existing'\n",
    )
    .unwrap();
    let result = bootstrap_config(&path, &args);
    std::fs::remove_file(&path).unwrap();
    assert!(result.is_err());
}
