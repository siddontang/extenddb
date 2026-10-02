// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog invariants run without network dependencies. These assertions are
//! reused by the real-cluster contract when the client feature is enabled.
#![cfg(feature = "test-support")]
mod common;
use extenddb_auth::CredentialStore;
use extenddb_storage::{authorization_store::AuthorizationStore, management_store::*};
use extenddb_storage_tikv::{
    TikvEngine,
    catalog::{TikvCatalog, crypto},
    engine::Clock,
    kv::memory::MemoryStore,
};
use serde_json::json;
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
async fn catalog_contract(engine: TikvEngine, clock: Arc<TestClock>) {
    let key = extenddb_storage::bootstrapper::helpers::generate_encryption_key();
    let c = TikvCatalog::new(engine)
        .with_encryption_key(key.clone())
        .unwrap();
    c.create_account("100000000001", "one").await.unwrap();
    c.create_account("100000000002", "two").await.unwrap();
    assert!(matches!(
        c.create_account("100000000003", "one").await,
        Err(OpError::AlreadyExists(_))
    ));
    let id = "100000000001";
    c.create_user(id, "alice", None).await.unwrap();
    c.create_user("100000000002", "alice", None).await.unwrap();
    assert!(matches!(
        c.create_user(id, "alice", None).await,
        Err(OpError::AlreadyExists(_))
    ));
    assert!(matches!(
        c.create_user("missing", "alice", None).await,
        Err(OpError::NotFound(_))
    ));
    let own = c.fetch_user_policies(id, "alice").await.unwrap();
    assert_eq!(own.len(), 1);
    assert!(!own[0].contains("SelfService"));
    assert!(own[0].contains("iam:CreateAccessKey"));
    c.create_group(id, "writers").await.unwrap();
    assert!(c.add_group_member(id, "writers", "ghost").await.is_err());
    c.add_group_member(id, "writers", "alice").await.unwrap();
    c.put_policy(id, "group", "writers", "write", &json!({"Statement":[]}))
        .await
        .unwrap();
    assert_eq!(
        c.fetch_user_group_policies(id, "alice")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        c.fetch_user_group_policies("100000000002", "alice")
            .await
            .unwrap()
            .is_empty()
    );
    c.tag_user(id, "alice", &[("department".into(), "engineering".into())])
        .await
        .unwrap();
    c.set_user_boundary(id, "alice", &json!({"Statement":[]}))
        .await
        .unwrap();
    assert!(c.fetch_user_boundary(id, "alice").await.unwrap().is_some());
    assert_eq!(c.fetch_user_tags(id, "alice").await.unwrap().len(), 1);
    c.untag_user(id, "alice", &["department".into()])
        .await
        .unwrap();
    c.delete_user_boundary(id, "alice").await.unwrap();
    assert!(c.fetch_user_tags(id, "alice").await.unwrap().is_empty());
    assert!(c.fetch_user_boundary(id, "alice").await.unwrap().is_none());
    let k = c.create_access_key(id, "alice").await.unwrap();
    let cred = c
        .lookup_credential(&k.access_key_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cred.account_id, id);
    assert_eq!(cred.secret_key, k.secret_access_key);
    assert!(matches!(
        c.import_access_key("100000000002", "alice", &k.access_key_id, "evil")
            .await,
        Err(OpError::AlreadyExists(_))
    ));
    assert!(
        c.list_access_keys("100000000002", "alice")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        c.delete_access_key("100000000002", "alice", &k.access_key_id)
            .await
            .is_err()
    );
    c.delete_access_key(id, "alice", &k.access_key_id)
        .await
        .unwrap();
    assert!(
        c.lookup_credential(&k.access_key_id)
            .await
            .unwrap()
            .is_none()
    );
    c.create_role(id, "runner", &json!({"Statement":[]}))
        .await
        .unwrap();
    c.tag_role(id, "runner", &[("env".into(), "test".into())])
        .await
        .unwrap();
    c.put_policy(id, "role", "runner", "run", &json!({"Statement":[]}))
        .await
        .unwrap();
    c.set_role_boundary(id, "runner", &json!({"Statement":[]}))
        .await
        .unwrap();
    assert_eq!(c.fetch_role_policies(id, "runner").await.unwrap().len(), 1);
    assert!(c.fetch_role_boundary(id, "runner").await.unwrap().is_some());
    let sid = "ASIA_SESSION_ONE";
    let encrypted = crypto::encrypt("session-secret", &key, sid).unwrap();
    let expiry = time::OffsetDateTime::from_unix_timestamp(2000).unwrap();
    c.store_session(
        "token",
        sid,
        &encrypted,
        id,
        "runner",
        "session",
        &Some(json!({"env":"staging"})),
        &Some(json!({"Statement":[]})),
        expiry,
    )
    .await
    .unwrap();
    let conflicting = crypto::encrypt("other-secret", &key, "ASIA_SESSION_OTHER").unwrap();
    assert!(
        c.store_session(
            "other-token",
            "ASIA_SESSION_OTHER",
            &conflicting,
            id,
            "runner",
            "session",
            &Some(json!({"env":"production"})),
            &Some(json!({"Statement":[]})),
            expiry,
        )
        .await
        .is_err()
    );
    assert!(
        c.lookup_credential("ASIA_SESSION_OTHER")
            .await
            .unwrap()
            .is_none()
    );
    let cred = c.lookup_credential(sid).await.unwrap().unwrap();
    assert_eq!(cred.secret_key, "session-secret");
    assert_eq!(cred.session_token.as_deref(), Some("token"));
    let data = c
        .fetch_session_data(id, "runner", "session")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.session_tags, vec![("env".into(), "staging".into())]);
    clock.0.store(2_000_000, Ordering::SeqCst);
    assert!(c.lookup_credential(sid).await.is_err());
    assert!(
        c.fetch_session_data(id, "runner", "session")
            .await
            .unwrap()
            .is_none()
    );
    c.delete_role(id, "runner").await.unwrap();
    assert!(c.lookup_credential(sid).await.unwrap().is_none());
    let k = c.create_access_key(id, "alice").await.unwrap();
    c.delete_user(id, "alice").await.unwrap();
    assert!(
        c.lookup_credential(&k.access_key_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        c.get_group_detail(id, "writers")
            .await
            .unwrap()
            .unwrap()
            .members
            .is_empty()
    );
    c.delete_group(id, "writers").await.unwrap();
    c.delete_account(id).await.unwrap();
    assert!(c.get_account_detail(id).await.unwrap().is_none());
    assert_eq!(c.list_all_accounts().await.unwrap().len(), 1);
}
#[tokio::test]
async fn iam_contract() {
    let clock = Arc::new(TestClock(AtomicI64::new(1_000_000)));
    let e = TikvEngine::new(Arc::new(MemoryStore::default()), "iam", "us-east-1")
        .unwrap()
        .with_clock(clock.clone());
    catalog_contract(e, clock).await;
}
#[tokio::test]
async fn operations_and_isolation() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock(AtomicI64::new(1_000_000)));
    let e = TikvEngine::new(store.clone(), "ops", "us-east-1")
        .unwrap()
        .with_clock(clock.clone());
    let c = TikvCatalog::new(e);
    c.set_setting("z", "3").await.unwrap();
    c.set_setting("a", "1").await.unwrap();
    c.set_setting("a", "2").await.unwrap();
    assert_eq!(
        c.list_settings().await.unwrap(),
        vec![("a".into(), "2".into()), ("z".into(), "3".into())]
    );
    let hash = bcrypt::hash("password", 4).unwrap();
    c.create_admin("admin", &hash).await.unwrap();
    assert!(matches!(
        c.create_admin("admin", &hash).await,
        Err(OpError::AlreadyExists(_))
    ));
    assert_eq!(
        c.verify_admin_password("admin", "password").await.unwrap(),
        Some(true)
    );
    assert_eq!(
        c.verify_admin_password("admin", "wrong").await.unwrap(),
        Some(false)
    );
    assert_eq!(
        c.verify_admin_password("missing", "wrong").await.unwrap(),
        None
    );
    c.change_admin_password("admin", &bcrypt::hash("changed", 4).unwrap())
        .await
        .unwrap();
    assert_eq!(
        c.verify_admin_password("admin", "changed").await.unwrap(),
        Some(true)
    );
    c.delete_admin("admin").await.unwrap();
    assert!(c.list_admins().await.unwrap().is_empty());
    c.record_failed_login("alice", Some("ip")).await;
    c.record_failed_login("bob", Some("ip")).await;
    assert_eq!(c.count_principal_failures("alice", 60).await.unwrap(), 1);
    assert_eq!(c.count_ip_failures("ip", 60).await.unwrap(), 2);
    clock.0.fetch_add(61_000, Ordering::SeqCst);
    assert_eq!(c.count_ip_failures("ip", 60).await.unwrap(), 0);
    c.cleanup_old_attempts(60).await;
    let now = time::OffsetDateTime::from_unix_timestamp(1061).unwrap();
    let row = MetricsRow {
        bucket: now,
        metric: "writes".into(),
        table_name: Some("t".into()),
        index_name: None,
        operation: None,
        sum: 8.0,
        count: 2,
        min: 3.0,
        max: 5.0,
    };
    c.insert_metrics(std::slice::from_ref(&row)).await.unwrap();
    c.insert_metrics(&[row]).await.unwrap();
    assert_eq!(
        c.query_metrics(now, now, Some("t"), Some("writes"))
            .await
            .unwrap()[0]
            .sum,
        16.0
    );
    assert!(
        c.query_metrics(now, now, Some("other"), None)
            .await
            .unwrap()
            .is_empty()
    );
    clock.0.fetch_add(61_000, Ordering::SeqCst);
    c.prune_metrics(std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        c.query_metrics(now, now, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    let other = TikvCatalog::new(TikvEngine::new(store, "other", "us-east-1").unwrap());
    assert!(other.list_settings().await.unwrap().is_empty());
}

#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires dedicated TiKV/PD cluster; set TIKV_PD_ENDPOINTS"]
async fn real_catalog_contract() {
    common::real_contract(|e| async move {
        let clock = Arc::new(TestClock(AtomicI64::new(1_000_000)));
        catalog_contract(e.with_clock(clock.clone()), clock).await;
    })
    .await;
}

async fn large_account_contract(e: TikvEngine) {
    let c = TikvCatalog::new(e.clone())
        .with_encryption_key(extenddb_storage::bootstrapper::helpers::generate_encryption_key())
        .unwrap();
    c.create_account("large", "large").await.unwrap();
    // Exercise the storage boundary directly: the logical aggregate exceeds the
    // old 4 MiB cap, while each independently stored policy is small enough.
    let policy = json!({"padding": "x".repeat(128 * 1024)});
    for i in 0..34 {
        let name = format!("user-{i:03}");
        c.create_user("large", &name, None).await.unwrap();
        c.put_policy("large", "user", &name, "large-policy", &policy)
            .await
            .unwrap();
    }
    assert_eq!(
        c.get_account_detail("large")
            .await
            .unwrap()
            .unwrap()
            .users
            .len(),
        34
    );
    let key = c.create_access_key("large", "user-033").await.unwrap();
    assert_eq!(
        c.lookup_credential(&key.access_key_id)
            .await
            .unwrap()
            .unwrap()
            .account_id,
        "large"
    );
    assert_eq!(
        c.fetch_user_policies("large", "user-000")
            .await
            .unwrap()
            .len(),
        2
    );
    c.delete_account("large").await.unwrap();
    c.create_account("large", "reused").await.unwrap();
    assert!(
        c.get_account_detail("large")
            .await
            .unwrap()
            .unwrap()
            .users
            .is_empty()
    );
    assert!(
        c.lookup_credential(&key.access_key_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        c.fetch_user_policies("large", "user-000")
            .await
            .unwrap()
            .is_empty()
    );
    e.lifecycle_step().await.unwrap();
}
#[tokio::test]
async fn account_can_exceed_four_mib() {
    large_account_contract(
        TikvEngine::new(
            Arc::new(MemoryStore::default()),
            "large-account",
            "us-east-1",
        )
        .unwrap(),
    )
    .await;
}
#[cfg(feature = "client")]
#[tokio::test]
#[ignore = "requires dedicated TiKV/PD cluster; set TIKV_PD_ENDPOINTS"]
async fn real_large_account_contract() {
    common::real_contract(large_account_contract).await;
}
