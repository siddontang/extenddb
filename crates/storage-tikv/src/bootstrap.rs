// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Namespace lifecycle, independent of the network client and CLI parser.
//! Initialization reserves a namespace atomically. Seeds use compare-and-create
//! transactions, so retries preserve encryption keys, account IDs and passwords.
//! Schema v1 has no multi-step DDL: its version check/update is one transaction,
//! which serializes concurrent migrations without a process-lifetime lease.
use crate::{
    TikvEngine,
    catalog::{Account, TikvCatalog, op_error},
    config::TikvConfig,
    kv,
};
use extenddb_storage::{
    bootstrapper::{AdminBootstrapResult, Bootstrapper, helpers},
    management_store::*,
};
use std::sync::Arc;
/// On-disk schema version, independent of the server release version.
pub const SCHEMA_VERSION: &str = "1";
pub struct TikvBootstrapper {
    engine: TikvEngine,
    config: TikvConfig,
}
impl TikvBootstrapper {
    pub fn new(engine: TikvEngine, config: TikvConfig) -> Self {
        Self { engine, config }
    }
    fn catalog(&self) -> TikvCatalog {
        TikvCatalog::new(self.engine.clone())
    }
    async fn seed(&self, name: &str, value: String) -> OpResult<()> {
        let key = self.engine.key(&["setting", name]);
        self.engine
            .db
            .run(move |tx| {
                let key = key.clone();
                let value = value.clone();
                Box::pin(async move {
                    if kv::get::<String>(tx, key.clone()).await?.is_none() {
                        kv::put(tx, key, &value).await?;
                    }
                    Ok(())
                })
            })
            .await
            .map_err(op_error)
    }
}
#[async_trait::async_trait]
impl Bootstrapper for TikvBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        Ok(())
    } // TiKV authenticates via cluster TLS, not SQL roles.
    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Ok(())
    }
    async fn create_catalog_db(&self) -> OpResult<()> {
        let key = self.engine.key(&["deployment"]);
        self.engine
            .db
            .run(move |tx| {
                let key = key.clone();
                Box::pin(async move {
                    if kv::get::<String>(tx, key.clone()).await?.is_some() {
                        return Ok(Err(OpError::AlreadyExists(
                            "TiKV namespace already initialized or reserved by an earlier init"
                                .into(),
                        )));
                    }
                    kv::put(tx, key, &"initializing").await?;
                    Ok(Ok(()))
                })
            })
            .await
            .map_err(op_error)?
    }
    async fn create_data_db(&self) -> OpResult<()> {
        Ok(())
    }
    async fn run_catalog_migrations(&self) -> OpResult<()> {
        let e = self.engine.clone();
        e.db.clone()
            .run(move |tx| {
                let e = e.clone();
                Box::pin(async move {
                    if kv::get::<String>(tx, e.key(&["deployment"]))
                        .await?
                        .is_none()
                    {
                        return Ok(Err(OpError::NotFound(
                            "TiKV namespace is not initialized".into(),
                        )));
                    }
                    let k = e.key(&["setting", "catalog_version"]);
                    if let Some(version) = kv::get::<String>(tx, k.clone()).await?
                        && version != SCHEMA_VERSION
                    {
                        return Ok(Err(OpError::Validation(format!(
                            "Unsupported TiKV schema version {version}; expected {SCHEMA_VERSION}"
                        ))));
                    }
                    kv::put(tx, k, &SCHEMA_VERSION).await?;
                    Ok(Ok(()))
                })
            })
            .await
            .map_err(op_error)?
    }
    async fn run_data_migrations(&self) -> OpResult<()> {
        self.run_catalog_migrations().await
    }
    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        match self.read_catalog_version().await? {
            Some(v) if v == SCHEMA_VERSION => Ok(vec![]),
            None => Ok(vec!["tikv-schema-v1".into()]),
            Some(v) => Err(OpError::Validation(format!(
                "Unsupported TiKV schema version {v}"
            ))),
        }
    }
    async fn record_data_connection(&self) -> OpResult<()> {
        self.seed(
            "data_database_connection_string",
            self.config.descriptor.clone(),
        )
        .await
    }
    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        self.seed("encryption_key", helpers::generate_encryption_key())
            .await
    }
    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let e = self.engine.clone();
        let id = helpers::generate_account_id();
        let now = self.catalog().now();
        e.db.clone()
            .run(move |tx| {
                let e = e.clone();
                let id = id.clone();
                Box::pin(async move {
                    let key = e.key(&["setting", "default_account_id"]);
                    if kv::get::<String>(tx, key.clone()).await?.is_some() {
                        return Ok(());
                    }
                    let existing: Option<String> =
                        kv::get(tx, e.key(&["account_name", "default"])).await?;
                    let id = if let Some(id) = existing {
                        id
                    } else {
                        if kv::get::<Account>(tx, e.key(&["account", &id]))
                            .await?
                            .is_some()
                        {
                            return Err(extenddb_storage::error::StorageError::Internal(
                                "Generated default account id already exists; retry init".into(),
                            ));
                        }
                        kv::put(
                            tx,
                            e.key(&["account", &id]),
                            &Account::new(id.clone(), "default".into(), now),
                        )
                        .await?;
                        kv::put(tx, e.key(&["account_name", "default"]), &id).await?;
                        id
                    };
                    kv::put(tx, key, &id).await
                })
            })
            .await
            .map_err(op_error)
    }
    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        let c = self.catalog();
        let username = env_user.unwrap_or("admin").to_owned();
        if c.list_admins()
            .await?
            .iter()
            .any(|a| a.admin_name == username)
        {
            return Ok(AdminBootstrapResult {
                username,
                generated_password: None,
                already_existed: true,
                from_env: env_password.is_some(),
            });
        }
        let password = env_password
            .map(str::to_owned)
            .unwrap_or_else(helpers::generate_random_password);
        let hash = helpers::hash_password_async(password.clone()).await?;
        let existed = match c.create_admin(&username, &hash).await {
            Ok(()) => false,
            Err(OpError::AlreadyExists(_)) => true,
            Err(err) => return Err(err),
        };
        let k = self.engine.key(&["deployment"]);
        self.engine
            .db
            .run(move |tx| {
                let k = k.clone();
                Box::pin(async move { kv::put(tx, k, &"ready").await })
            })
            .await
            .map_err(op_error)?;
        Ok(AdminBootstrapResult {
            username,
            generated_password: (!existed && env_password.is_none()).then_some(password),
            already_existed: existed,
            from_env: env_password.is_some(),
        })
    }
    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        Ok(self.read_catalog_version().await?.is_some())
    }
    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        self.engine
            .db
            .run(|tx| {
                let e = self.engine.clone();
                Box::pin(async move {
                    Ok(e.tables(tx)
                        .await?
                        .into_iter()
                        .map(|t| format!("{}/{}", t.account, t.description.table_name))
                        .collect())
                })
            })
            .await
            .map_err(op_error)
    }
    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        Ok(Some(self.config.namespace.clone()))
    }
    async fn drop_databases(&self, data_db: &str) -> OpResult<()> {
        if data_db != self.config.namespace {
            return Err(OpError::Validation(
                "Destroy namespace does not match configured namespace".into(),
            ));
        }
        let p = self.engine.key(&[]);
        loop {
            let count = self
                .engine
                .db
                .run(|tx| {
                    let p = p.clone();
                    Box::pin(async move {
                        let rows = kv::prefix(tx, p, 256).await?;
                        let count = rows.len();
                        for (k, _) in rows {
                            kv::delete(tx, k).await?;
                        }
                        Ok(count)
                    })
                })
                .await
                .map_err(op_error)?;
            if count < 256 {
                break;
            }
        }
        Ok(())
    }
    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        self.catalog().get_setting("catalog_version").await
    }
    fn expected_catalog_version(&self) -> String {
        SCHEMA_VERSION.into()
    }
    fn catalog_database_name(&self) -> String {
        self.config.namespace.clone()
    }
    fn endpoint_info(&self) -> String {
        self.config.pd_endpoints.join(",")
    }
    fn catalog_connection_url(&self) -> String {
        self.config.descriptor.clone()
    }
    fn generate_backend_config_section(&self) -> String {
        format!(
            "[storage.tikv]\n{}",
            toml::to_string(&self.config).expect("TiKV config serializes")
        )
    }
}
/// Validate all prerequisites before accepting signed requests.
pub async fn ready_catalog(engine: TikvEngine) -> OpResult<Arc<TikvCatalog>> {
    let c = TikvCatalog::new(engine);
    let version = c.get_setting("catalog_version").await?.ok_or_else(|| {
        OpError::NotFound("TiKV namespace has not been initialized; run extenddb init".into())
    })?;
    if version != SCHEMA_VERSION {
        return Err(OpError::Validation(format!(
            "TiKV schema mismatch: expected {SCHEMA_VERSION}, found {version}"
        )));
    }
    let key = c.get_setting("encryption_key").await?.ok_or_else(|| {
        OpError::NotFound("Encryption key is missing; initialization is incomplete".into())
    })?;
    let id = c.default_account_id().await?.ok_or_else(|| {
        OpError::NotFound("Default account is missing; initialization is incomplete".into())
    })?;
    if c.get_account_detail(&id).await?.is_none() || c.list_admins().await?.is_empty() {
        return Err(OpError::NotFound("Catalog bootstrap is incomplete".into()));
    }
    Ok(Arc::new(c.with_encryption_key(key)?))
}
