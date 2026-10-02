// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Thin integration shell: config parsing, client connection and backend factories.
//! All business logic is available through injected engines without this module.
//! Factories preserve the complete descriptor (namespace, endpoints and TLS).
use crate::{
    TikvEngine,
    bootstrap::{SCHEMA_VERSION, TikvBootstrapper, ready_catalog},
    catalog::TikvCatalog,
    config::TikvConfig,
};
use extenddb_storage::{
    Backend,
    config::StorageConfig,
    error::StorageError,
    operations::{ConnectionParts, OperationsEngine},
};
use futures::future::BoxFuture;
use std::sync::Arc;
struct Operations;
impl OperationsEngine for Operations {
    fn parse_connection_string(&self, s: &str) -> Result<ConnectionParts, StorageError> {
        let c = TikvConfig::from_descriptor(s)?;
        Ok(ConnectionParts {
            host: c.pd_endpoints.join(","),
            port: 2379,
            user: String::new(),
            password: String::new(),
            database: c.namespace,
        })
    }
    fn redact_connection_string(&self, s: &str) -> String {
        TikvConfig::from_descriptor(s)
            .map(|c| format!("tikv://{}/{}", c.pd_endpoints.join(","), c.namespace))
            .unwrap_or_else(|_| "tikv://<invalid descriptor>".into())
    }
    fn validate_identifier(&self, name: &str, _label: &str) -> Result<(), StorageError> {
        crate::config::validate_namespace(name)
    }
    fn catalog_version(&self) -> String {
        SCHEMA_VERSION.into()
    }
    fn is_sensitive_key(&self, key: &str) -> bool {
        ["password", "secret", "encryption_key", "token", "key_path"]
            .iter()
            .any(|s| key.contains(s))
    }
}
pub fn backend() -> Backend {
    Backend {
        name: "tikv",
        bootstrapper: bootstrap_factory,
        storage_config: |t| {
            let c: TikvConfig = toml::Value::Table(t.clone())
                .try_into()
                .map_err(|e: toml::de::Error| e.to_string())?;
            Ok(Box::new(c.validated().map_err(|e| e.to_string())?) as Box<dyn StorageConfig>)
        },
        operations: &Operations,
        settings_store: |s| {
            let s = s.to_owned();
            Box::pin(async move {
                let e = connect_descriptor(&s, "us-east-1").await.map_err(|e| {
                    extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(
                        e.to_string(),
                    )
                })?;
                Ok(Box::new(TikvCatalog::new(e))
                    as Box<
                        dyn extenddb_storage::management_store::SettingsStore,
                    >)
            })
        },
        diagnostics_store: |s| {
            let s = s.to_owned();
            Box::pin(async move {
                let e = connect_descriptor(&s, "us-east-1").await.map_err(|e| {
                    extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(
                        e.to_string(),
                    )
                })?;
                Ok(Box::new(TikvCatalog::new(e))
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: |config, region, _options| {
            let descriptor = config.connection_config().to_owned();
            let region = region.to_owned();
            Box::pin(async move {
                use extenddb_storage::server_components::{BackendError, ServerComponents};
                let engine = Arc::new(connect_descriptor(&descriptor, &region).await.map_err(
                    |e| BackendError::ConnectionFailed {
                        backend: "tikv".into(),
                        details: e.to_string(),
                    },
                )?);
                let catalog = ready_catalog(engine.as_ref().clone())
                    .await
                    .map_err(|e| BackendError::InitializationFailed(format!("{e:?}")))?;
                Ok(ServerComponents {
                    engine: engine.clone(),
                    catalog_store: catalog.clone(),
                    credential_store: catalog,
                    runtime_hooks: Some(Box::new(crate::runtime::TikvRuntime { engine })),
                })
            })
        },
    }
}
async fn connect_descriptor(s: &str, region: &str) -> Result<TikvEngine, StorageError> {
    let c = TikvConfig::from_descriptor(s)?;
    TikvEngine::new(c.connect().await?, &c.namespace, region)
}
fn bootstrap_factory(
    path: String,
    args: Vec<String>,
) -> BoxFuture<'static, Result<Box<dyn extenddb_storage::bootstrapper::Bootstrapper>, StorageError>>
{
    Box::pin(async move {
        let c = bootstrap_config(&path, &args)?;
        let e = TikvEngine::new(c.connect().await?, &c.namespace, "us-east-1")?;
        Ok(Box::new(TikvBootstrapper::new(e, c))
            as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
    })
}
/// Parse init overrides separately so precedence/conflicts can be tested offline.
pub fn bootstrap_config(path: &str, args: &[String]) -> Result<TikvConfig, StorageError> {
    use extenddb_storage::bootstrapper::helpers::{check_conflict, extract_arg};
    let exists = std::path::Path::new(path).exists();
    let mut c = if exists {
        let text =
            std::fs::read_to_string(path).map_err(|e| StorageError::Internal(e.to_string()))?;
        let doc: toml::Value =
            toml::from_str(&text).map_err(|e| StorageError::Validation(e.to_string()))?;
        let storage = doc
            .get("storage")
            .ok_or_else(|| StorageError::Validation("Missing [storage] configuration".into()))?;
        if storage
            .get("backend")
            .and_then(toml::Value::as_str)
            .is_some_and(|v| v != "tikv")
        {
            return Err(StorageError::Validation(
                "Config selects another storage backend".into(),
            ));
        }
        storage
            .get("tikv")
            .cloned()
            .unwrap_or(toml::Value::Table(Default::default()))
            .try_into::<TikvConfig>()
            .map_err(|e| StorageError::Validation(e.to_string()))?
    } else {
        TikvConfig::default()
    };
    if let Some(value) = extract_arg(args, "--tikv-pd-endpoints") {
        let endpoints: Vec<String> = value.split(',').map(|s| s.trim().to_owned()).collect();
        if exists && endpoints != c.pd_endpoints {
            return Err(StorageError::Validation(
                "--tikv-pd-endpoints conflicts with config".into(),
            ));
        }
        c.pd_endpoints = endpoints;
    }
    if let Some(value) = extract_arg(args, "--tikv-namespace") {
        if exists {
            check_conflict(Some(&value), &c.namespace, "--tikv-namespace")?;
        }
        c.namespace = value;
    }
    c.validated()
}
